#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_page.py — 拉取单个 Flink 文档页(带缓存)

设计:
  - 优先读 cache/,7 天有效
  - 缓存未命中 → 联网,HTML → markdown,落 cache,返回 stdout
  - 仅在 stdout 输出 markdown 主体,警告/进度信息走 stderr

Usage:
  py -3.12 scripts/fetch_page.py <url>                           # 拉取并打印 markdown
  py -3.12 scripts/fetch_page.py <url> --force                   # 忽略缓存,刷新
  py -3.12 scripts/fetch_page.py <url> --max-chars 5000          # 截断正文
  py -3.12 scripts/fetch_page.py <url> --cache-days 3            # 自定义 TTL
  py -3.12 scripts/fetch_page.py --list-cache                   # 列出已缓存页面
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
import html2text

USER_AGENT = "Mavis-FlinkSkill-Runtime/1.0"
TIMEOUT = 20
ALLOWED_DOMAINS = {"flink.apache.org", "nightlies.apache.org"}


def _default_cache_dir() -> Path:
    here = Path(__file__).resolve().parent.parent
    return here / "cache"


def url_hash(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]


def is_flink_url(url: str) -> bool:
    from urllib.parse import urlparse
    u = urlparse(url)
    return u.netloc in ALLOWED_DOMAINS


def fetch_html(url: str) -> Optional[str]:
    """UTF-8 优先解析(同 build.py 处理方式一致)。"""
    try:
        r = requests.get(
            url,
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
    except requests.RequestException as e:
        print(f"[fetch error] {e}", file=sys.stderr)
        return None
    if r.status_code != 200 or not r.content:
        print(f"[fetch http] {r.status_code}", file=sys.stderr)
        return None
    m = re.search(rb'<meta[^>]*charset=["\']?([^"\'\s>]+)', r.content[:1024], re.I)
    charset = (m.group(1).decode("ascii", errors="ignore").lower() if m else "utf-8")
    try:
        return r.content.decode(charset, errors="replace")
    except LookupError:
        return r.content.decode("utf-8", errors="replace")


def html_to_markdown(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "form", "button"]):
        tag.decompose()
    for selector in [
        "nav", "header.site-header", "footer", "aside",
        ".sidebar", ".nav-tabs", ".breadcrumb",
        ".edit-page-link", ".page-nav", ".toc-affix",
        "[role='navigation']", "[aria-label='breadcrumb']",
    ]:
        for el in soup.select(selector):
            el.decompose()
    h = html2text.HTML2Text()
    h.body_width = 0
    h.ignore_images = True
    h.ignore_links = False
    h.protect_links = True
    h.unicode_snob = True
    h.bypass_tables = False
    md = h.handle(str(soup))
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r"[ \t]+\n", "\n", md)
    return md.strip()


def load_cache(cache_dir: Path, url: str, max_age_days: int) -> Optional[dict]:
    """读缓存,返回 {md, meta} 或 None。"""
    h = url_hash(url)
    md_path = cache_dir / f"{h}.md"
    meta_path = cache_dir / f"{h}.json"
    if not md_path.exists() or not meta_path.exists():
        return None
    age = (time.time() - md_path.stat().st_mtime) / 86400
    if age > max_age_days:
        return None
    try:
        md = md_path.read_text(encoding="utf-8")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return {"md": md, "meta": meta, "age_days": age}
    except Exception as e:
        print(f"[cache read error] {e}", file=sys.stderr)
        return None


def save_cache(cache_dir: Path, url: str, md: str, meta: dict) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    h = url_hash(url)
    (cache_dir / f"{h}.md").write_text(md, encoding="utf-8")
    meta["url"] = url
    meta["cached_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    (cache_dir / f"{h}.json").write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8"
    )


def fetch(url: str, cache_dir: Path, force: bool, max_age_days: int) -> dict:
    """主函数:返回 {url, md, meta, cache_hit}。"""
    if not is_flink_url(url):
        return {
            "url": url,
            "md": "",
            "meta": {},
            "error": f"URL not under allowed domains: {ALLOWED_DOMAINS}",
        }

    if not force:
        cached = load_cache(cache_dir, url, max_age_days)
        if cached:
            return {
                "url": url,
                "md": cached["md"],
                "meta": cached["meta"],
                "cache_hit": True,
                "age_days": round(cached["age_days"], 1),
            }

    html = fetch_html(url)
    if html is None:
        return {"url": url, "md": "", "meta": {}, "error": "fetch failed"}
    md = html_to_markdown(html)
    if not md:
        return {"url": url, "md": "", "meta": {}, "error": "empty markdown"}

    # 从 md 提标题
    h1 = re.search(r"^#\s+(.+?)$", md, re.MULTILINE)
    title = h1.group(1).strip() if h1 else ""
    meta = {
        "title": title,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    save_cache(cache_dir, url, md, meta)
    return {"url": url, "md": md, "meta": meta, "cache_hit": False}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Fetch a single Flink doc page")
    p.add_argument("url", nargs="?", help="Flink URL to fetch")
    p.add_argument("--cache-dir", type=Path, default=None, help="缓存目录")
    p.add_argument("--force", action="store_true", help="忽略缓存强制刷新")
    p.add_argument("--cache-days", type=int, default=7, help="缓存 TTL(天)")
    p.add_argument("--max-chars", type=int, default=0, help="截断 markdown 到 N 字符")
    p.add_argument("--json", action="store_true", help="以 JSON 输出而非 markdown")
    p.add_argument("--list-cache", action="store_true", help="列出已缓存的页面")
    args = p.parse_args()

    cache_dir = args.cache_dir or _default_cache_dir()

    if args.list_cache:
        cache_dir.mkdir(parents=True, exist_ok=True)
        for meta_path in sorted(cache_dir.glob("*.json")):
            try:
                m = json.loads(meta_path.read_text(encoding="utf-8"))
                age = (time.time() - meta_path.stat().st_mtime) / 86400
                print(f"[{age:5.1f}d] {m.get('title','(no title)')[:50]:<50}  {m.get('url','')}")
            except Exception:
                pass
        return 0

    if not args.url:
        p.error("需要 url 或 --list-cache")

    result = fetch(args.url, cache_dir, args.force, args.cache_days)

    if result.get("error"):
        print(f"[ERROR] {result['error']}", file=sys.stderr)
        return 1

    md = result["md"]
    if args.max_chars > 0 and len(md) > args.max_chars:
        md = md[: args.max_chars] + "\n\n...[truncated]"

    if args.json:
        result_clean = {k: v for k, v in result.items() if k != "md"}
        result_clean["md_len"] = len(md)
        result_clean["md_preview"] = md[:500]
        print(json.dumps(result_clean, ensure_ascii=False, indent=2))
        return 0

    # 正常输出:meta 行 + markdown
    title = result.get("meta", {}).get("title", "(no title)")
    cache_hit = result.get("cache_hit", False)
    age = result.get("age_days")
    age_str = f" (cache, {age:.1f}d old)" if cache_hit and age is not None else " (fresh fetch)"
    print(f"# {title}{age_str}", file=sys.stderr)
    print(f"# URL: {args.url}", file=sys.stderr)
    print(f"# MD length: {len(md)} chars", file=sys.stderr)
    print("---", file=sys.stderr)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
