#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build.py — Apache Flink 官方文档全站 BFS 爬取 + 索引构建

设计:
  - 从 SEEDS 出发,同域 BFS 抓 URL
  - 限制只爬 flink.apache.org 与 nightlies.apache.org/flink
  - HTML → markdown(html2text)→ 启发式摘要
  - 输出 4 个索引文件:catalog.json / summary.md / tag_index.json / version_map.json

Usage:
  py -3.12 scripts/build.py                              # 全量
  py -3.12 scripts/build.py --max-pages 100              # 小规模验证
  py -3.12 scripts/build.py --refresh-stale 14           # 刷新 14 天前抓过的
  py -3.12 scripts/build.py --output ./references        # 自定义输出
  py -3.12 scripts/build.py --refresh-posts              # 只刷博客
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

# 依赖(部署时确保:pip install requests beautifulsoup4 html2text lxml)
import requests
from bs4 import BeautifulSoup
import html2text

# 自身依赖
from summarize import summarize_markdown

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

USER_AGENT = "Mavis-FlinkSkill-Builder/1.0 (+contact:mavis-local)"
REQUEST_TIMEOUT = 20
DEFAULT_DELAY = 0.4           # 礼貌延迟,秒
MAX_RETRIES = 2

ALLOWED_DOMAINS = {
    "flink.apache.org",
    "nightlies.apache.org",
}

# 入口种子
SEEDS = [
    # 主站
    "https://flink.apache.org/",
    "https://flink.apache.org/sitemap.xml",
    "https://flink.apache.org/zh/",
    "https://flink.apache.org/zh/sitemap.xml",
    "https://flink.apache.org/posts/",
    "https://flink.apache.org/downloads/",
    "https://flink.apache.org/what-is-flink/flink-architecture/",
    "https://flink.apache.org/what-is-flink/flink-applications/",
    "https://flink.apache.org/what-is-flink/flink-operations/",
    "https://flink.apache.org/what-is-flink/use-cases/",
    # 主仓文档
    "https://nightlies.apache.org/flink/flink-docs-stable/",
    "https://nightlies.apache.org/flink/flink-docs-lts/",
    "https://nightlies.apache.org/flink/flink-docs-master/",
    # 子项目
    "https://nightlies.apache.org/flink/flink-cdc-docs-stable/",
    "https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/",
    "https://nightlies.apache.org/flink/flink-agents-docs-latest/",
    "https://nightlies.apache.org/flink/flink-ml-docs-stable/",
    "https://nightlies.apache.org/flink/flink-statefun-docs-stable/",
]

# 子项目识别(从 hostname / path 推断)
SUBPROJECT_PATTERNS = [
    ("flink-cdc",        re.compile(r"flink-cdc-docs-?")),
    ("flink-k8s-operator", re.compile(r"flink-kubernetes-operator-docs-?")),
    ("flink-agents",     re.compile(r"flink-agents-docs-?")),
    ("flink-ml",         re.compile(r"flink-ml-docs-?")),
    ("flink-statefun",   re.compile(r"flink-statefun-docs-?")),
    ("flink-core",       re.compile(r"flink-docs-?")),  # 主仓
    ("flink-blog",       re.compile(r"/posts/")) ,
    ("flink-website",    re.compile(r"^https?://flink\.apache\.org/")),  # 中英共享, lang 字段独立
]

VERSION_PATTERNS = [
    ("stable", re.compile(r"flink-docs-stable|flink-cdc-docs-stable|flink-kubernetes-operator-docs-stable|flink-ml-docs-stable|flink-statefun-docs-stable")),
    ("lts",    re.compile(r"flink-docs-lts")),
    ("master", re.compile(r"flink-docs-master|flink-cdc-docs-master|flink-kubernetes-operator-docs-main|flink-agents-docs-main|flink-statefun-docs-master|flink-ml-docs-master")),
    ("agents-latest", re.compile(r"flink-agents-docs-latest")),
]

# 排除 path
EXCLUDE_PATH_PATTERNS = [
    re.compile(r"/u/\w+"),             # 用户页
    re.compile(r"/people/"),
    re.compile(r"/events/"),
    re.compile(r"/community/"),        # 社区 mailing list 之类,价值低
    re.compile(r"/sponsorship"),
    re.compile(r"/privacy"),
    re.compile(r"/license"),
    re.compile(r"/thanks"),
]

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

log = logging.getLogger("flink-build")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def normalize_url(url: str) -> str:
    """归一化 URL:去掉 fragment、统一末尾斜杠。"""
    u = urlparse(url)
    path = u.path
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    if not path:
        path = "/"
    return f"{u.scheme}://{u.netloc}{path}"


def is_allowed(url: str) -> bool:
    u = urlparse(url)
    if u.netloc not in ALLOWED_DOMAINS:
        return False
    # nightlies 必须是 /flink/ 路径
    if u.netloc == "nightlies.apache.org" and not u.path.startswith("/flink/"):
        return False
    for pat in EXCLUDE_PATH_PATTERNS:
        if pat.search(u.path):
            return False
    return True


def classify_subproject(url: str) -> str:
    """从 URL 推断子项目。"""
    for name, pat in SUBPROJECT_PATTERNS:
        if pat.search(url):
            return name
    return "other"


def classify_version(url: str) -> str:
    for tag, pat in VERSION_PATTERNS:
        if pat.search(url):
            return tag
    # flink.apache.org 主站:不指版本
    return "main"


def classify_type(url: str) -> str:
    """粗分类:docs-page / blog / download / index / lang-index / sitemap"""
    p = urlparse(url).path
    if p.endswith("/sitemap.xml") or "sitemap" in p:
        return "sitemap"
    if "/posts/" in p and p != "/posts/" and p != "/posts":
        return "blog"
    if "/downloads/" in p and p != "/downloads/":
        return "download"
    if "/api/" in p:
        return "api-reference"
    if "/zh/" in p and (p == "/zh" or p == "/zh/" or p.endswith("/zh/")):
        return "lang-index"
    if p == "/" or p == "/zh/" or p == "/zh":
        return "index"
    # docs 路径通常以 concepts / dev / ops / learn 开头
    if any(seg in p for seg in ("/concepts/", "/dev/", "/ops/", "/learn-flink/", "/try-flink", "/connectors/", "/deployment/", "/flink-architecture", "/flink-applications", "/flink-operations")):
        return "docs-page"
    return "page"


def detect_lang(url: str) -> str:
    return "zh" if "/zh/" in urlparse(url).path or urlparse(url).path == "/zh" else "en"


# ---------------------------------------------------------------------------
# HTML → Markdown
# ---------------------------------------------------------------------------

def html_to_markdown(html: str, base_url: str) -> str:
    """HTML → 干净 markdown。"""
    soup = BeautifulSoup(html, "lxml")

    # 移除干扰
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "form", "button"]):
        tag.decompose()
    # 导航 / 侧栏 / 页脚
    for selector in [
        "nav", "header.site-header", "footer", "aside",
        ".sidebar", ".nav-tabs", ".breadcrumb",
        ".edit-page-link", ".page-nav", ".toc-affix",
        "[role='navigation']", "[aria-label='breadcrumb']",
    ]:
        for el in soup.select(selector):
            el.decompose()

    h = html2text.HTML2Text()
    h.body_width = 0                 # 不自动换行
    h.ignore_images = True
    h.ignore_links = False
    h.protect_links = True
    h.unicode_snob = True           # 用 unicode 字符
    h.bypass_tables = False
    md = h.handle(str(soup))

    # 清理
    md = re.sub(r"\n{3,}", "\n\n", md)
    md = re.sub(r"[ \t]+\n", "\n", md)
    return md.strip()


# ---------------------------------------------------------------------------
# 网络
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,zh;q=0.9",
    })
    return s


def fetch_html(session: requests.Session, url: str) -> Optional[str]:
    """带重试地拉 HTML。失败返回 None。

    关键:强制用 utf-8 / apparent_encoding 解码,避免 requests 默认 ISO-8859-1 把 ® 之类弄成 mojibake。
    """
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200 and r.content:
                # 优先 meta charset → apparent_encoding → utf-8
                # 显式用 bytes 重解码,避免 requests.text 默认 ISO-8859-1
                from_charset = None
                m = re.search(
                    rb'<meta[^>]*charset=["\']?([^"\'\s>]+)',
                    r.content[:1024],
                    re.I,
                )
                if m:
                    from_charset = m.group(1).decode("ascii", errors="ignore").lower()
                # 处理 HTML5 简写
                if from_charset in (None, ""):
                    from_charset = "utf-8"
                try:
                    text = r.content.decode(from_charset, errors="replace")
                except (LookupError, UnicodeDecodeError):
                    text = r.content.decode("utf-8", errors="replace")
                return text
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt)
    log.warning("fetch failed: %s (%s)", url, last_err)
    return None


# ---------------------------------------------------------------------------
# 索引结构
# ---------------------------------------------------------------------------

class Index:
    """内存中的索引,build 结束统一写出。"""

    def __init__(self) -> None:
        self.catalog: list[dict] = []
        self.tag_index: dict[str, list[str]] = defaultdict(list)
        self.version_map: dict[str, str] = {}
        self.summary_lines: list[str] = []
        self.seen_urls: set[str] = set()

    def add(self, page: dict) -> None:
        """page = {url, title, type, subproject, version, lang, summary, tags, last_modified}"""
        self.catalog.append(page)
        for tag in page["tags"] + [page["type"], page["subproject"], page["version"], page["lang"]]:
            if tag and page["url"] not in self.tag_index[tag]:
                self.tag_index[tag].append(page["url"])

    def set_version_root(self, version: str, root_url: str) -> None:
        if version not in self.version_map:
            self.version_map[version] = root_url

    def render_summary_md(self) -> str:
        lines = ["# Flink Official Docs — Page Summaries", ""]
        lines.append("> Generated by build.py. Format: `[<type>] <title> — <summary>`")
        lines.append("")
        # 按 subproject/version 排序
        sorted_catalog = sorted(
            self.catalog,
            key=lambda p: (p["subproject"], p["version"], p["url"])
        )
        current_group = None
        for p in sorted_catalog:
            group = f"{p['subproject']} / {p['version']} / {p['lang']}"
            if group != current_group:
                lines.append("")
                lines.append(f"## `{group}`")
                lines.append("")
                current_group = group
            url = p["url"]
            title = p["title"] or "(no title)"
            summary = p["summary"].replace("\n", " ").strip() or "(no summary)"
            tags = " ".join(f"#{t}" for t in p["tags"][:5])
            lines.append(f"- **[{p['type']}]** [{title}]({url}) — {summary}")
            if tags:
                lines[-1] += f"  \n  {tags}"
        return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 抓取流程
# ---------------------------------------------------------------------------

def extract_links(html: str, base_url: str) -> set[str]:
    """从 HTML 里抽出同域链接,只保留 .html / .htm / 目录 / 无后缀的。"""
    soup = BeautifulSoup(html, "lxml")
    out = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        absolute = urljoin(base_url, href)
        # 去掉 fragment
        absolute = absolute.split("#")[0]
        normalized = normalize_url(absolute)
        if is_allowed(normalized):
            out.add(normalized)
    return out


def crawl(args, index: Index) -> int:
    """主入口。BFS 抓取 + 写索引。"""
    session = make_session()
    queue: list[tuple[str, int]] = [(u, 0) for u in SEEDS]
    visited: set[str] = set()
    pages_done = 0

    while queue and (args.max_pages <= 0 or pages_done < args.max_pages):
        url, depth = queue.pop(0)
        norm = normalize_url(url)
        if norm in visited:
            continue
        visited.add(norm)

        if not is_allowed(norm):
            continue

        # refresh-stale 模式:已抓过且未过期 -> 跳过
        cache_path = args.cache_dir / f"{abs(hash(norm))}.md"
        meta_path = args.cache_dir / f"{abs(hash(norm))}.json"
        if args.refresh_stale > 0 and cache_path.exists() and meta_path.exists():
            age_days = (time.time() - cache_path.stat().st_mtime) / 86400
            if age_days < args.refresh_stale:
                # 复用上次抓的 summary 进 catalog
                try:
                    cached = json.loads(meta_path.read_text(encoding="utf-8"))
                    index.add(cached)
                    pages_done += 1
                    log.info("[CACHED %dd] %s", int(age_days), norm)
                    continue
                except Exception:
                    pass

        # 是否纯博客模式
        if args.refresh_posts and "/posts/" not in norm:
            continue

        # skip sitemap.xml(只用来当种子来源)
        is_sitemap = norm.endswith("sitemap.xml")

        if is_sitemap:
            html = fetch_html(session, norm)
            if html:
                # 从 sitemap 提 URL 入队
                for m in re.finditer(r"<loc>(.*?)</loc>", html):
                    sub = normalize_url(m.group(1))
                    if is_allowed(sub) and sub not in visited:
                        queue.append((sub, depth + 1))
            pages_done += 1
            continue

        html = fetch_html(session, norm)
        if html is None:
            continue

        # 提取并扩展 BFS
        if depth < args.max_depth:
            new_links = extract_links(html, norm)
            for nl in new_links:
                if nl not in visited:
                    queue.append((nl, depth + 1))

        # HTML → markdown
        md = html_to_markdown(html, norm)
        if not md or len(md) < 50:
            pages_done += 1
            continue

        # 摘要 + 元数据
        sm = summarize_markdown(md, title="", url=norm)
        subproject = classify_subproject(norm)
        version = classify_version(norm)
        type_ = classify_type(norm)
        lang = detect_lang(norm)
        tags = _extract_tags(md, norm)

        page = {
            "url": norm,
            "title": sm["title"],
            "summary": sm["summary"],
            "type": type_,
            "subproject": subproject,
            "version": version,
            "lang": lang,
            "tags": sorted(set(tags)),
            "code_langs": sm["code_langs"],
            "last_modified": time.strftime("%Y-%m-%d"),
            "path_depth": depth,
        }
        index.add(page)

        # 版本根
        if type_ in ("index", "docs-page") and "/" == urlparse(norm).path.rstrip("/")[-1:] and version in ("stable", "lts", "master"):
            # 子项目根
            pass
        if norm.endswith("flink-docs-stable/") or "flink-docs-stable/" in norm and norm.count("/") <= 5:
            index.set_version_root("stable-flink-core", norm)
        if "flink-docs-lts" in norm and norm.count("/") <= 5:
            index.set_version_root("lts-flink-core", norm)
        if "flink-docs-master" in norm and norm.count("/") <= 5:
            index.set_version_root("master-flink-core", norm)
        if "flink-cdc-docs-stable" in norm and norm.count("/") <= 5:
            index.set_version_root("stable-flink-cdc", norm)

        # 落缓存
        if not args.no_cache:
            cache_path.write_text(md, encoding="utf-8")
            meta_path.write_text(json.dumps(page, ensure_ascii=False), encoding="utf-8")

        pages_done += 1

        # 周期 flush 到 references/(中断可恢复)
        if args.flush_every > 0 and pages_done % args.flush_every == 0:
            write_outputs(index, args.output)
            log.info("flush checkpoint: %d pages → %s", pages_done, args.output)
        if pages_done % 25 == 0:
            log.info("progress: %d pages done, queue=%d", pages_done, len(queue))
        else:
            log.info("[%d/%d] %s — %s", pages_done, args.max_pages or 0, type_, sm["title"][:60])

        time.sleep(args.delay)

    return pages_done


def _extract_tags(md: str, url: str) -> list[str]:
    """从 markdown 与 URL 提取 tag。"""
    tags = []
    path = urlparse(url).path
    # 章节
    for seg in path.split("/"):
        if seg and seg not in ("docs", "zh", "flink-docs-stable", "flink-docs-lts", "flink-cdc-docs-stable",
                                "flink-kubernetes-operator-docs-stable", "flink-agents-docs-latest",
                                "flink-ml-docs-stable", "flink-statefun-docs-stable", "nightlies.apache.org",
                                "flink.apache.org") and not seg.endswith(".html") and not seg.endswith(".htm"):
            tags.append(seg)
    # 关键词命中
    KW = {
        "checkpoint": ["checkpoint"],
        "savepoint": ["savepoint"],
        "watermark": ["watermark"],
        "window": ["window"],
        "sql": ["sql"],
        "datastream-api": ["datastream"],
        "table-api": ["table api"],
        "state": ["state backends", "stateful"],
        "cdc": ["cdc", "debezium", "change data capture"],
        "k8s": ["kubernetes", "k8s"],
        "agents": ["agents"],
        "ml": ["machine learning", "ml "],
        "statefun": ["stateful functions"],
    }
    lower = md.lower()
    for tag, kws in KW.items():
        for kw in kws:
            if kw in lower:
                tags.append(tag)
                break
    return tags


# ---------------------------------------------------------------------------
# 写出
# ---------------------------------------------------------------------------

def write_outputs(index: Index, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)

    (output / "catalog.json").write_text(
        json.dumps(index.catalog, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output / "tag_index.json").write_text(
        json.dumps(dict(index.tag_index), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output / "version_map.json").write_text(
        json.dumps(index.version_map, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output / "summary.md").write_text(
        index.render_summary_md(),
        encoding="utf-8",
    )

    log.info("wrote %d catalog entries → %s", len(index.catalog), output)
    for fname in ("catalog.json", "tag_index.json", "version_map.json", "summary.md"):
        p = output / fname
        size = p.stat().st_size if p.exists() else 0
        log.info("  %s: %.1f KB", fname, size / 1024)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Build Flink docs offline index")
    parser.add_argument("--output", type=Path, default=Path("./references"),
                        help="输出目录")
    parser.add_argument("--cache-dir", type=Path, default=Path("./cache"),
                        help="页面 markdown 缓存目录")
    parser.add_argument("--max-pages", type=int, default=0,
                        help="最大页面数,0=不限")
    parser.add_argument("--max-depth", type=int, default=6,
                        help="BFS 最大深度")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help="请求间隔(秒)")
    parser.add_argument("--refresh-stale", type=int, default=0,
                        help="refresh 模式:用缓存(天),0=不读缓存")
    parser.add_argument("--refresh-posts", action="store_true",
                        help="只刷博客(/posts/)")
    parser.add_argument("--no-cache", action="store_true",
                        help="不写缓存")
    parser.add_argument("--flush-every", type=int, default=0,
                        help="每 N 页写一次 references/(默认 0 = 只在结尾写)")
    args = parser.parse_args()

    args.cache_dir.mkdir(parents=True, exist_ok=True)

    index = Index()
    log.info("crawl start, seeds=%d, max_pages=%s", len(SEEDS), args.max_pages or "ALL")
    done = crawl(args, index)
    log.info("crawl done, pages=%d", done)

    write_outputs(index, args.output)
    log.info("DONE.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
