#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
search.py — 在 references/ 索引里检索候选 URL

匹配策略:
  1. 关键词命中(标题 + tags + summary 全文)
  2. 子项目 / 版本 / 语言过滤
  3. 评分:title 命中 > tag 命中 > summary 命中;URL 子串命中 + 1
  4. 返回 top N(默认 5),带 confidence 提示

Usage:
  py -3.12 scripts/search.py "checkpoint savepoint"
  py -3.12 scripts/search.py "MySQL CDC" --subproject flink-cdc --version stable
  py -3.12 scripts/search.py "watermark" --lang en --top 10
  py -3.12 scripts/search.py "Table API" --type docs-page
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

INDEX_DIR = Path(__file__).resolve().parent.parent / "references"


STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "of", "in", "on", "to",
    "for", "and", "or", "by", "with", "as", "at", "be", "this", "that",
    "from", "do", "does", "how", "what", "which", "who", "where", "when",
    "it", "its", "i", "you", "your",
}

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_\-]{1,}|\b[\u4e00-\u9fff]+\b")


def load_index() -> dict:
    """读 catalog.json + version_map.json。"""
    cat_path = INDEX_DIR / "catalog.json"
    ver_path = INDEX_DIR / "version_map.json"
    if not cat_path.exists():
        return {"catalog": [], "version_map": {}}
    catalog = json.loads(cat_path.read_text(encoding="utf-8"))
    vm = {}
    if ver_path.exists():
        vm = json.loads(ver_path.read_text(encoding="utf-8"))
    return {"catalog": catalog, "version_map": vm}


def tokenize(text: str) -> list[str]:
    """英文小写 + 中文按字保留。"""
    tokens = []
    for tok in TOKEN_RE.findall(text or ""):
        if tok.isascii():
            low = tok.lower()
            if low in STOPWORDS or len(low) < 2:
                continue
            tokens.append(low)
        else:
            tokens.append(tok)
    return tokens


def score_page(page: dict, query_tokens: list[str], query_str: str) -> float:
    """给一个页面打分。返回 -1 表示不命中。"""
    if not query_tokens:
        return -1.0

    title_l = (page.get("title") or "").lower()
    summary_l = (page.get("summary") or "").lower()
    tags = [t.lower() for t in page.get("tags", [])]
    url_l = page.get("url", "").lower()

    title_tokens = set(tokenize(page.get("title", "")))
    sum_tokens = set(tokenize(page.get("summary", "")))

    # 整串命中(强信号)
    if query_str and len(query_str) >= 3:
        if query_str.lower() in title_l:
            return 100.0
        if query_str.lower() in summary_l:
            return 50.0
        if any(query_str.lower() in t for t in tags):
            return 80.0

    # token 命中累计
    score = 0.0
    hits_title = 0
    hits_tag = 0
    hits_summary = 0
    hits_url = 0
    for tok in query_tokens:
        if tok in title_tokens:
            score += 8.0
            hits_title += 1
        if tok in tags:
            score += 5.0
            hits_tag += 1
        if tok in sum_tokens:
            score += 2.0
            hits_summary += 1
        if tok in url_l:
            score += 3.0
            hits_url += 1

    # 至少有一个命中
    if score == 0:
        return -1.0

    # bonus:全部 token 都命中 → 更相关
    distinct_hits = sum(1 for t in query_tokens if t in title_tokens | set(tags) | sum_tokens | {url_l})
    if distinct_hits == len(query_tokens):
        score *= 1.5

    return score


def filter_pages(
    catalog: list[dict],
    subproject: Optional[str] = None,
    version: Optional[str] = None,
    lang: Optional[str] = None,
    type_: Optional[str] = None,
) -> list[dict]:
    out = []
    for p in catalog:
        if subproject and p.get("subproject") != subproject:
            continue
        if version:
            # 版本 alias:lts 同时匹配 lts / lts-old
            pv = p.get("version") or ""
            if version in ("stable", "lts", "master", "main"):
                if not pv.startswith(version):
                    continue
            elif pv != version:
                continue
        if lang and p.get("lang") != lang:
            continue
        if type_ and p.get("type") != type_:
            continue
        out.append(p)
    return out


def search(
    query: str,
    catalog: list[dict],
    top: int = 5,
    subproject: Optional[str] = None,
    version: Optional[str] = None,
    lang: Optional[str] = None,
    type_: Optional[str] = None,
    min_score: float = 1.0,
) -> list[tuple[dict, float]]:
    query_tokens = tokenize(query)
    candidates = filter_pages(catalog, subproject, version, lang, type_)
    scored = []
    for p in candidates:
        s = score_page(p, query_tokens, query)
        if s >= min_score:
            scored.append((p, s))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top]


# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Search Flink docs local index")
    p.add_argument("query", help="查询字符串")
    p.add_argument("--top", type=int, default=5, help="返回 top N")
    p.add_argument("--subproject", help="限定子项目(flink-core / flink-cdc / flink-blog / ...)")
    p.add_argument("--version", help="限定版本(stable / lts / master / agents-latest)")
    p.add_argument("--lang", choices=["en", "zh"], help="限定语言")
    p.add_argument("--type", dest="type_", help="限定 type(docs-page / blog / api-reference / index ...)")
    p.add_argument("--min-score", type=float, default=2.0, help="最低分阈值")
    p.add_argument("--json", action="store_true", help="JSON 输出")
    args = p.parse_args()

    idx = load_index()
    if not idx["catalog"]:
        print("[!] catalog.json 不存在或为空。先跑 build.py:", file=sys.stderr)
        print("    py -3.12 scripts/build.py --output ./references", file=sys.stderr)
        return 1

    results = search(
        args.query,
        idx["catalog"],
        top=args.top,
        subproject=args.subproject,
        version=args.version,
        lang=args.lang,
        type_=args.type_,
        min_score=args.min_score,
    )

    if args.json:
        out = [
            {
                "rank": i + 1,
                "score": round(s, 1),
                "url": r["url"],
                "title": r.get("title", ""),
                "subproject": r["subproject"],
                "version": r["version"],
                "lang": r["lang"],
                "type": r["type"],
                "summary": r.get("summary", ""),
            }
            for i, (r, s) in enumerate(results)
        ]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    if not results:
        print(f"[!] no hits for: {args.query!r}", file=sys.stderr)
        print("    try lowering --min-score or removing --filter", file=sys.stderr)
        return 0

    print(f"# results for: {args.query!r}\n")
    for i, (r, s) in enumerate(results, 1):
        title = r.get("title", "(no title)")[:70]
        summary = r.get("summary", "").replace("\n", " ")[:140]
        print(f"## [{i}] score={s:.1f} — {title}")
        print(f"  URL       : {r['url']}")
        print(f"  subproj   : {r['subproject']} / version={r['version']} / lang={r['lang']} / type={r['type']}")
        print(f"  tags      : {', '.join(r.get('tags', [])[:6])}")
        print(f"  summary   : {summary}…")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
