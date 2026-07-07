#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
summarize.py — 启发式页面摘要

设计目标:
  - 不依赖 LLM,构建时离线可用
  - 输出 1~3 行中文/英文摘要,足以让 LLM 决定"这页要不要拉"
  - 提取首段(过滤导航/广告/代码块),加 h1 + 关键标签

约束:
  - 摘要字数 ≤ 240 字
  - 不抓代码块(代码块单独标记)
  - 跨语言保留原文,不翻译

Usage:
  from summarize import summarize_markdown
  s = summarize_markdown(markdown_text, title="...", url="...")
"""

from __future__ import annotations

import re
from typing import Optional

# 常见导航/广告/版权段过滤
NOISE_PATTERNS = [
    r"^\s*Skip to main content\s*$",
    r"^\s*This (page|website|site).*uses? cookies?\s*$",
    r"^\s*Edit this page\s*$",
    r"^\s*Was this (page|article|helpful).*$",
    r"^\s*Previous\s*\|?\s*Next\s*$",
    r"^\s*(Copyright|License|The contents of).*Apache.*$",
]

NOISE_RE = re.compile("|".join(NOISE_PATTERNS), re.IGNORECASE)

# 代码块
CODE_FENCE_RE = re.compile(r"^```.*?$", re.MULTILINE)
CODE_BLOCK_RE = re.compile(r"```[\s\S]*?```", re.MULTILINE)

# 链接 [text](url) 转换:只保留文本
LINK_RE = re.compile(r"\[([^\]]*)\]\([^\)]*\)")

# 标题
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)


def _strip_noise_lines(text: str) -> str:
    """逐行去掉导航/广告/版权。"""
    kept = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if NOISE_RE.match(s):
            continue
        kept.append(line)
    return "\n".join(kept)


def _clean_for_summary(md: str) -> str:
    """去代码块、去链接尾 URL、去多余空白。"""
    md = CODE_BLOCK_RE.sub("", md)  # 整段代码块
    md = LINK_RE.sub(r"\1", md)     # [text](url) → text
    # 删掉孤立的链接 URL 行
    md = re.sub(r"^\s*https?://\S+\s*$", "", md, flags=re.MULTILINE)
    md = _strip_noise_lines(md)
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip()


def _extract_first_paragraph(md: str, max_chars: int = 240) -> str:
    """取第一个非标题的段落,截断到 max_chars。"""
    paragraphs = re.split(r"\n\s*\n", md)
    for p in paragraphs:
        p = p.strip()
        if not p:
            continue
        # 跳过纯标题
        if HEADING_RE.match(p) and "\n" not in p:
            continue
        # 跳过过短段落
        if len(p) < 15:
            continue
        return p[:max_chars].rstrip() + ("…" if len(p) > max_chars else "")
    # 兜底:返回首个非空行
    for line in md.splitlines():
        line = line.strip()
        if line and not HEADING_RE.match(line) and len(line) > 15:
            return line[:max_chars]
    return ""


def _extract_h1(md: str) -> Optional[str]:
    m = re.search(r"^#\s+(.*?)$", md, re.MULTILINE)
    return m.group(1).strip() if m else None


def _extract_code_lang_hints(md: str) -> list[str]:
    """检测出现的语言,用于 catalog 的 tags。"""
    langs = set()
    for m in re.finditer(r"^```(\w+)", md, re.MULTILINE):
        langs.add(m.group(1).lower())
    return sorted(langs)


def summarize_markdown(md: str, title: str = "", url: str = "") -> dict:
    """
    返回 dict:
      {
        "title": "...",            # 优先 h1,其次入参 title
        "summary": "...",          # 1~3 行摘要
        "code_langs": [...],       # 出现的代码语言
      }
    """
    h1 = _extract_h1(md)
    final_title = h1 or title or ""

    cleaned = _clean_for_summary(md)
    body_summary = _extract_first_paragraph(cleaned)

    return {
        "title": final_title,
        "summary": body_summary,
        "code_langs": _extract_code_lang_hints(md),
    }


if __name__ == "__main__":
    # 自测
    sample = """
# Checkpoint vs Savepoint

This page describes the differences between checkpoint and savepoint in Apache Flink.

## Overview

A checkpoint is an automatic, periodic snapshot of an application's state...

```java
env.enableCheckpointing(60000);
```

[Edit this page](https://github.com/...)

Was this page helpful?

## Next
"""
    import json
    print(json.dumps(summarize_markdown(sample, title="Fallback", url="..."), ensure_ascii=False, indent=2))
