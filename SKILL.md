---
name: flink-skill
description: |
  Apache Flink 官方文档与生态(核心 / CDC / Kubernetes Operator / Agents / ML / Statefun)的本地索引 + 按需检索入口。
  触发条件:用户提到 Flink,或询问其概念、API、配置、SQL、部署、版本差异、排错等需要引用官方内容的场景。
  严格基于 flink.apache.org 与 nightlies.apache.org/flink 的官方文档与博客,
  任何引用必须标注 Version(stable=2.3 / LTS=1.20 / master)与原文 URL。
version: 1.0.0
---

# Flink Official Site Skill

把 Apache Flink 官方文档体系压缩成可被 LLM 代理按需调用的索引入口。
设计上把"轻量索引"(references/)与"按需拉取"(scripts/)解耦,使主上下文永远只关心地图,不背文档正文。

## 何时启用

- 用户提到 **Flink**、**DataStream**、**Table/SQL API**、**Checkpoint**、**Savepoint**、**Watermark**、**ProcessFunction**、**Window**
- 用户提到 **Flink CDC**、**Kubernetes Operator**、**Agents**、**ML**、**Stateful Functions** 任一子项目
- 用户问"flink 1.x 和 2.x 区别"、"Flink SQL 怎么做 XX"、"Flink 怎么部署到 K8s"这类问题
- 排错:用户贴一段 Flink 异常 / 配置,需要查官方定位

## 不要启用

- 通用 Java / 大数据问题,不限定 Flink
- 用户只想聊架构 / 概念科普,不要 reference
- Kafka / Spark / Beam 单话题

## 工作流(标准动作)

### 步骤 1 — 定位(local index lookup)

读 `references/catalog.json` 或 `references/summary.md`,用关键词匹配找出候选 URL。
可直接调用:`py -3.12 scripts/search.py "<query>" [--subproject cdc] [--version lts]`。

匹配从这些字段下手:
- URL path(子项目 / 章节)
- title(标题)
- tags / subproject
- summary(每页一句话)

### 步骤 2 — 版本确认

Flink 文档**对版本敏感**。响应里必须明确标注:

| 别名   | 含义              | 当前版本                  |
|--------|-------------------|---------------------------|
| stable | 当前 stable release | Flink 2.3               |
| lts    | 长期支持           | Flink 1.20               |
| master | 主干快照           | nightlies-docs-master    |

用户未指定时,默认 `stable`。涉及兼容性 / 弃用 API 时,同步给 `lts`。

### 步骤 3 — 按需拉取(content fetch)

对每个候选 URL:`py -3.12 scripts/fetch_page.py <url>`。

- 优先读 `cache/`(7 天有效,基于 `Last-Modified` 或文件 mtime)
- 缓存未命中 → 联网拉,HTML → markdown,落 `cache/`
- 失败兜底 → 仅返回 summary.md 那一行,提示用户当前 fetch 受阻

### 步骤 4 — 回答

**铁律**:
1. 任何事实必须 → `<URL>` + `Version: X.Y`
2. 跨版本差异 → 并列出两版本各自段落,**不混合叙述**
3. 官方未覆盖 → 直说"官网未提供相关信息",**不臆造**
4. API 签名 / 配置项 → 以 docs reference 为准,博客里"将要支持"不算

回答结构建议:
```
[结论 1~3 行]
[引用段落 1]:<URL>
[引用段落 2]:<URL>
引用版本:Version: <version>
```

## 资源说明

### references/(构建产物,会进上下文)
- `catalog.json` — 全站 URL 树 + 元数据(title, type, subproject, version, lang, last_modified)
- `summary.md` — 每页 1~2 行摘要(LLM 友好,**先读这个**)
- `tag_index.json` — tag → URLs 反向索引
- `version_map.json` — stable / lts / master → 文档根 URL 映射

### scripts/(运行时工具)
- `build.py` — 全站 BFS 爬取 + 索引构建(一次性)
- `fetch_page.py` — 单页拉取 + 缓存(运行时)
- `search.py` — 本地索引检索(运行时)
- `summarize.py` — 启发式摘要提取(无 LLM 依赖)

### cache/(临时产物)
- `<sha1(url)>.md`,7 天过期

## 常见用法速查

| 场景                                | 命令/动作                                                                     |
|-------------------------------------|-------------------------------------------------------------------------------|
| 查概念解释                          | `search.py "<query>"` → fetch top 3                                           |
| 查 API 签名 / 配置示例              | 加 `--type api-reference` 或 `--type config` 过滤                            |
| 跨版本对比                          | 分别用 `--version stable` `--version lts` 各 fetch 一次                       |
| 只想看大纲不拉正文                  | 只读 `summary.md` 中对应行                                                   |
| 子项目导航                          | `browse <subproject>` — 列出该子项目所有章节 URL                              |
| 中英对照                            | URL 含 `/zh/` 的页面已在 catalog 标注 lang=zh;默认回答用英文,引用中文版可加 `--lang zh` |

## 约束 / 边界

- **不爬非 Apache 域名**:Linkedin、Twitter、Confluence 第三方一律跳过
- **不抓 user/people 页**:`/u/<username>` 这类链接不进索引
- **博客优先新鲜度**:`/posts/` 路径下的内容建议直接 fetch,不走缓存(版本可能已变)
- **API 类变更极高频**:`docs/api/` 类页面默认缓存期 3 天而非 7 天

## 烟雾测试清单(smoke test)

跑通下面 5 个场景算 skill 健康:

1. `search.py "checkpoint vs savepoint"` → 返回 concepts/stateful 下 2~3 URL
2. `search.py "MySQL CDC connector"` → 返回 flink-cdc 下 connector/mysql 章节
3. `fetch_page.py "https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful/"` → 落缓存 + 输出 markdown
4. `catalog.json` 包含 ≥ 1 个 stable / lts / master 三版本样本
5. `summary.md` 每页 1~2 行,无空段

## 重新构建

```bash
# 小规模(快速验证,约 100 页,~2 分钟)
py -3.12 scripts/build.py --max-pages 100 --output ./references

# 全量(全站 3000+ URL,30~60 分钟)
py -3.12 scripts/build.py --output ./references

# 增量(刷新 last_modified 较旧的页面)
py -3.12 scripts/build.py --refresh-stale 14 --output ./references
```

## 维护

- 新 release 公告(博客)出现时:`build.py --refresh-posts` 单独刷博客
- 子项目独立发版:加新的种子 URL 到 `build.py` SEEDS,重跑
- catalog 漂移(`version_map.json` 与实际 nightly 路径对不上):把 nightly 路径覆盖到 scripts/build.py
