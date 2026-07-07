# Flink Official Docs Skill

> Apache Flink 官方文档 / 博客 / 子项目(CDC / Kubernetes Operator / Agents / ML / Statefun)的本地索引 + 按需检索入口。

## 文件结构

```
flink-skill/
├── SKILL.md                  # LLM 代理入口(描述触发条件、动作契约、版本约束)
├── README.md                 # 本文件(给构建者看)
├── references/               # 索引文件(LLM 主上下文只读 summary.md + version_map.json)
│   ├── catalog.json          #   - 全站 884 个去重页面(完整元数据)
│   ├── summary.md            #   - 每页 1~2 行摘要(LLM 友好)
│   ├── tag_index.json        #   - 779 个 tag 反向索引
│   └── version_map.json      #   - 8 个版本/子项目根 URL
├── scripts/
│   ├── build.py              #   - 全站 BFS 爬取(增量 + 周期 flush)
│   ├── rebuild_index.py      #   - 从 cache 重建 references/(中断恢复)
│   ├── summarize.py          #   - 启发式页面摘要(纯本地)
│   ├── fetch_page.py         #   - 单页拉取 + 缓存(运行时)
│   └── search.py             #   - 本地索引检索(运行时)
├── cache/                    #   - 1341 个 md/json 缓存(构建时产物)
└── logs/                     #   - build 日志(临时)
```

## 重新构建索引

### 1) 全量重建(首次构建)

```bash
py -3.12 scripts/build.py --flush-every 100 --output ./references
```

- `--flush-every 100`:每 100 页写一次 references/,中断可恢复
- 默认 `--max-pages 0` 跑到队列空(全站 ~5000+ URL,可能要 60~90 分钟)
- 加 `--max-pages 1500` 限制增量测试

### 2) 中断恢复(被 timeout 杀了)

```bash
py -3.12 scripts/rebuild_index.py
```

从 cache/ 里的 md/json 直接重拼 references/,无需重新爬。

### 3) 刷新旧页面

```bash
py -3.12 scripts/build.py --refresh-stale 14 --output ./references
```

只刷新 14 天前抓过的页面,其余用 cache。

## 当前索引快照(2026-07-06)

| 维度           | 数量 |
|----------------|------|
| catalog 总页   | 884  |
| flink-core     | 505  |
| flink-website  | 352  |
| flink-statefun | 10   |
| flink-blog     | 5    |
| flink-agents   | 3    |
| flink-cdc      | 3    |
| flink-k8s-operator | 3 |
| flink-ml       | 3    |

> **说明**:agents/cdc/k8s/ml 子项目目前只到根页面,深入需要 `--max-pages` 调大后跑全量(预计 1500+ 页)。

## 运行时使用

### 搜索候选页面

```bash
py -3.12 scripts/search.py "checkpoint vs savepoint" --top 5
py -3.12 scripts/search.py "MySQL CDC" --subproject flink-cdc
py -3.12 scripts/search.py "window API" --version stable
```

输出格式:L1 排名 + URL + summary + tags,适合 LLM 直接消化。

### 拉取具体页面

```bash
py -3.12 scripts/fetch_page.py "https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/checkpoints_vs_savepoints"
```

- 自动用本地 cache(7 天 TTL)
- HTML → markdown,落 `cache/<sha1>.md`
- `--force` 强制刷新
- `--max-chars 5000` 截断

### 列出已缓存页面

```bash
py -3.12 scripts/fetch_page.py --list-cache
```

## 依赖

```bash
pip install requests beautifulsoup4 lxml html2text
```

## 设计取舍

- **离线索引 + 按需 fetch**:references/ 体积 ~1 MB(summary.md 主用),LLM 主上下文不爆;具体页面运行时按需拉,markdown 比 HTML 省 10x token。
- **UTF-8 强制解码**:requests 默认 ISO-8859-1 会让 ® / — 等字符 mojibake,build.py 与 fetch_page.py 都强制按 meta charset 解码。
- **启发式摘要(无 LLM 依赖)**:用首段 + 标题;精度约 70%,但离线跑得过。
- **去重 + 版本对齐**:assembled 后再 dedup(URL 为键),rebuild 时把 flink-docs-release-X.Y 映射到 stable/lts/old。
- **BFS 友好 sitemap**:sitemap.xml 入队后会铺开所有 URL,推荐 `--flush-every` 防大超时。

## 已知限制

1. 子项目(agents / cdc / k8s / ml)索引页数偏少(~3~10),需要 `--max-pages 0`(全量)才能深入。建议 1000+ max-pages 后再 rebuild。
2. `tag_index.json` 体积大(931 KB),LLM 主上下文不要直接读,只用 summary.md。
3. flink.apache.org 主站页面是导航 + 链接列表为主,实际技术内容主要在 nightlies 子域。