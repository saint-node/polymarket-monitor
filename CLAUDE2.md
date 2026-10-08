# Polymarket Monitor — 项目完整状态文档（2026-03-28）

## 项目概述

监控 Polymarket 预测市场的价格异常波动，结合新闻验证和跨市场传导链分析，通过 Telegram 推送信号，辅助在 Hyperliquid 上做交易决策。

**核心理念：**
- 机器做标准化判断（扫描、过滤、新闻搜索、链条梳理）
- 人做核心判断（方向、仓位、执行）
- 奥卡姆剃刀原则：步骤越少，信息失真越少

---

## 技术栈

| 组件 | 技术 |
|---|---|
| 运行环境 | Python 3，VPS（荷兰 Amsterdam，77.90.63.252） |
| 市场数据 | Polymarket Gamma API (`gamma-api.polymarket.com`) |
| 新闻搜索 | Tavily API（按需调用，仅 spike 触发时） |
| LLM 分析 | OpenRouter → `anthropic/claude-sonnet-4.6` |
| 推送 | Telegram Bot API |
| RSS 监听 | Python 内置 `xml.etree.ElementTree`（免费，无 API） |
| 时间解析 | `email.utils.parsedate_to_datetime`（兼容 RFC 2822） |
| 部署 | `nohup` 后台运行，日志写入 `monitor.log` |

---

## 文件结构

```
/Users/youngtrade/Desktop/polymarket/
├── monitor_v5.py     ← 当前生产版本（同步运行在荷兰 VPS）
├── CLAUDE.md         ← 原始项目文档（旧）
└── CLAUDE2.md        ← 本文件（最新完整状态）
```

**VPS 部署路径：** `/root/monitor_v5.py`
**启动命令：** `nohup python3 monitor_v5.py > monitor.log 2>&1 &`
**查看日志：** `tail -f monitor.log`

---

## 核心参数

```python
SCAN_INTERVAL    = 300     # 市场扫描间隔：5分钟
RSS_INTERVAL     = 180     # RSS 检查间隔：3分钟
SPIKE_THRESHOLD  = 0.02    # spike 触发阈值：2%
MIN_LIQUIDITY    = 2_000   # 最低流动性
MIN_DAYS_LEFT    = 7       # 最少到期天数
PRICE_MIN        = 0.10    # 价格（概率）下限
PRICE_MAX        = 0.90    # 价格（概率）上限
```

---

## 已实现功能

### 主循环架构

- **心跳**：60 秒轮询，Track A 和 Track B 各自用时间戳独立计时，互不干扰
- **启动**：发 Telegram 开机通知 → RSS 建立基线 → 市场价格建立基线（第一次扫描不触发 spike）

---

### Track A：价格 Spike 监控

**完整流程：**

```
每 5 分钟扫描 200 个市场（按成交量排序）
  → 过滤：流动性≥2000 / 剩余天数≥7 / 价格10%-90% / 类别≠other
  → 快照对比：当前价格 vs 上次价格，差值≥2% = spike
  → 噪声过滤：days_left≤12 且 40%≤price≤60% → 跳过（3-sigma+二八定律）
  → 收集本轮所有 spike → 判断分流：
```

**分流逻辑（核心）：**

```
本轮 spike 涉及几个类别？
         ↓
   ┌─────┴──────┐
  1 个类别      2+ 个类别
   ↓                ↓
逐个处理        大事件模式
（原有路径）
```

**单类别路径：**
```
Tavily 搜索（基于市场标题关键词 + 当月年份）
  有新闻 → LLM 传导链分析 → 发"已确认"推送
  无新闻 → 发"待确认"推送 → 加入 pending 队列
           → 5分钟后跟进一次（有/无新闻各一种格式）
           → 同一市场已有 pending 时不重置计时器（防永远无法触发的 bug）
```

**大事件模式（多类别）：**
```
一次宽泛 Tavily 搜索（合并所有市场关键词，时间窗口 2.5h，max_results=7）
→ 一次 LLM 全景传导链分析（所有市场 + 所有新闻一起喂入）
→ 一条合并推送（列出所有受影响市场）
→ 不触发单独推送，不加入 pending 队列
```

**市场分类（三类，排除 other）：**
- `crypto`：bitcoin/btc/ethereum/eth/crypto/sec/etf/solana/coinbase 等
- `geopolitics`：war/ceasefire/sanction/nato/ukraine/russia/china/iran/israel 等
- `macro`：fed/federal reserve/inflation/gdp/cpi/tariff/trump/election 等

**新闻搜索设计：**
- 从市场标题提取关键词（去除 Will/by/before 等预测语气词）
- 加入当前月份年份：`March 2026`
- 排除 Polymarket 自身：`-polymarket -"prediction market"`
- 按分类设置时效窗口：crypto→1h / macro→1.5h / geopolitics→2.5h
- 客户端按 `published_date` 过滤，无日期直接丢弃

**LLM 传导链分析（单次调用）：**
- 模型：`anthropic/claude-sonnet-4.6`（via OpenRouter）
- 输入：原始文章（标题+内容片段）+ spike 数据，直接投入，无中间摘要层
- 输出：★★★★☆ 及以上链条，最多 3 条，按置信度排序
- 置信度标准：假设节点数量（0个=★★★★★，1个=★★★★☆）
- 约束：只能使用提供的文章内容，不得引用背景知识
- 不判断方向（涨/跌），由用户决定

---

### Track B：RSS 新闻主动监听

**完整流程：**
```
每 3 分钟拉取 5 个 RSS 源
  → URL 去重（seen_rss_articles）
  → 新文章 → 标题匹配关键词？
      无命中 → 丢弃
      命中 → 判断类别 → 推送
  → 首次运行建立基线，不推送历史文章
```

**RSS 源：**
- Reuters Top News
- BBC News
- AP News（via rsshub）
- CoinDesk
- Politico

**实体关键词（三类）：**
- geopolitics：iran/russia/ukraine/china/ceasefire/nato/nuclear/missile 等
- macro：federal reserve/powell/tariff/cpi/fomc/rate cut/rate hike 等
- crypto：bitcoin/btc/ethereum/crypto/sec crypto/coinbase/stablecoin 等

---

## 消息格式与设计风格

### Track B — RSS 推送格式

**标题规则（按内容类别，不按来源权威性）：**

| 命中类别 | 标题 |
|---|---|
| crypto only | `Crypto Update` |
| geopolitics only | `Geopolitics Flash` |
| macro only | `Policy Alert` |
| 2+ 类别同时命中 | `Breaking News` |

**格式（来源在上，标题在下，无 🟢/⚪ 图标）：**
```
📰 *Geopolitics Flash*

Reuters · 03-24 11:28
Inside the alleged Russian operation to trigger anti-government protests

🔗 https://...
```

### Track A — Spike 推送格式

**已确认 spike（单市场）：**
```
⚡ *Price Spike Alert*

*Market:* Will BTC hit $100K by April?
*Category:* Crypto

`📈 +2.1%  ·  57.1% → 59.2%`
`📅 18天  ·  📡 适合 HL 传导链信号`

*News:*
🟢 Trump signs Iran sanctions order, Bitcoin surges
   reuters.com · 03-24 11:28
⚪ BTC approaches resistance after macro shift
   coindesk.com · 03-24 11:15

Chain 1: ★★★★★
...

🔵 *Hyperliquid:* https://app.hyperliquid.xyz/trade
🔗 https://polymarket.com/event/[slug]
_11:28:22_
```

**待确认 spike（无新闻）：**
```
⚡ *Price Spike — Unconfirmed*

*Market:* ...
⚠️ *无新闻 — 信号待确认*
→ 可能是知情早盘或情绪驱动
→ 5 分钟后跟进
```

**大事件合并推送（多市场）：**
```
🌐 *Multi-Market Event — Crypto · Geopolitics*

*Markets:*
• Will BTC hit $100K by April?  `+2.3%` (41%→43%)
• Will Iran nuclear deal happen by June?  `-3.1%` (52%→49%)

*News:*
🟢 Trump signs Iran sanctions, Bitcoin surges
   reuters.com · 03-24 11:28

Chain 1: ★★★★★
...

🔵 *Hyperliquid:* https://app.hyperliquid.xyz/trade
🔗 https://polymarket.com/event/[slug-1]
🔗 https://polymarket.com/event/[slug-2]
_11:28:22_
```

**策略提示逻辑：**
- `days_left ≤ 12` → `⚡ 近期到期 — Poly 均值回归优先`
- `days_left > 12` → `📡 适合 HL 传导链信号`

**图标使用规则：**
- 🟢/⚪ 仅用于 Track A spike 推送中的新闻来源标注（权威/非权威）
- Track B RSS 推送不使用 🟢/⚪

---

## API 与服务

| 服务 | 用途 | 额度/成本 |
|---|---|---|
| Polymarket Gamma API | 市场数据 | 免费，无 key |
| Tavily API | 新闻搜索 | 免费 1000次/月，按 spike 触发 |
| OpenRouter | LLM 调用 | 按 token 计费 |
| Telegram Bot API | 推送 | 免费 |
| RSS Feeds | 主动新闻监听 | 完全免费 |

---

## 待解决 / 待优化

### 高优先级

1. **Crypto 盲区（长期方案）**
   BTC spike 由地缘/宏观事件驱动时，基于市场标题的搜索词无法找到跨类别新闻。
   - 短期：大事件模式（多类别联动）已覆盖大多数情况（BTC 流动性驱动天然跨资产）
   - 长期根本方案：接入 Obsidian 知识图谱，建立实体→资产映射表，替代硬编码关键词

2. **RSS 关键词误报**
   `"trade"` 误匹配商业新闻，`"military"` 在体育新闻中命中。
   需要短语匹配或上下文过滤。

### 中优先级

3. **Polymarket 链接偶发 404**
   已改用事件级别 slug（`events[0].get("slug")`），成功率提升，但部分市场无父级 event 字段仍有失效。

### 低优先级 / 长期方向

4. **Hyperliquid 价格 API**
   查询 HL 资产当前价格，判断是否已 priced in，作为信号质量参考。目前为静态链接，用户手动判断。

5. **Obsidian 知识图谱接入**
   用户正在建立个人金融知识图谱（实体关系映射），成熟后替代当前硬编码关键词列表。这是系统长期 alpha 的核心资产，也是 crypto 盲区的根本解决方案。

6. **信号质量回测**
   积累数据后，统计有新闻确认 vs 无新闻信号的实际表现，验证均值回归和传导链逻辑的有效性。

---

## 设计原则与决策记录

**为什么不让 AI 判断方向（利多/利空）**
AI 方向判断基于训练数据的刻板印象，同一事件在不同市场周期方向可能相反。用户的 edge 在于独立判断方向，AI 只负责梳理逻辑链条。

**为什么置信度用假设数量而非 AI 主观评分**
客观标准，可验证：假设节点越少，推断越直接，置信度越高。

**为什么 Track B 不用 Tavily**
RSS 是各大新闻源的公开 XML 接口，免费实时，无额度限制。Tavily 只在需要精确搜索特定事件时使用。

**为什么噪声过滤用 40%-60% 而非 45%-55%**
3-sigma 原则 + 二八定律：概率接近 50% 的市场内在方差最大（Bernoulli σ=0.5），40%-60% 覆盖绝大多数噪声来源。

**为什么 pending_news_checks 不覆盖已有记录**
防止高频市场导致 follow-up 永远无法触发的 bug：同一市场重复 spike 时，若已有 follow-up 在队列中，不重置计时器。

**为什么主循环改为 60s 心跳**
Track A（5分钟）和 Track B（3分钟）需要独立计时，60s 心跳 + 时间戳比较允许两条轨道真正解耦运行。

**为什么大事件模式不加入 pending 队列**
多类别 spike 已做宽泛搜索，无新闻时说明事件本身驱动不明确，不适合跟进；且合并推送已充分传递信息。

**为什么 Crypto 盲区暂不加兜底搜索**
BTC 当前是流动性驱动而非单一事件驱动，资金流动必然跨资产，大概率会同时触发多类别市场波动，由大事件模式覆盖。泛泛的 "Bitcoin crypto breaking news" 查询时效性存疑，效果不如等 Obsidian 知识图谱成熟后接入。

**为什么 RSS 标题按类别而非来源权威性区分**
Reuters、BBC、CoinDesk 都是正规媒体，权威性分层在 RSS 场景没有实际价值。类别标题（Crypto Update / Geopolitics Flash 等）信息密度更高，一眼知道该关注什么市场。

---

## 版本历史

- **v2/v3**：早期版本，美国 VPS，已停用删除
- **v4**：引入新闻搜索、LLM 传导链分析、Telegram 推送
- **v5（当前）**：
  - 两次 LLM 调用合并为一次（原始文章直接投入，无中间摘要层）
  - 新闻时效按类别动态控制
  - 噪声过滤（3-sigma + 二八定律）
  - Poly 链接改用事件 slug
  - 新闻显示加时间戳和来源域名
  - pending_news_checks 防重置 bug 修复
  - Track B RSS 双轨架构
  - 主循环改为 60s 心跳
  - **多市场联动检测**：2+ 类别触发大事件模式，一次搜索+分析+合并推送
  - **RSS 格式重构**：来源在上/标题在下，类别驱动标题，去除冗余图标和文字
  - **时间解析 bug 修复**：兼容 RFC 2822 格式（BBC 等来源的 pubDate）
