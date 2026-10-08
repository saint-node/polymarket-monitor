# Polymarket Monitor — 项目状态文档

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
| 运行环境 | Python 3, VPS（荷兰 Amsterdam，77.90.63.252） |
| 市场数据 | Polymarket Gamma API (`gamma-api.polymarket.com`) |
| 新闻搜索 | Tavily API（按需调用，仅 spike 触发时） |
| LLM 分析 | OpenRouter → `anthropic/claude-sonnet-4.6` |
| 推送 | Telegram Bot API |
| RSS 监听 | Python 内置 `xml.etree.ElementTree`（免费，无 API） |
| 部署 | `nohup` 后台运行，日志写入 `monitor.log` |

---

## 文件结构

```
/Users/youngtrade/Desktop/polymarket/
├── monitor_v5.py     ← 当前生产版本（运行在荷兰 VPS）
└── CLAUDE.md         ← 本文件
```

**VPS 部署路径：** `/root/monitor_v5.py`
**启动命令：** `nohup python3 monitor_v5.py > monitor.log 2>&1 &`
**查看日志：** `cat monitor.log` 或 `tail -f monitor.log`

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

### Track A：价格 Spike 监控

**流程：**
```
每 5 分钟扫描 200 个市场（按成交量排序）
  → 过滤条件（流动性/到期天数/价格区间/分类）
  → 快照对比：当前价格 vs 上次扫描价格
  → 差值 ≥ 2% → spike 触发
  → 3-sigma + 二八定律噪声过滤：
      days_left ≤ 12 且 40% ≤ price ≤ 60% → 跳过（噪声区）
  → 立即搜索新闻（Tavily）
      有新闻 → LLM 传导链分析 → 发送确认信号
      无新闻 → 发送"未确认"信号 → 5分钟后跟进一次
```

**市场分类（三类，排除 other）：**
- `crypto`：BTC/ETH/SEC/ETF 等关键词
- `geopolitics`：war/ceasefire/russia/china/iran 等
- `macro`：fed/inflation/trump/tariff/cpi 等

**新闻搜索设计：**
- 从市场标题提取关键词（去除 Will/by/before 等预测语气词）
- 加入当前月份年份：`March 2026`
- 排除 Polymarket 自身文章：`-polymarket -"prediction market"`
- 按分类设置时效窗口：
  - crypto → 1小时
  - macro → 1.5小时
  - geopolitics → 2.5小时
- 客户端按 `published_date` 过滤，无日期直接丢弃

**LLM 传导链分析（单次调用）：**
- 模型：`anthropic/claude-sonnet-4.6`（via OpenRouter）
- 输入：原始文章（标题+内容片段）+ spike 数据
- 输出：★★★★☆ 及以上链条，最多3条，按置信度排序
- 置信度标准：假设节点数量决定星级（越少越高）
- 语言：链条推理中文，ticker 英文
- 约束：只能使用提供的文章内容，不得引用背景知识

**Telegram 推送格式（确认信号）：**
```
⚡ Price Spike Alert

Market: [市场标题]
Category: [分类]

📈 +2.1%  ·  57.1% → 55.0%
📅 18天  ·  📡 适合 HL 传导链信号

News:
🟢 [标题]
   reuters.com · 03-24 11:28
⚪ [标题]
   coindesk.com · 03-24 11:15

## 传导链分析
Chain 1: ★★★★★
...

🔵 Hyperliquid: https://app.hyperliquid.xyz/trade
🔗 https://polymarket.com/event/[slug]
11:28:22
```

**策略提示逻辑：**
- `days_left ≤ 12` → `⚡ 近期到期 — Poly 均值回归优先`
- `days_left > 12` → `📡 适合 HL 传导链信号`

### Track B：RSS 新闻主动监听

**流程：**
```
每 3 分钟拉取 5 个 RSS 源
  → 本地关键词匹配（无 API 消耗）
  → 发现新文章且命中关键词 → 立即推送预警
  → 首次运行建立基线，不推送历史文章
```

**RSS 源：**
- Reuters Top News
- BBC News
- AP News（via rsshub）
- CoinDesk
- Politico

**实体关键词（三类）：**
- geopolitics：iran/russia/ceasefire/nato/nuclear 等
- macro：federal reserve/powell/tariff/cpi/fomc 等
- crypto：bitcoin/btc/sec crypto/coinbase 等

**推送格式：**
```
📰 Breaking News — Geopolitics · Crypto

🟢 Trump announces Iran ceasefire framework
   Reuters · 03-24 11:28

📌 关注：Geopolitics · Crypto 市场可能联动

🔗 [原文链接]
```

---

## 设计原则与决策记录

### 为什么不让 AI 判断方向（利多/利空）
AI 的方向判断基于训练数据的刻板印象，同一事件在不同市场周期方向可能相反。用户的 edge 在于独立判断方向，AI 只负责梳理逻辑链条。

### 为什么置信度用假设数量而非 AI 主观评分
客观标准，可验证：假设节点越少，推断越直接，置信度越高。避免 AI 主观评分引入偏差。

### 为什么 Track B 不用 Tavily
RSS 是各大新闻源的公开 XML 接口，免费、实时（新文章发布即可拉取），无 API 额度限制。Tavily 只在需要精确搜索特定事件时使用。

### 为什么噪声过滤用 40%-60% 而非 45%-55%
3-sigma 原则 + 二八定律双重验证：概率接近 50% 的市场内在方差最大（Bernoulli σ = 0.5），40%-60% 覆盖了绝大多数噪声来源，45%-55% 不够严格。

### 为什么 pending_news_checks 不覆盖已有记录
同一市场重复 spike 时，若已有 follow-up 在队列中，不重置计时器。防止高频市场导致 follow-up 永远无法触发的 bug。

### 为什么主循环改为 60s 心跳
Track A（5分钟）和 Track B（3分钟）需要独立计时，60s 心跳 + 时间戳比较允许两条轨道真正解耦运行。

---

## 待解决 / 待优化

### 高优先级
1. **crypto 类市场新闻盲区**：当 BTC spike 由地缘/宏观事件驱动时，当前搜索词（基于市场标题）无法找到跨类别新闻。方案：crypto spike 时并行执行第二条搜索（`Bitcoin crypto breaking news {month_year}`）。

2. **多市场联动检测**：大事件往往导致多个类别同时 spike（如特朗普/伊朗消息→BTC涨+伊朗市场跌）。方案：每次扫描收集所有 spike，若涉及 2+ 类别，触发"大事件模式"——一次宽泛搜索 + 一次综合 LLM 分析 + 一条合并推送。

### 中优先级
3. **Polymarket 链接偶发 404**：目前已改用事件级别 slug（`events[0].get("slug")`），成功率提升但仍有偶发失效，原因是部分市场无父级 event 字段。

4. **RSS 关键词精度**：`"trade"` 这类词会产生误报，`"military"` 在体育新闻里也会命中。需要更精确的短语匹配或上下文过滤。

### 低优先级 / 长期方向
5. **Hyperliquid 价格 API**：查询 HL 资产当前价格，判断是否已 priced in，作为信号质量参考。目前是展示层（静态链接），用户手动判断。

6. **Obsidian 知识库接入**：用户正在建立个人金融知识图谱（实体关系映射），成熟后可接入作为动态的实体-市场映射表，替代当前硬编码的关键词列表。这是系统长期 alpha 的核心资产。

7. **信号质量回测**：积累一段时间数据后，统计有新闻确认的信号 vs 无新闻信号的实际表现，验证均值回归和传导链逻辑的有效性。

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

## 版本历史简要

- **v2/v3**：早期版本，在主力 VPS（美国）运行，已停用删除
- **v4**：引入新闻搜索、LLM 传导链分析、Telegram 推送
- **v5（当前）**：
  - 合并两次 LLM 调用为一次
  - 新闻时效按类别动态控制
  - 噪声过滤（3-sigma + 二八定律）
  - Poly 链接改用事件 slug
  - 新闻显示加时间戳和来源域名
  - pending_news_checks 防重置 bug 修复
  - Track B RSS 双轨架构
  - 主循环改为 60s 心跳
