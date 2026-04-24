"""
Polymarket Monitor v6 — 研究模式 (三角色架构: rules-only / llm-only / human-only)

本文件按 blueprint §1-§9 分节编写，分里程碑渐进式落地：

进度:
  M1 ✅ 数据管道最小闭环: fetch_markets -> passes_mg -> check_spike -> prune_price_history
  M2 ✅ RC (sort_candidates + in_cooldown + rate_limit_allows)
  M3 ✅ Sonar + LLM (search_news_sonar + llm_judge)
  M4 ✅ Supabase 四表持久化 + Bot A (§4 DB helpers + §6 HTML + inline keyboard)
  M5 ✅ aiohttp Webhook + Bot B (§7 handle_webhook + 共享事件循环)
  M6 ✅ Startup 消息 + 自动 setWebhook + 结算回填 (send_startup + set_webhook + resolution_loop)

M4 产出: LLM 成功后串行执行持久化 → Bot A：
         upsert markets (失败即终止, 不发 Bot A) →
         upsert spikes  (失败即终止, 不发 Bot A) →
         if sonar=ok: insert articles (best-effort, 失败仅日志继续) →
         send Bot A (HTML + 一行四按钮 callback_data=j=..|s=<spike_id>) →
         update bot_a_sent_at (best-effort) + RC 记账 (cooldown_state + rate_state)
         RC 记账点从 M2 的 RC pass 正式迁移到 Bot A 成功后 (blueprint §8 约定)。
         任一终止分支均不计 RC (不占冷却槽, 不占每小时配额)。

M5 产出: aiohttp webhook 与 scan_loop 共享同一事件循环 (asyncio.gather)。
         POST {WEBHOOK_URL path} 收到 callback_query 后固定顺序 (bota-human plan §交互通道):
           1. 校验 header X-Telegram-Bot-Api-Secret-Token == WEBHOOK_SECRET_TOKEN
           2. 解析 callback_data=j=<abbr>|s=<spike_id>, 非法 -> 400/忽略 + 日志
           3. answerCallbackQuery 立即消除按钮 loading 态
           4. 后台 task: fetch_bot_b_context (spikes JOIN markets + articles + 最新
              human_click) -> send_bot_b (只读 HTML, 无按钮) -> insert_human_click
              (best-effort, 失败仅日志不阻断)
           5. webhook 响应 200 OK
         spike 未命中: 跳过 Bot B, 仍尝试 insert_human_click (防御性, FK 失败仅日志)。
         多次改判: 每次点击产生独立的 Bot B 回显 + 独立的 human_clicks 行
                  (append-only, Bot B 始终显示本次点击的 judgment)。

M6 产出: amain() 入口三件事 (blueprint §9 终态):
         1. send_startup: 通过 Bot A / Bot B 两个 token 向同一 TELEGRAM_CHAT_ID
            各发一条 HTML 配置摘要 (rules-only plan §Startup 消息冻结模板),
            失败仅 WARN, 不阻断启动
         2. set_webhook: 仅注册 Bot A 的 webhook
            (POST /bot<token>/setWebhook url=<WEBHOOK_URL> secret_token=<...>),
            Bot B 无入站通道 (llm-botb plan §Telegram 通道)
         3. asyncio.gather(scan_loop, serve_webhook, resolution_loop) 三协程
            共享同一事件循环

         resolution_loop (observability plan §结算任务, 每 24h):
           a. SELECT markets WHERE resolution_status='open' AND end_date < today
           b. 逐个 GET {GAMMA_API}/markets/{market_id}
           c. 若 gamma.closed=true 且 outcomePrices[0] ∈ {0.0, 1.0}
              -> UPDATE resolution_status='resolved' + resolution_result∈{YES,NO}
              否则保持 open, 下一轮再查 (blueprint §未结算处理)
         best-effort: 任何失败 (网络/解析/DB) 仅日志, 不阻断 scan_loop/serve_webhook。
"""

# ── §1 Imports ───────────────────────────────────────────────────────────────
import asyncio
import html
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from dotenv import load_dotenv
from supabase import Client, create_client


# ── §2 配置与常量 ─────────────────────────────────────────────────────────────
# secrets（M3 起: OPENROUTER_API_KEY; M4 起: TELEGRAM_BOT_A_TOKEN / TELEGRAM_BOT_B_TOKEN /
#   TELEGRAM_CHAT_ID / SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY;
#   M5-M6: WEBHOOK_URL / WEBHOOK_SECRET_TOKEN）
# 预期来自 .env (与 monitor_v6.py 同目录, 已在 .gitignore)。
# 缺 key 即 KeyError 快速失败 (blueprint 硬约定, 不做静默降级)。
#
# Telegram 通道拆分 (blueprint §2 + llm-botb plan §Bot B 触发与内容):
#   Bot A (人类界面 + webhook 回调) 使用 TELEGRAM_BOT_A_TOKEN;
#   Bot B (LLM 回显, 只读出站) 使用 TELEGRAM_BOT_B_TOKEN;
#   两者共享 TELEGRAM_CHAT_ID (私聊场景下 chat_id == 用户账号 id), Telegram
#   客户端会把两个 bot 的消息分别呈现在两个独立会话窗口。
#   Bot B 不注册 webhook, 不接受任何入站回调 (纯出站通道)。
load_dotenv()
OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
TELEGRAM_BOT_A_TOKEN = os.environ["TELEGRAM_BOT_A_TOKEN"]
TELEGRAM_BOT_B_TOKEN = os.environ["TELEGRAM_BOT_B_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WEBHOOK_URL = os.environ["WEBHOOK_URL"]
WEBHOOK_SECRET_TOKEN = os.environ["WEBHOOK_SECRET_TOKEN"]

# 行为 / 阈值常量（模块级硬编码，不走 env）
SCAN_INTERVAL = 300                    # seconds between scans
SPIKE_THRESHOLD = 0.02                 # 与 SCAN_INTERVAL=300s 绑定
MIN_LIQUIDITY = 2_000                  # Market Gating 流动性下限
MIN_SPIKE_VOLUME = 20_000              # Spike 成交量下限

# 噪声带打标 (命中仅打标 noise_band=true, 不过滤样本)
NOISE_BAND_DAYS_LEFT_MAX = 12
NOISE_BAND_PRICE_LOW = 0.40
NOISE_BAND_PRICE_HIGH = 0.60

# RC (M2 启用)
COOLDOWN_LONG_MINUTES = 60
COOLDOWN_REVERSAL_MINUTES = 30
SIGNALS_PER_HOUR = 2

# 外部模型 (M3 启用)
SONAR_MODEL = "perplexity/sonar"
LLM_MODEL = "anthropic/claude-sonnet-4.6"

# 外部 API
GAMMA_API = "https://gamma-api.polymarket.com"
OPENROUTER_API_URL = "https://openrouter.ai/api/v1"
TELEGRAM_API = "https://api.telegram.org"

# Bot A 发送参数 (bota-human plan §Telegram 交互通道 / §消息文本模板)
BOT_A_TIMEOUT_SEC = 15
CALLBACK_DATA_MAX_BYTES = 64   # Telegram 硬上限 (bota plan §四元按钮)

# Webhook (M5 启用): 监听本地端口, 由反代 / 直监听 + TLS 暴露为 HTTPS
# port 必须是 Telegram 允许的 443/80/88/8443 之一 (bota-human plan §Telegram 交互通道)
# 选 8443: 无需 root 权限监听, 生产环境通常走 nginx/caddy 反代到此端口
WEBHOOK_PORT = 18080
WEBHOOK_HOST = "127.0.0.1"
# 从 WEBHOOK_URL 提取 path 作为 aiohttp 路由, 保证本地路由与注册给 Telegram 的一致
# 例: WEBHOOK_URL="https://xxx.example.com/tg/webhook/abc" -> path="/tg/webhook/abc"
WEBHOOK_PATH = urlparse(WEBHOOK_URL).path or "/webhook"

# Bot B 发送超时
BOT_B_TIMEOUT_SEC = 15
# answerCallbackQuery 要尽快响应, 较短超时; 失败不影响后续 Bot B 流程
ANSWER_CALLBACK_TIMEOUT_SEC = 5

# M6: startup 消息 / setWebhook 注册的 Telegram 调用超时 (一次性启动路径, 较短超时)
STARTUP_TIMEOUT_SEC = 10
SET_WEBHOOK_TIMEOUT_SEC = 10

# M6: 结算任务 (observability plan §结算任务: 每天一次, 凌晨定时)
# 当前实现无绝对时钟锚点, 进程启动起每 24h 跑一次; 生产可接 cron 式 scheduler。
RESOLUTION_INTERVAL_SEC = 86_400
# 每个待结算市场的 Gamma /markets/{id} 查询超时; 失败则该市场本轮跳过
GAMMA_RESOLVE_TIMEOUT_SEC = 15
# 每次结算扫描内 Gamma 调用的软上限, 保护 API 并限制单轮运行时长;
# 超出后本轮剩余市场推迟到下一轮
RESOLUTION_MAX_PER_SCAN = 200
# 单个 Gamma 查询之间的最小间隔 (秒), 轻量限流避免瞬时高频
RESOLUTION_GAMMA_DELAY_SEC = 0.2

# M3 调用参数 (sonar-for-llm / llm-botb plan 冻结)
SONAR_MAX_TOKENS = 1000
SONAR_TEMPERATURE = 0.0
SONAR_TIMEOUT_SEC = 30
LLM_MAX_TOKENS = 800
LLM_TEMPERATURE = 0.1
LLM_TIMEOUT_SEC = 45
CORE_FACT_MAX_CHARS = 200
SONAR_MAX_ARTICLES = 5

# 枚举白名单 (signal_type / judgment 跨 3/5 处强一致，非法值兜底 unknown / None)
VALID_SIGNAL_TYPES = {"hard_news", "opinion", "sentiment", "unknown"}
VALID_JUDGMENTS = {"probability_up", "probability_down", "irrational", "uncertain"}

# 开发期开关（默认 False，手动临时改 True 打印内部追踪）
DEBUG_TRACE = False

# 分类关键词（v5 基线 + v6 新增 finance 类别）
MARKET_KEYWORDS = {
    "crypto": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "sec", "etf",
        "solana", "coinbase", "binance", "stablecoin", "defi", "token",
    ],
    "geopolitics": [
        "war", "ceasefire", "sanction", "nato", "ukraine", "russia",
        "china", "iran", "israel", "taiwan", "military", "invasion",
        "nuclear", "missile", "regime", "coup",
    ],
    "macro": [
        "fed", "federal reserve", "interest rate", "inflation", "gdp",
        "recession", "treasury", "cpi", "unemployment", "tariff",
        "trade", "trump", "election", "president", "congress", "senate",
        "government", "minister", "policy", "vote", "referendum",
    ],
    "finance": [
        "stock", "equity", "nasdaq", "s&p", "sp500", "dow jones",
        "earnings", "ipo", "merger", "acquisition", "bond", "yield",
        "apple", "tesla", "nvidia", "microsoft", "amazon", "meta",
        "google", "alphabet", "berkshire",
    ],
}


# ── §3 客户端单例 ─────────────────────────────────────────────────────────────
# aiohttp: gamma-api / OpenRouter / Telegram 共用 (M1/M3/M4 起)
# supabase-py: REST (PostgREST) 同步客户端, 通过 asyncio.to_thread 异步化 (M4 起)
_aiohttp_session: aiohttp.ClientSession | None = None
_sb_client: Client | None = None


def _logger() -> logging.Logger:
    return logging.getLogger("monitor_v6")


async def get_session() -> aiohttp.ClientSession:
    global _aiohttp_session
    if _aiohttp_session is None or _aiohttp_session.closed:
        _aiohttp_session = aiohttp.ClientSession()
    return _aiohttp_session


async def close_session() -> None:
    global _aiohttp_session
    if _aiohttp_session is not None and not _aiohttp_session.closed:
        await _aiohttp_session.close()
    _aiohttp_session = None


def get_sb() -> Client:
    """
    Supabase 同步客户端单例 (service_role key, 绕过 RLS)。
    供 §4 helpers 用 asyncio.to_thread 包装为异步调用。
    """
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _sb_client


# ── §4 DB helpers (M4 启用) ──────────────────────────────────────────────────
# 写入点与失败语义对齐 observability plan §写入点与失败语义:
#   1. upsert_market / upsert_spike 失败 -> 终止本 spike, 不发 Bot A
#   2. insert_articles 失败 -> best-effort, 仅日志, 不阻断 Bot A
#   3. update_bot_a_sent 失败 -> best-effort, 仅日志, 不影响 RC 记账
#   4. (M5) insert_human_click / fetch_bot_b_context -> 人类点击落库 + Bot B 读
#   5. (M6) fetch_unresolved_markets / update_market_resolution -> 结算回填


def _ts_iso(epoch_sec: float | None = None) -> str:
    """ISO 8601 UTC 字符串, 供 TIMESTAMPTZ 列使用。"""
    if epoch_sec is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(epoch_sec, timezone.utc).isoformat()


def _normalize_utc_iso(raw_ts: object) -> str | None:
    """
    将外部时间字段标准化为 UTC ISO 字符串。

    规则:
      - 空值/无法解析 -> None
      - 无时区信息的字符串按 UTC 解释
      - 未来时间视为异常值 -> None
    """
    if raw_ts is None:
        return None
    text = str(raw_ts).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    if dt_utc > datetime.now(timezone.utc):
        return None
    return dt_utc.isoformat()


def _relative_time_zh(utc_iso: str | None) -> str:
    """将 UTC 时间转为中文相对时间文案。"""
    if not utc_iso:
        return "发布时间未知"
    normalized = _normalize_utc_iso(utc_iso)
    if normalized is None:
        return "发布时间未知"
    dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    delta_sec = int((datetime.now(timezone.utc) - dt).total_seconds())
    if delta_sec < 0:
        return "发布时间未知"
    if delta_sec < 60:
        return "刚刚"
    if delta_sec < 3600:
        return f"{delta_sec // 60}分钟前"
    if delta_sec < 86400:
        return f"{delta_sec // 3600}小时前"
    if delta_sec < 86400 * 30:
        return f"{delta_sec // 86400}天前"
    return dt.strftime("%Y-%m-%d")


def _utc_to_utc8_display(utc_iso: str | None) -> str:
    """将 UTC 时间格式化为东八区展示字符串。"""
    if not utc_iso:
        return "时间未知"
    normalized = _normalize_utc_iso(utc_iso)
    if normalized is None:
        return "时间未知"
    dt_utc = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    dt_utc8 = dt_utc.astimezone(timezone(timedelta(hours=8)))
    return dt_utc8.strftime("%Y-%m-%d %H:%M:%S UTC+8")


async def upsert_market(spike: dict) -> bool:
    """
    按 market_id upsert markets 行 (observability plan §markets 表)。
    刷新字段: question / category / end_date / slug / volume / updated_at。
    resolution_status / resolution_result 由 DDL 默认值 + 结算任务维护, 此处不动。
    返回: True 成功, False 失败 (调用方终止本 spike, 不发 Bot A)。
    """
    row = {
        "market_id": spike["market_id"],
        "question": spike["question"],
        "category": spike["category"],
        "end_date": spike["end_date"] or None,
        "slug": spike["slug"],
        "volume": spike["volume"],
        "updated_at": _ts_iso(),
    }
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("markets")
            .upsert(row, on_conflict="market_id")
            .execute()
        )
        return True
    except Exception as e:
        _logger().warning(
            "[db] upsert_market failed spike=%s market=%s err=%s: %s",
            spike["spike_id"], spike["market_id"], type(e).__name__, e,
        )
        return False


async def upsert_spike(spike: dict, sonar_status: str, llm_out: dict) -> bool:
    """
    按 spike_id upsert spikes 行 (observability plan §spikes 表)。
    仅在 sonar_status ∈ {ok, empty} 且 llm_out 非 None 时调用
    (error / llm_fail 已在上游终止, 不会走到这里)。
    返回: True 成功, False 失败 (调用方终止, 不发 Bot A)。
    """
    row = {
        "spike_id": spike["spike_id"],
        "market_id": spike["market_id"],
        "spike_time_ms": spike["spike_time_ms"],
        "triggered_at": _ts_iso(spike["spike_time_ms"] / 1000.0),
        "price_was": spike["price_was"],
        "price_now": spike["price_now"],
        "delta": spike["delta"],
        "noise_band_tag": "命中" if spike["noise_band"] else None,
        "sonar_status": sonar_status,
        "llm_judgment": llm_out["judgment"],
        "llm_causal_chains": llm_out["causal_chains"],
        "llm_uncertainty": llm_out["uncertainty"],
    }
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("spikes")
            .upsert(row, on_conflict="spike_id")
            .execute()
        )
        return True
    except Exception as e:
        _logger().warning(
            "[db] upsert_spike failed spike=%s err=%s: %s",
            spike["spike_id"], type(e).__name__, e,
        )
        return False


async def insert_articles(spike_id: str, articles: list[dict]) -> None:
    """
    Bulk insert articles 行 (observability plan §articles 表, best-effort)。
    rank 取 articles 下标 (0-based), 对应 Sonar 返回顺序。
    pub_date 统一写 UTC ISO 字符串 (无法解析/缺失/未来值 -> NULL)。
    失败仅日志, 不阻断 Bot A。
    """
    if not articles:
        return
    rows = [
        {
            "spike_id": spike_id,
            "rank": idx,
            "title": a.get("title"),
            "url": a.get("url"),
            "source_name": a.get("source_name"),
            "signal_type": a.get("signal_type"),
            "core_fact": a.get("core_fact"),
            "pub_date": _normalize_utc_iso(a.get("pub_date")),
        }
        for idx, a in enumerate(articles)
    ]
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("articles").insert(rows).execute()
        )
    except Exception as e:
        _logger().warning(
            "[db] insert_articles best-effort failed spike=%s n=%d err=%s: %s",
            spike_id, len(rows), type(e).__name__, e,
        )


async def update_bot_a_sent(spike_id: str) -> None:
    """
    回填 spikes.bot_a_sent_at (observability plan §写入点 2, best-effort)。
    失败仅日志, 不影响 RC 记账 (Bot A 已送达事实不因 DB 失败而回退)。
    """
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("spikes")
            .update({"bot_a_sent_at": _ts_iso()})
            .eq("spike_id", spike_id)
            .execute()
        )
    except Exception as e:
        _logger().warning(
            "[db] update_bot_a_sent best-effort failed spike=%s err=%s: %s",
            spike_id, type(e).__name__, e,
        )


async def insert_human_click(
    spike_id: str,
    judgment: str,
    user_id: int | None,
    msg_id: int | None,
) -> None:
    """
    append-only 记录人类点击 (observability plan §human_clicks 表 / §写入点 3)。
    clicked_at / created_at 交由 DB DEFAULT now() 填; telegram_user_id /
    telegram_message_id 缺省允许 NULL, 纯用于事后分析。

    best-effort: 任何失败 (含 FK 冲突 / 网络 / 超时) 仅日志, 不阻断 Bot B 回显。
    spike 未命中 (LLM 成功才会发 Bot A, 理论不应未命中) 时此插入会触发 FK 冲突,
    属于防御性路径, 仅日志即可。

    judgment 必须在 VALID_JUDGMENTS 中, 由调用方负责校验; 这里不再做白名单兜底
    (错误的 judgment 应当在 webhook 入口就被拒绝)。
    """
    row = {
        "spike_id": spike_id,
        "human_judgment": judgment,
        "telegram_user_id": user_id,
        "telegram_message_id": msg_id,
    }
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("human_clicks").insert(row).execute()
        )
    except Exception as e:
        _logger().warning(
            "[db] insert_human_click best-effort failed spike=%s judgment=%s "
            "user=%s msg=%s err=%s: %s",
            spike_id, judgment, user_id, msg_id, type(e).__name__, e,
        )


async def fetch_bot_b_context(spike_id: str) -> dict | None:
    """
    Bot B 回显所需的完整上下文 (observability plan §写入点 4 / §Bot B 读取).

    三次 REST 查询并合并:
      1. spikes JOIN markets (PostgREST embedded select 走 FK spikes.market_id
         -> markets.market_id): 读 LLM 判断 + spike 上下文 + 市场问题
      2. articles: 按 rank 升序, 全部返回
      3. human_clicks: 按 clicked_at DESC LIMIT 1, 取最新人类判断 (仅用于事后
         诊断; Bot B 当前显示的 human_judgment 来自本次点击 payload, 不来自此字段)

    返回:
      - 命中 -> dict: {
            "spike_id", "triggered_at", "question", "category", "end_date",
            "sonar_status", "llm_judgment", "causal_chains", "uncertainty",
            "articles": [{title, source_name}, ...],
            "latest_human_judgment": str | None,  # DB 当前最新 (诊断用)
        }
      - 未命中 / 查询异常 -> None (调用方仅 insert_human_click, 不发 Bot B)
    """
    def _q_spike():
        return (
            get_sb()
            .table("spikes")
            .select("*, markets(*)")
            .eq("spike_id", spike_id)
            .limit(1)
            .execute()
        )

    def _q_articles():
        return (
            get_sb()
            .table("articles")
            .select("title, source_name, rank")
            .eq("spike_id", spike_id)
            .order("rank")
            .execute()
        )

    def _q_latest_human():
        return (
            get_sb()
            .table("human_clicks")
            .select("human_judgment, clicked_at")
            .eq("spike_id", spike_id)
            .order("clicked_at", desc=True)
            .limit(1)
            .execute()
        )

    try:
        spike_resp = await asyncio.to_thread(_q_spike)
    except Exception as e:
        _logger().warning(
            "[db] fetch_bot_b_context spike query failed spike=%s err=%s: %s",
            spike_id, type(e).__name__, e,
        )
        return None
    spike_rows = spike_resp.data or []
    if not spike_rows:
        _logger().info("[db] fetch_bot_b_context spike miss spike_id=%s", spike_id)
        return None
    spike_row = spike_rows[0]
    market_row = spike_row.get("markets") or {}

    try:
        articles_resp = await asyncio.to_thread(_q_articles)
        articles_rows = articles_resp.data or []
    except Exception as e:
        _logger().warning(
            "[db] fetch_bot_b_context articles query failed spike=%s err=%s: %s",
            spike_id, type(e).__name__, e,
        )
        articles_rows = []

    try:
        human_resp = await asyncio.to_thread(_q_latest_human)
        human_rows = human_resp.data or []
    except Exception as e:
        _logger().warning(
            "[db] fetch_bot_b_context human_clicks query failed spike=%s err=%s: %s",
            spike_id, type(e).__name__, e,
        )
        human_rows = []

    latest_human = human_rows[0]["human_judgment"] if human_rows else None

    return {
        "spike_id": spike_row["spike_id"],
        "triggered_at": spike_row.get("triggered_at"),
        "question": market_row.get("question") or spike_row.get("market_id"),
        "category": market_row.get("category"),
        "end_date": market_row.get("end_date"),
        "sonar_status": spike_row.get("sonar_status"),
        "llm_judgment": spike_row.get("llm_judgment"),
        "causal_chains": spike_row.get("llm_causal_chains") or [],
        "uncertainty": spike_row.get("llm_uncertainty"),
        "articles": [
            {
                "title": a.get("title") or "",
                "source_name": a.get("source_name") or "",
            }
            for a in articles_rows
        ],
        "latest_human_judgment": latest_human,
    }


async def fetch_unresolved_markets() -> list[dict]:
    """
    拉取结算任务的候选市场 (observability plan §结算任务 / §过滤逻辑).

    条件 (PostgREST 过滤):
      resolution_status = 'open' AND end_date < today (UTC)

    返回列表 (空列表 = 无待结算 / 查询失败; 失败仅日志, 不抛):
      [{"market_id": str, "end_date": str | None, "question": str | None}, ...]

    仅选必要列: market_id 用于 Gamma 查询, end_date/question 用于日志可观测性。
    """
    today = datetime.now(timezone.utc).date().isoformat()

    def _q():
        return (
            get_sb()
            .table("markets")
            .select("market_id, end_date, question")
            .eq("resolution_status", "open")
            .not_.is_("end_date", "null")
            .lt("end_date", today)
            .execute()
        )

    try:
        resp = await asyncio.to_thread(_q)
    except Exception as e:
        _logger().warning(
            "[db] fetch_unresolved_markets failed err=%s: %s",
            type(e).__name__, e,
        )
        return []
    return resp.data or []


async def update_market_resolution(
    market_id: str,
    resolution_status: str,
    resolution_result: str | None,
) -> bool:
    """
    回填 markets.resolution_status / resolution_result / updated_at
    (observability plan §结算任务 / §回填字段).

    约束 (调用方负责, 此处不再兜底):
      - resolution_status ∈ {'resolved', 'cancelled'} (已从 'open' 晋升才会调用)
      - resolution_result ∈ {'YES', 'NO', None}

    best-effort: 任何失败 (DB 异常 / CHECK 冲突) 仅日志, 不抛, 不影响其他市场。
    返回: True 成功 / False 失败 (供 resolution_loop 统计日志使用)。
    """
    row = {
        "resolution_status": resolution_status,
        "resolution_result": resolution_result,
        "updated_at": _ts_iso(),
    }
    try:
        await asyncio.to_thread(
            lambda: get_sb().table("markets")
            .update(row)
            .eq("market_id", market_id)
            .execute()
        )
        return True
    except Exception as e:
        _logger().warning(
            "[db] update_market_resolution failed market=%s status=%s result=%s err=%s: %s",
            market_id, resolution_status, resolution_result,
            type(e).__name__, e,
        )
        return False


# ── §5 管道阶段 (纯逻辑，单测友好) ────────────────────────────────────────────

def classify_market(question: str) -> str:
    q = (question or "").lower()
    for category, keywords in MARKET_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return category
    return "other"


def passes_mg(m: dict) -> bool:
    """Market Gating: active/closed + end_date 未过期 + liquidity + 分类命中 (crypto/macro/geopolitics/finance)."""
    if not m.get("_active", False):
        return False
    if m.get("_closed", False):
        return False
    # 过滤已过期市场: days_left < 0 表示 end_date 在今天之前, Gamma 偶发仍 active=true
    # 的过期市场会带来噪音信号 (历史已结算市场不该再被扫描)
    if m.get("days_left", 0) < 0:
        return False
    if m.get("liquidity", 0.0) < MIN_LIQUIDITY:
        return False
    if m.get("category") == "other":
        return False
    return True


async def fetch_markets() -> list[dict]:
    """
    抓取 gamma-api /markets，按 v6 Market Gating 规则解析并过滤。
    单条市场解析失败 → 安全跳过，不影响主循环。
    """
    session = await get_session()
    params = {
        "limit": 500,
        "active": "true",
        "closed": "false",
        "order": "volumeNum",
        "ascending": "false",
        "liquidity_num_min": MIN_LIQUIDITY,
    }
    timeout = aiohttp.ClientTimeout(total=15)
    try:
        async with session.get(f"{GAMMA_API}/markets", params=params, timeout=timeout) as r:
            r.raise_for_status()
            raw = await r.json()
    except Exception as e:
        _logger().warning("[fetch_markets] Failed: %s", e)
        return []

    now = datetime.now(timezone.utc)
    parsed: list[dict] = []
    for m in raw:
        try:
            mid = m.get("id")
            question = m.get("question", "")
            if mid is None or not question:
                continue

            prices = json.loads(m.get("outcomePrices", "[]"))
            if not prices:
                continue
            price = float(prices[0])

            end_str = m.get("endDate", "")
            if not end_str:
                continue
            try:
                end_date = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                continue
            days_left = (end_date - now).days

            liquidity = float(m.get("liquidityNum") or m.get("liquidity") or 0)
            volume = float(m.get("volumeNum") or m.get("volume") or 0)
            active = bool(m.get("active", False))
            closed = bool(m.get("closed", False))
            category = classify_market(question)

            # slug 三级回退: events[0].slug -> m.slug -> str(id)
            events = m.get("events") or []
            event_slug = events[0].get("slug", "") if events else ""
            slug = event_slug or m.get("slug") or str(mid)

            parsed.append({
                "id": str(mid),
                "slug": slug,
                "question": question,
                "price": price,
                "days_left": days_left,
                "liquidity": liquidity,
                "volume": volume,
                "end_date": end_str[:10],
                "category": category,
                "_active": active,
                "_closed": closed,
            })
        except Exception:
            # 单条异常不影响主循环
            continue

    return [m for m in parsed if passes_mg(m)]


def check_spike(market: dict, price_history: dict) -> dict | None:
    """
    Spike 判定 (spike-trigger plan §判定顺序):
      1. market_id 不在 price_history → 写入基线, 不产 spike (warm-up)
      2. 计算 delta, 更新基线
      3. |delta| < SPIKE_THRESHOLD → 结束
      4. volume < MIN_SPIKE_VOLUME → 结束
      5. 命中噪声带 → 打标签 noise_band=true (不剔除)
      6. 产出 spike 字典
    spike_time_ms = int(time.time()*1000); spike_id = f"{market_id}_{spike_time_ms}"
    """
    mid = market["id"]
    price = market["price"]
    now = time.time()

    if mid not in price_history:
        price_history[mid] = {"price": price, "time": now}
        return None

    last = price_history[mid]
    delta = price - last["price"]
    price_history[mid] = {"price": price, "time": now}

    if abs(delta) < SPIKE_THRESHOLD:
        return None
    if market["volume"] < MIN_SPIKE_VOLUME:
        return None

    noise_band = (
        market["days_left"] <= NOISE_BAND_DAYS_LEFT_MAX
        and NOISE_BAND_PRICE_LOW <= price <= NOISE_BAND_PRICE_HIGH
    )

    spike_time_ms = int(time.time() * 1000)
    spike_id = f"{mid}_{spike_time_ms}"

    return {
        "spike_id": spike_id,
        "market_id": mid,
        "slug": market["slug"],
        "question": market["question"],
        "category": market["category"],
        "price_was": last["price"],
        "price_now": price,
        "delta": delta,
        "delta_abs": abs(delta),
        "volume": market["volume"],
        "liquidity": market["liquidity"],
        "end_date": market["end_date"],
        "days_left": market["days_left"],
        "noise_band": noise_band,
        "spike_time_ms": spike_time_ms,
    }


def prune_price_history(price_history: dict, current_ids: set[str]) -> None:
    """每轮结束后删除本轮未出现的 id（保持滚动基线同步）。"""
    absent = [mid for mid in price_history if mid not in current_ids]
    for mid in absent:
        del price_history[mid]


def sort_candidates(spikes: list[dict]) -> list[dict]:
    """
    同轮多 spike 的三层排序 (rate-cooldown plan §同轮多候选时的顺序):
      1. delta_abs 降序
      2. volume 降序
      3. market_id 字典序升序 (纯机械兜底, 打破平局)
    不按 category 分桶, 不引入主观偏好; 结果全序、可复现。
    """
    return sorted(
        spikes,
        key=lambda s: (-s["delta_abs"], -s["volume"], s["market_id"]),
    )


def in_cooldown(
    market_id: str,
    delta: float,
    now_ts: float,
    cooldown_state: dict,
) -> tuple[bool, str]:
    """
    单市场冷却判定 (rate-cooldown plan §单市场冷却):
      - 无历史发送记录 -> 不在冷却
      - 两次 delta 符号相反且均非零 (反转) -> 30 min 内拦截
      - 否则 (同向或零边界) -> 60 min 内拦截

    cooldown_state 形态: {market_id: {"last_sent_at": ts, "last_delta": float}}
    仅记录最近一次发送, 不保留无限期历史。

    返回: (blocked, reason)
      reason ∈ {"none", "cooldown_long", "cooldown_reversal"}
    """
    prev = cooldown_state.get(market_id)
    if not prev:
        return False, "none"

    elapsed = now_ts - prev["last_sent_at"]
    last_delta = prev["last_delta"]
    is_reversal = (last_delta * delta) < 0

    if is_reversal:
        if elapsed < COOLDOWN_REVERSAL_MINUTES * 60:
            return True, "cooldown_reversal"
        return False, "none"

    if elapsed < COOLDOWN_LONG_MINUTES * 60:
        return True, "cooldown_long"
    return False, "none"


def rate_limit_allows(now_ts: float, rate_state: list[float]) -> bool:
    """
    全局每小时上限 (rate-cooldown plan §全局每小时上限):
      滑动 1h 窗口内主推送次数 < SIGNALS_PER_HOUR。
    顺带就地 prune 过期时间戳, 保持 rate_state 有界。
    计数对象: 已 RC pass 的 episode 主推送 (M2 阶段即 RC pass 点;
             M4+ 迁移到 Bot A 发送成功后记账)。
    """
    cutoff = now_ts - 3600
    while rate_state and rate_state[0] < cutoff:
        rate_state.pop(0)
    return len(rate_state) < SIGNALS_PER_HOUR


async def _openrouter_chat(
    model: str,
    messages: list[dict],
    schema: dict,
    schema_name: str,
    max_tokens: int,
    temperature: float,
    timeout_sec: int,
) -> tuple[dict | None, int, str | None]:
    """
    OpenRouter /chat/completions 统一封装 (Sonar & LLM 共用)。
    返回: (parsed_json_object | None, latency_ms, error_msg | None)
      - 成功 -> (dict, latency, None)
      - 失败 -> (None, latency, "<error_type>: <msg>")
    失败类型: timeout / http / json_decode / schema / empty_content。
    """
    session = await get_session()
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema_name,
                "strict": True,
                "schema": schema,
            },
        },
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    t0 = time.monotonic()
    try:
        async with session.post(
            f"{OPENROUTER_API_URL}/chat/completions",
            json=payload,
            headers=headers,
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            body = await r.json()
    except asyncio.TimeoutError:
        return None, int((time.monotonic() - t0) * 1000), "timeout: request exceeded budget"
    except aiohttp.ClientResponseError as e:
        return None, int((time.monotonic() - t0) * 1000), f"http: {e.status} {e.message}"
    except Exception as e:
        return None, int((time.monotonic() - t0) * 1000), f"{type(e).__name__}: {e}"

    latency = int((time.monotonic() - t0) * 1000)
    try:
        content = body["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        return None, latency, f"schema: choices[0].message.content missing ({e})"
    if not content:
        return None, latency, "empty_content: model returned empty string"
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as e:
        return None, latency, f"json_decode: {e}"
    if not isinstance(parsed, dict):
        return None, latency, "schema: top-level is not a JSON object"
    return parsed, latency, None


async def search_news_sonar(spike: dict) -> dict:
    """
    Sonar 供料 (sonar-for-llm plan §API 调用参数 / §Prompt 模板).

    返回统一结构:
      {"status": "ok" | "empty" | "error",
       "articles": list[dict],
       "error_msg": str | None,
       "request_latency_ms": int}

    status 语义 (冻结):
      ok    : 请求成功 + JSON 合法 + 至少 1 条清洗后有效文章
      empty : 请求成功 + JSON 合法 + 清洗后为空数组
      error : 网络 / HTTP / 超时 / JSON 非法 / 结构不满足契约

    清洗规则:
      - signal_type 不在白名单 -> "unknown"
      - core_fact 超 CORE_FACT_MAX_CHARS -> 硬截断 + INFO 日志
        (双保险之一, 不当作 error, 不改变 status)
      - title / url / core_fact 任一为空 -> 丢弃该条
      - published_at 解析后统一为 UTC ISO; 异常/未来值 -> None
      - 最多保留 SONAR_MAX_ARTICLES 条
    """
    scan_minutes = max(SCAN_INTERVAL // 60, 1)
    system_prompt = (
        "You are a news retrieval assistant. Your sole task is to find recent, "
        "relevant news for a prediction market that just experienced a price "
        "spike, and return the results as a strict JSON object. Return only "
        "the JSON object, no prose, no markdown."
    )
    user_prompt = (
        f"A Polymarket prediction market just experienced a price spike within "
        f"the past {scan_minutes} minutes. Find the most recent news that could "
        f"be contemporaneous with or explain this spike.\n\n"
        f"Market question: {spike['question']}\n"
        f"Category: {spike['category']}\n"
        f"Price change: {spike['delta']:+.1%} "
        f"({spike['price_was']:.1%} -> {spike['price_now']:.1%})\n\n"
        f"Return up to {SONAR_MAX_ARTICLES} results, ordered by recency "
        f"(most recent first).\n\n"
        "Output format: a JSON object with key \"articles\" whose value is "
        "an array of objects, each with exactly these fields:\n"
        "- title: string\n"
        "- url: string (actual article URL)\n"
        "- source_name: string (e.g., \"Reuters\", \"Bloomberg\")\n"
        "- signal_type: one of \"hard_news\" | \"opinion\" | \"sentiment\" | \"unknown\"\n"
        "- published_at: ISO 8601 datetime string, or null if unknown\n"
        "- core_fact: one factual sentence describing what happened, "
        "no causal inference, keep it under 200 characters\n\n"
        "signal_type definitions:\n"
        "- hard_news: factual event, official announcement, data release\n"
        "- opinion: analyst commentary, editorial, forecast\n"
        "- sentiment: social media chatter, forum mood, retail sentiment\n"
        "- unknown: cannot classify\n\n"
        "Rules:\n"
        "- Prioritize recency; do not return clearly outdated news.\n"
        "- core_fact is a plain factual statement only — no causal inference, "
        "no probability judgment.\n"
        "- Exclude prediction market aggregators (polymarket, kalshi, manifold) "
        "as primary sources.\n"
        "- If no relevant news is found, return {\"articles\": []}.\n"
        "- Output the JSON object only. No prose, no markdown fences."
    )
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "articles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "title": {"type": "string"},
                        "url": {"type": "string"},
                        "source_name": {"type": "string"},
                        "signal_type": {
                            "type": "string",
                            "enum": list(VALID_SIGNAL_TYPES),
                        },
                        "published_at": {"type": ["string", "null"]},
                        "core_fact": {"type": "string"},
                    },
                    "required": [
                        "title", "url", "source_name", "signal_type",
                        "published_at", "core_fact",
                    ],
                },
            },
        },
        "required": ["articles"],
    }

    parsed, latency, err = await _openrouter_chat(
        model=SONAR_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        schema=schema,
        schema_name="sonar_articles",
        max_tokens=SONAR_MAX_TOKENS,
        temperature=SONAR_TEMPERATURE,
        timeout_sec=SONAR_TIMEOUT_SEC,
    )
    if err is not None:
        _logger().warning(
            "[sonar] error spike=%s latency=%sms msg=%s",
            spike["spike_id"], latency, err,
        )
        return {
            "status": "error",
            "articles": [],
            "error_msg": err,
            "request_latency_ms": latency,
        }

    raw_articles = parsed.get("articles")
    if not isinstance(raw_articles, list):
        _logger().warning(
            "[sonar] error spike=%s latency=%sms msg=articles field is not a list",
            spike["spike_id"], latency,
        )
        return {
            "status": "error",
            "articles": [],
            "error_msg": "schema: articles field is not a list",
            "request_latency_ms": latency,
        }

    cleaned: list[dict] = []
    for a in raw_articles:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        url = str(a.get("url") or "").strip()
        source_name = str(a.get("source_name") or "").strip() or "unknown"
        signal_type = a.get("signal_type") or "unknown"
        if signal_type not in VALID_SIGNAL_TYPES:
            signal_type = "unknown"
        core_fact = str(a.get("core_fact") or "").strip()
        if len(core_fact) > CORE_FACT_MAX_CHARS:
            _logger().info(
                "[sonar] core_fact truncated spike=%s orig_len=%d",
                spike["spike_id"], len(core_fact),
            )
            core_fact = core_fact[:CORE_FACT_MAX_CHARS]
        if not (title and url and core_fact):
            continue
        cleaned.append({
            "title": title,
            "url": url,
            "source_name": source_name,
            "signal_type": signal_type,
            "pub_date": _normalize_utc_iso(a.get("published_at")),
            "core_fact": core_fact,
        })
        if len(cleaned) >= SONAR_MAX_ARTICLES:
            break

    status = "ok" if cleaned else "empty"
    _logger().info(
        "[sonar] %s spike=%s articles=%d latency=%sms",
        status, spike["spike_id"], len(cleaned), latency,
    )
    return {
        "status": status,
        "articles": cleaned,
        "error_msg": None,
        "request_latency_ms": latency,
    }


async def llm_judge(spike: dict, articles_or_none: list[dict] | None) -> dict | None:
    """
    LLM 判断 (llm-botb plan §Prompt 模板 / §输出 JSON).

    路由 (sonar-for-llm plan §下游分流规则):
      articles_or_none is None -> 无新闻版 (Sonar status=empty)
      articles_or_none is list -> 有新闻版 (Sonar status=ok, len>=1)

    返回:
      {"judgment": str, "causal_chains": list[str],
       "uncertainty": str | None, "request_latency_ms": int}
      或 None (失败/超时/枚举非法/约束冲突, 仅日志, 不发 Bot A, 不写库)

    约束校验 (非法 -> 视为失败, 返回 None):
      - judgment ∈ VALID_JUDGMENTS
      - 有新闻版 + judgment ∈ {probability_up, probability_down}
        -> causal_chains 必须非空
      - 无新闻版 -> causal_chains 必须为空数组
    """
    system_prompt = (
        "You are a causal logic analyst for prediction markets.\n\n"
        "Rules (strict):\n"
        "- Reason only from the information explicitly provided by the user. "
        "Do not search or reference external information.\n"
        "- Do not simulate or infer market sentiment from price movement alone.\n"
        "- Output a single JSON object only. No prose, no markdown.\n"
        "- Reasoning content (inside causal_chains / uncertainty strings) "
        "must be in Chinese. JSON keys and enum values remain in English.\n"
        "- Judgments evaluate the support of current information for a price "
        "direction. Do NOT predict whether the market will reverse."
    )

    has_news = articles_or_none is not None and len(articles_or_none) > 0
    price_line = (
        f"Price change: {spike['delta']:+.1%} "
        f"({spike['price_was']:.1%} -> {spike['price_now']:.1%})"
    )

    if has_news:
        news_lines = "\n".join(
            f"[{a['source_name']} / {a['signal_type']}] {a['core_fact']}"
            for a in articles_or_none
        )
        user_prompt = (
            "A Polymarket prediction market just experienced a price spike. "
            "Based on the facts below, make a four-way judgment.\n\n"
            f"Market: {spike['question']}\n"
            f"Category: {spike['category']}\n"
            f"{price_line}\n\n"
            f"News facts:\n{news_lines}\n\n"
            "Judgment definitions (evaluate information support, not future "
            "price direction):\n"
            "- probability_up: credible news and a plausible causal chain "
            "supporting a price increase\n"
            "- probability_down: credible news and a plausible causal chain "
            "supporting a price decrease\n"
            "- irrational: news is unreliable, low-quality, or unrelated to "
            "the market's fundamentals; the current move lacks an informational "
            "basis (this does NOT imply the price will reverse)\n"
            "- uncertain: news is present but causal direction cannot be "
            "reliably determined, or quality is insufficient to conclude\n\n"
            "Task:\n"
            "1. Assess the credibility of each news fact.\n"
            "2. For credible facts, identify causal chains linking them to "
            "the market outcome.\n"
            "3. Synthesize into one of the four judgments above.\n"
            "4. State your primary uncertainty, if any.\n\n"
            "Output format (JSON only):\n"
            "{\n"
            "  \"judgment\": \"probability_up | probability_down | irrational | uncertain\",\n"
            "  \"causal_chains\": [\"[source_name/signal_type] 事实 → 机制 → 市场影响\", ...],\n"
            "  \"uncertainty\": \"一句话说明主要不确定因素，无则为 null\"\n"
            "}\n\n"
            "Constraints:\n"
            "- If judgment is probability_up or probability_down, causal_chains "
            "MUST contain at least one chain.\n"
            "- If judgment is irrational or uncertain, causal_chains MAY be empty []."
        )
    else:
        user_prompt = (
            "A Polymarket prediction market just experienced a price spike, "
            "but no relevant news was retrieved. Make a four-way judgment "
            "based on price behavior and market context only.\n\n"
            f"Market: {spike['question']}\n"
            f"Category: {spike['category']}\n"
            f"{price_line}\n\n"
            "News facts: (none retrieved)\n\n"
            "Judgment definitions (evaluate information support, not future "
            "price direction):\n"
            "- probability_up: rarely applicable without news; only if market "
            "context alone credibly supports upward direction\n"
            "- probability_down: rarely applicable without news; only if "
            "market context alone credibly supports downward direction\n"
            "- irrational: the move lacks any informational basis given no "
            "news was found and context does not explain it (this does NOT "
            "imply the price will reverse)\n"
            "- uncertain: insufficient information to reach a directional or "
            "irrational conclusion (default when in doubt)\n\n"
            "Task:\n"
            "1. Note explicitly that no news was retrieved.\n"
            "2. Consider only the market question, category, and price change.\n"
            "3. Choose a judgment; prefer `uncertain` unless the absence of "
            "news combined with context clearly implies `irrational`.\n"
            "4. State your primary uncertainty.\n\n"
            "Output format (JSON only):\n"
            "{\n"
            "  \"judgment\": \"probability_up | probability_down | irrational | uncertain\",\n"
            "  \"causal_chains\": [],\n"
            "  \"uncertainty\": \"一句话说明主要不确定因素，无则为 null\"\n"
            "}\n\n"
            "Constraints:\n"
            "- causal_chains MUST be an empty array [] in this version.\n"
            "- Prefer uncertain unless evidence for irrational is clear."
        )

    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "judgment": {
                "type": "string",
                "enum": list(VALID_JUDGMENTS),
            },
            "causal_chains": {
                "type": "array",
                "items": {"type": "string"},
            },
            "uncertainty": {"type": ["string", "null"]},
        },
        "required": ["judgment", "causal_chains", "uncertainty"],
    }

    parsed, latency, err = await _openrouter_chat(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        schema=schema,
        schema_name="llm_judgment",
        max_tokens=LLM_MAX_TOKENS,
        temperature=LLM_TEMPERATURE,
        timeout_sec=LLM_TIMEOUT_SEC,
    )
    if err is not None:
        _logger().warning(
            "[llm] error spike=%s latency=%sms msg=%s",
            spike["spike_id"], latency, err,
        )
        return None

    judgment = parsed.get("judgment")
    causal_chains = parsed.get("causal_chains")
    uncertainty = parsed.get("uncertainty")

    if judgment not in VALID_JUDGMENTS:
        _logger().warning(
            "[llm] invalid judgment spike=%s value=%r", spike["spike_id"], judgment,
        )
        return None
    if not isinstance(causal_chains, list) or any(
        not isinstance(x, str) for x in causal_chains
    ):
        _logger().warning(
            "[llm] invalid causal_chains spike=%s type=%s",
            spike["spike_id"], type(causal_chains).__name__,
        )
        return None
    if uncertainty is not None and not isinstance(uncertainty, str):
        _logger().warning(
            "[llm] invalid uncertainty spike=%s type=%s",
            spike["spike_id"], type(uncertainty).__name__,
        )
        return None

    if has_news:
        if judgment in {"probability_up", "probability_down"} and not causal_chains:
            _logger().warning(
                "[llm] constraint violated spike=%s judgment=%s but causal_chains empty",
                spike["spike_id"], judgment,
            )
            return None
    else:
        if causal_chains:
            _logger().warning(
                "[llm] constraint violated spike=%s no-news but causal_chains=%d",
                spike["spike_id"], len(causal_chains),
            )
            return None

    _logger().info(
        "[llm] ok spike=%s judgment=%s chains=%d latency=%sms",
        spike["spike_id"], judgment, len(causal_chains), latency,
    )
    return {
        "judgment": judgment,
        "causal_chains": causal_chains,
        "uncertainty": uncertainty,
        "request_latency_ms": latency,
    }


# ── §6 Telegram 发送与 HTML 渲染 (M4+) ───────────────────────────────────────
# M4 启用: build_bot_a_html / build_bot_a_keyboard / send_bot_a
# M5-M6 再补: build_bot_b_html / send_bot_b / send_startup / set_webhook


# Bot A 四按钮缩写 <-> 完整 judgment 的正反映射 (callback_data ≤64B 压缩)。
# 正向给 build_bot_a_keyboard 生成 callback_data; 反向给 M5 webhook 解析。
JUDGMENT_ABBR = {
    "irrational": "irr",
    "uncertain": "unc",
    "probability_up": "up",
    "probability_down": "down",
}
JUDGMENT_FROM_ABBR = {v: k for k, v in JUDGMENT_ABBR.items()}

# Bot B 判断文案映射 (llm-botb plan §Bot B 消息文本模板 / §排版约定).
# llm_judgment / human_judgment 显示时走此表; 未知值兜底原字符串以便观察。
JUDGMENT_ZH = {
    "probability_up": "概率上升",
    "probability_down": "概率下降",
    "irrational": "非理性",
    "uncertain": "不确定",
}


def build_bot_a_html(spike: dict, articles: list[dict]) -> str:
    """
    构造 Bot A HTML 消息文本 (bota plan §Bot A 消息文本模板)。
      - 有新闻版 (sonar status=ok, articles 非空): 列出新闻条目
      - 无新闻版 (sonar status=empty, articles 为空): 显示 "暂无相关新闻"
    噪声带行仅在命中时显示, 否则整行省略。
    所有动态文本走 html.escape; URL 通过 escape(quote=True) 作为属性安全。

    parse_mode = HTML。最终字符串由调用方传 Telegram sendMessage。
    """
    q = html.escape(spike["question"])
    cat = html.escape(spike["category"])
    end_date = html.escape(spike["end_date"] or "")
    delta_line = (
        f"📈 {spike['delta']:+.1%}　"
        f"<code>{spike['price_was']:.0%} → {spike['price_now']:.0%}</code>"
    )

    lines = [
        f"📌 <b>{q}</b>",
        "",
        f"🏷 {cat} · 结算日 {end_date}",
        delta_line,
    ]
    if spike["noise_band"]:
        lines.append("⚠️ 噪声带：命中")

    lines.append("")

    if articles:
        lines.append("📰 <b>相关新闻</b>")
        for a in articles:
            url = html.escape(a["url"], quote=True)
            src = html.escape(a["source_name"])
            sig = html.escape(a["signal_type"])
            core = html.escape(a["core_fact"])
            rel = html.escape(_relative_time_zh(a.get("pub_date")))
            lines.append(f'• <a href="{url}">[{src} / {sig}]</a>')
            lines.append(f"  {core}")
            lines.append(f"  🕒 {rel}")
    else:
        lines.append("🔇 暂无相关新闻")

    lines.append("")
    lines.append("❓ 你认为这次波动的原因是？")
    return "\n".join(lines)


def build_bot_a_keyboard(spike_id: str) -> dict:
    """
    一行四按钮 inline_keyboard (bota plan §四元按钮):
      非理性 | 不确定 | 概率上升 | 概率下降
    callback_data 格式: j=<abbr>|s=<spike_id>; abbr ∈ {irr, unc, up, down}
    所有 callback_data 字节长度必须 ≤ CALLBACK_DATA_MAX_BYTES (64), 否则抛错快速失败。
    """
    buttons = [
        ("非理性", JUDGMENT_ABBR["irrational"]),
        ("不确定", JUDGMENT_ABBR["uncertain"]),
        ("概率上升", JUDGMENT_ABBR["probability_up"]),
        ("概率下降", JUDGMENT_ABBR["probability_down"]),
    ]
    row = []
    for text, abbr in buttons:
        data = f"j={abbr}|s={spike_id}"
        encoded = data.encode("utf-8")
        if len(encoded) > CALLBACK_DATA_MAX_BYTES:
            raise ValueError(
                f"callback_data exceeds {CALLBACK_DATA_MAX_BYTES} bytes: "
                f"len={len(encoded)} data={data!r}"
            )
        row.append({"text": text, "callback_data": data})
    return {"inline_keyboard": [row]}


async def send_bot_a(spike: dict, articles: list[dict]) -> int | None:
    """
    Telegram sendMessage 发 Bot A (bota plan §Telegram 交互通道 / §消息文本模板).

    - parse_mode=HTML, disable_web_page_preview=True (避免多链接预览堆叠)
    - reply_markup=inline_keyboard (一行四按钮)
    - 返回: message_id (int) 成功 / None 失败; 失败仅 WARN 日志, 调用方终止链路

    不在本函数做 RC 记账与 bot_a_sent_at 回填, 由 scan_loop 在成功分支统一做。
    """
    text = build_bot_a_html(spike, articles)
    reply_markup = build_bot_a_keyboard(spike["spike_id"])
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": reply_markup,
    }
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=BOT_A_TIMEOUT_SEC)
    try:
        async with session.post(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_A_TOKEN}/sendMessage",
            json=payload,
            timeout=timeout,
        ) as r:
            body = await r.json()
    except asyncio.TimeoutError:
        _logger().warning(
            "[bot_a] timeout spike=%s after %ss",
            spike["spike_id"], BOT_A_TIMEOUT_SEC,
        )
        return None
    except Exception as e:
        _logger().warning(
            "[bot_a] transport error spike=%s err=%s: %s",
            spike["spike_id"], type(e).__name__, e,
        )
        return None

    if not body.get("ok"):
        _logger().warning(
            "[bot_a] telegram returned error spike=%s body=%s",
            spike["spike_id"], body,
        )
        return None
    try:
        message_id = int(body["result"]["message_id"])
    except (KeyError, TypeError, ValueError) as e:
        _logger().warning(
            "[bot_a] malformed response spike=%s err=%s body=%s",
            spike["spike_id"], e, body,
        )
        return None
    _logger().info(
        "[bot_a] sent spike=%s message_id=%s articles=%d",
        spike["spike_id"], message_id, len(articles),
    )
    return message_id


def build_bot_b_html(ctx: dict, human_judgment: str) -> str:
    """
    构造 Bot B HTML (llm-botb plan §Bot B 消息文本模板 / §排版约定).

    固定展示区:
      - 标题 + spike_id + triggered_at (东八区展示)
      - LLM 判断 (中文映射) / 人类判断 (中文映射, 取本次点击 payload)
      - 市场问题原文

    条件区 (符合排版约定):
      - 📰 当时新闻: 仅 sonar_status=ok 且 articles 非空时显示 (每条: 标题 + source)
      - 💡 因果链:    仅 causal_chains 非空时显示
      - ⚠️ 不确定因素: 仅 uncertainty 非 null/空时显示

    只读: 不附 inline_keyboard (llm-botb plan §交互约束: Bot B 是终点)。
    """
    spike_id = html.escape(ctx["spike_id"])
    triggered_at = html.escape(_utc_to_utc8_display(ctx.get("triggered_at")))
    question = html.escape(ctx.get("question") or "")

    llm_key = ctx.get("llm_judgment") or ""
    llm_zh = JUDGMENT_ZH.get(llm_key, llm_key or "(unknown)")
    hm_zh = JUDGMENT_ZH.get(human_judgment, human_judgment or "(unknown)")

    lines = [
        f"🔍 <b>判断对比</b>　<code>{spike_id}</code>　· {triggered_at}",
        "",
        f"🤖 LLM：<b>{html.escape(llm_zh)}</b>",
        f"👤 你：<b>{html.escape(hm_zh)}</b>",
        "",
        f"📌 {question}",
    ]

    articles = ctx.get("articles") or []
    sonar_status = ctx.get("sonar_status")
    if sonar_status == "ok" and articles:
        lines.append("")
        lines.append("📰 <b>当时新闻</b>")
        for a in articles:
            src = html.escape(a.get("source_name") or "unknown")
            title = html.escape(a.get("title") or "")
            lines.append(f"• [{src}] {title}")

    chains = ctx.get("causal_chains") or []
    if chains:
        lines.append("")
        lines.append("💡 <b>因果链</b>")
        for ch in chains:
            lines.append(f"• {html.escape(str(ch))}")

    unc = ctx.get("uncertainty")
    if unc:
        lines.append("")
        lines.append("⚠️ <b>不确定因素</b>")
        lines.append(html.escape(str(unc)))

    return "\n".join(lines)


async def send_bot_b(ctx: dict, human_judgment: str) -> bool:
    """
    Telegram sendMessage 发 Bot B (llm-botb plan §Bot B 触发与内容 / §Telegram 通道).

    - 使用独立的 TELEGRAM_BOT_B_TOKEN, 与 Bot A 的 token 完全分离;
      chat_id 固定为共享的 TELEGRAM_CHAT_ID (不再复用 callback_query 里的 chat.id),
      Telegram 客户端会把 Bot B 的消息自动归入与 Bot B 的独立会话窗口。
    - parse_mode=HTML, disable_web_page_preview=True
    - **不附任何 inline_keyboard** (llm-botb plan §交互约束: 只读终点)
    - 返回 True/False 供日志统计; 发送失败仅 WARN, 不阻断 human_clicks 写入

    human_judgment 必须是 VALID_JUDGMENTS 之一, 来自本次点击 payload;
    使得 "多次改判" 场景下每次 Bot B 都如实显示本次点击 (blueprint §验收点)。
    """
    text = build_bot_b_html(ctx, human_judgment)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=BOT_B_TIMEOUT_SEC)
    try:
        async with session.post(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_B_TOKEN}/sendMessage",
            json=payload,
            timeout=timeout,
        ) as r:
            body = await r.json()
    except asyncio.TimeoutError:
        _logger().warning(
            "[bot_b] timeout spike=%s after %ss",
            ctx.get("spike_id"), BOT_B_TIMEOUT_SEC,
        )
        return False
    except Exception as e:
        _logger().warning(
            "[bot_b] transport error spike=%s err=%s: %s",
            ctx.get("spike_id"), type(e).__name__, e,
        )
        return False

    if not body.get("ok"):
        _logger().warning(
            "[bot_b] telegram returned error spike=%s body=%s",
            ctx.get("spike_id"), body,
        )
        return False

    _logger().info(
        "[bot_b] sent spike=%s human=%s llm=%s",
        ctx.get("spike_id"), human_judgment, ctx.get("llm_judgment"),
    )
    return True


async def answer_callback_query(callback_query_id: str, text: str | None = None) -> None:
    """
    立即 ACK Telegram 按钮点击, 消除客户端 loading 态 (bota-human plan §Handler 第 2 步).

    best-effort: 失败仅 WARN, 不阻断后续 Bot B / human_clicks 流程。
    text 可选, 默认空 -> 仅消除 loading; 若提供则弹一个短 toast (目前只在 callback_data
    非法分支用到, 告知用户数据格式错误)。
    """
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=ANSWER_CALLBACK_TIMEOUT_SEC)
    try:
        async with session.post(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_A_TOKEN}/answerCallbackQuery",
            json=payload,
            timeout=timeout,
        ) as r:
            body = await r.json()
            if not body.get("ok"):
                _logger().warning(
                    "[answer_callback] telegram error cq=%s body=%s",
                    callback_query_id, body,
                )
    except asyncio.TimeoutError:
        _logger().warning(
            "[answer_callback] timeout cq=%s after %ss",
            callback_query_id, ANSWER_CALLBACK_TIMEOUT_SEC,
        )
    except Exception as e:
        _logger().warning(
            "[answer_callback] transport error cq=%s err=%s: %s",
            callback_query_id, type(e).__name__, e,
        )


# M6: 启动路径 (send_startup / set_webhook) ───────────────────────────────────
# - send_startup: 冻结模板来自 rules-only plan §Startup 消息; parse_mode=HTML;
#   通过 Bot A / Bot B 两个 token 发同一份文案到同一 TELEGRAM_CHAT_ID, 让两个
#   会话窗口同时亮; secrets 不入正文, 只显示 URL 本身。失败仅 WARN, 不阻断。
# - set_webhook: 仅 Bot A 调 POST /bot<token>/setWebhook (url + secret_token);
#   Bot B 不注册 webhook (llm-botb plan §Telegram 通道: 纯出站).
#   失败仅 WARN + stdout, 不阻断 (下次启动会再注册, 或人工 curl 兜底)。


def build_startup_html() -> str:
    """
    构造启动摘要 HTML (rules-only plan §Startup 消息 冻结模板)。

    展示内容 (严格对齐模板, 顺序不改):
      扫描间隔 / Spike 阈值 / 最低流动性 / Sonar 模型 / LLM 模型 /
      Supabase URL / Webhook URL / started_at (UTC ISO)

    约定:
      - parse_mode=HTML, 所有动态文本 html.escape
      - secrets (token / key) 不出现在正文; SUPABASE_URL / WEBHOOK_URL 按模板原样展示
        (WEBHOOK_URL 的路径 secret 与二次校验 header 解耦, 暴露路径不会导致伪造通过)
    """
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "🚀 <b>Monitor v6 已启动</b>",
        "",
        f"扫描间隔：{SCAN_INTERVAL}s",
        f"Spike 阈值：{SPIKE_THRESHOLD:+.0%}",
        f"最低流动性：{MIN_LIQUIDITY:,}",
        f"Sonar 模型：{html.escape(SONAR_MODEL)}",
        f"LLM 模型：{html.escape(LLM_MODEL)}",
        f"Supabase：{html.escape(SUPABASE_URL)}",
        f"Webhook：{html.escape(WEBHOOK_URL)}",
        "",
        html.escape(started_at),
    ]
    return "\n".join(lines)


async def _send_startup_via(token: str, label: str, text: str) -> bool:
    """
    向指定 Bot (token) 发一条启动摘要; label 仅供日志区分 A / B。
    best-effort: 任何失败 (超时 / HTTP / Telegram ok=false) 仅 WARN, 不抛。
    """
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=STARTUP_TIMEOUT_SEC)
    try:
        async with session.post(
            f"{TELEGRAM_API}/bot{token}/sendMessage",
            json=payload,
            timeout=timeout,
        ) as r:
            body = await r.json()
    except asyncio.TimeoutError:
        _logger().warning(
            "[startup] %s timeout after %ss", label, STARTUP_TIMEOUT_SEC,
        )
        print(f"  [startup] {label} ⚠️ timeout after {STARTUP_TIMEOUT_SEC}s")
        return False
    except Exception as e:
        _logger().warning(
            "[startup] %s transport error err=%s: %s", label, type(e).__name__, e,
        )
        print(f"  [startup] {label} ⚠️ transport error {type(e).__name__}: {e}")
        return False

    if not body.get("ok"):
        _logger().warning("[startup] %s telegram returned error body=%s", label, body)
        print(f"  [startup] {label} ⚠️ telegram error: {body}")
        return False
    _logger().info("[startup] %s sent", label)
    print(f"  [startup] {label} ✅ sent")
    return True


async def send_startup() -> None:
    """
    启动摘要消息 (blueprint §6 + rules-only plan §Startup).

    - 仅在 amain() 入口调用一次, 不循环; 构造文案走 build_startup_html。
    - 通过 Bot A / Bot B 两个 token 向共享 TELEGRAM_CHAT_ID 各发一条,
      让两个独立会话窗口都能收到"已启动"信号 (blueprint §6 约定).
    - best-effort: 任一/全部失败均不抛, 不阻断后续 set_webhook / gather 启动。
    """
    text = build_startup_html()
    await _send_startup_via(TELEGRAM_BOT_A_TOKEN, "Bot A", text)
    await _send_startup_via(TELEGRAM_BOT_B_TOKEN, "Bot B", text)


async def set_webhook() -> None:
    """
    向 Telegram 注册 Bot A 的 webhook (bota-human plan §Telegram 交互通道).

    - 仅 Bot A 调 setWebhook; Bot B 无 webhook (llm-botb plan 约束).
    - url=WEBHOOK_URL (完整 HTTPS, 含 secret path); secret_token=WEBHOOK_SECRET_TOKEN
      Telegram 后续每次 POST 会带 header X-Telegram-Bot-Api-Secret-Token,
      本文件入口 handle_webhook 做一致性校验。
    - best-effort: 失败仅 WARN + stdout; 可通过 curl 手动重注册兜底。
    """
    payload = {
        "url": WEBHOOK_URL,
        "secret_token": WEBHOOK_SECRET_TOKEN,
    }
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=SET_WEBHOOK_TIMEOUT_SEC)
    try:
        async with session.post(
            f"{TELEGRAM_API}/bot{TELEGRAM_BOT_A_TOKEN}/setWebhook",
            json=payload,
            timeout=timeout,
        ) as r:
            body = await r.json()
    except asyncio.TimeoutError:
        _logger().warning("[set_webhook] timeout after %ss", SET_WEBHOOK_TIMEOUT_SEC)
        print(f"  [set_webhook] ⚠️ timeout after {SET_WEBHOOK_TIMEOUT_SEC}s")
        return
    except Exception as e:
        _logger().warning(
            "[set_webhook] transport error err=%s: %s", type(e).__name__, e,
        )
        print(f"  [set_webhook] ⚠️ transport error {type(e).__name__}: {e}")
        return

    if not body.get("ok"):
        _logger().warning("[set_webhook] telegram returned error body=%s", body)
        print(f"  [set_webhook] ⚠️ telegram error: {body}")
        return
    _logger().info("[set_webhook] registered url=%s", WEBHOOK_URL)
    print(f"  [set_webhook] ✅ registered url={WEBHOOK_URL}")


# ── §7 aiohttp Webhook (M5) ──────────────────────────────────────────────────
# 形态: 与 scan_loop 共享同一 asyncio 事件循环 (blueprint §9 asyncio.gather).
# 部署: 监听 WEBHOOK_HOST:WEBHOOK_PORT 明文 HTTP, 由反代 (nginx/caddy) 终结 TLS,
#       WEBHOOK_URL (注册给 Telegram) 必须是 HTTPS 且 path 与 WEBHOOK_PATH 一致。
# 安全: 双重 secret_token 校验 —— setWebhook 注册 secret_token (M6),
#       Telegram 每次 POST 带 header X-Telegram-Bot-Api-Secret-Token, 本文件入口
#       比对 WEBHOOK_SECRET_TOKEN, 不一致 -> 401。


def _parse_callback_data(data: str) -> tuple[str, str] | None:
    """
    解析 Bot A 按钮的 callback_data: "j=<abbr>|s=<spike_id>"。
    合法 -> (judgment, spike_id); 非法 -> None (调用方走 400 分支)。

    校验:
      - 恰好两个 '|' 分段 (容错: 允许 spike_id 内部出现其他字符, 但必须以 j=/s= 开头)
      - abbr ∈ JUDGMENT_FROM_ABBR; judgment 反推到 VALID_JUDGMENTS
      - spike_id 非空
    不校验 spike_id 的详细结构 ({market_id}_{spike_time_ms}); 下游以库命中为准。
    """
    if not data or "|" not in data:
        return None
    parts = data.split("|", 1)
    if len(parts) != 2:
        return None
    left, right = parts
    if not left.startswith("j=") or not right.startswith("s="):
        return None
    abbr = left[2:]
    spike_id = right[2:]
    judgment = JUDGMENT_FROM_ABBR.get(abbr)
    if judgment is None or judgment not in VALID_JUDGMENTS:
        return None
    if not spike_id:
        return None
    return judgment, spike_id


async def _process_click(
    spike_id: str,
    judgment: str,
    user_id: int | None,
    msg_id: int | None,
) -> None:
    """
    webhook 后台串行任务 (asyncio.create_task 调度, 不阻塞 200 OK 响应):
      fetch_bot_b_context -> (命中) send_bot_b -> insert_human_click

    顺序按 bota-human plan §Handler 步骤 3~4:
      - Bot B 发送失败不阻断 human_clicks 写入
      - spike 未命中时跳过 Bot B, 仍尝试 insert (防御性; FK 冲突仅日志)
    Bot B chat_id 来自模块级 TELEGRAM_CHAT_ID (llm-botb plan §Telegram 通道),
    与 callback_query 里的 chat 无关。
    整个 task 吞异常, 不影响后续 webhook 请求 / scan_loop。
    """
    try:
        ctx = await fetch_bot_b_context(spike_id)
        if ctx is None:
            _logger().warning(
                "[webhook] spike miss spike_id=%s human=%s (skip Bot B, still insert)",
                spike_id, judgment,
            )
        else:
            await send_bot_b(ctx, judgment)
        await insert_human_click(spike_id, judgment, user_id, msg_id)
    except Exception as e:
        _logger().exception(
            "[webhook] _process_click unexpected spike=%s err=%s: %s",
            spike_id, type(e).__name__, e,
        )


async def handle_webhook(request: web.Request) -> web.Response:
    """
    aiohttp webhook 入口 (bota-human plan §Handler 固定顺序).

    流程:
      1. 校验 header X-Telegram-Bot-Api-Secret-Token == WEBHOOK_SECRET_TOKEN
         不一致 -> 401 + WARN
      2. 解析 JSON body; 非 callback_query 更新 (e.g. message) -> 200 noop
      3. 解析 callback_data; 非法 -> answerCallbackQuery("数据格式错误") + 200
      4. answerCallbackQuery (立即消除 loading)
      5. asyncio.create_task(_process_click(...)) 后台跑 Bot B + human_clicks
      6. 200 OK (让 Telegram 不重试; 后台 task 失败仅日志)

    该 handler 总是返回 200/401/400, 不抛异常, 不阻塞主循环。
    """
    provided = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if provided != WEBHOOK_SECRET_TOKEN:
        _logger().warning(
            "[webhook] secret token mismatch remote=%s path=%s",
            request.remote, request.path,
        )
        return web.Response(status=401, text="unauthorized")

    try:
        update = await request.json()
    except Exception as e:
        _logger().warning("[webhook] invalid json from %s err=%s", request.remote, e)
        return web.Response(status=400, text="bad request")

    cq = update.get("callback_query")
    if not isinstance(cq, dict):
        # 不关心 message / edited_message / 其他更新, 直接 200
        return web.Response(text="ok")

    cq_id = cq.get("id")
    data = cq.get("data") or ""
    user_id = (cq.get("from") or {}).get("id")
    msg = cq.get("message") or {}
    msg_id = msg.get("message_id")

    parsed = _parse_callback_data(data)
    if parsed is None:
        _logger().warning(
            "[webhook] invalid callback_data=%r user=%s msg=%s",
            data, user_id, msg_id,
        )
        if cq_id:
            await answer_callback_query(cq_id, text="数据格式错误")
        return web.Response(text="ok")

    judgment, spike_id = parsed
    _logger().info(
        "[webhook] click spike=%s judgment=%s user=%s msg=%s",
        spike_id, judgment, user_id, msg_id,
    )

    if cq_id:
        ack_text = f"已记录: {JUDGMENT_ZH.get(judgment, judgment)}"
        await answer_callback_query(cq_id, text=ack_text)

    asyncio.create_task(
        _process_click(spike_id, judgment, user_id, msg_id)
    )
    return web.Response(text="ok")


def build_app() -> web.Application:
    """aiohttp 应用: 单路由挂 handle_webhook 到 WEBHOOK_PATH。"""
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    return app


async def serve_webhook() -> None:
    """
    启动 aiohttp AppRunner + TCPSite, 监听本地端口, 与 scan_loop 共享事件循环
    (blueprint §9 asyncio.gather(scan_loop(), serve_webhook())).

    不主动 setWebhook (M6 再做); 启动后永远 await, 直到被取消时 cleanup runner。
    """
    app = build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_HOST, WEBHOOK_PORT)
    await site.start()
    _logger().info(
        "[webhook] listening on %s:%d path=%s (public URL: %s)",
        WEBHOOK_HOST, WEBHOOK_PORT, WEBHOOK_PATH, WEBHOOK_URL,
    )
    print(
        f"  [webhook] listening on {WEBHOOK_HOST}:{WEBHOOK_PORT} "
        f"path={WEBHOOK_PATH} public={WEBHOOK_URL}"
    )
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


# ── §8 周期任务 (协程) ───────────────────────────────────────────────────────

async def scan_loop() -> None:
    """
    主扫描循环 (blueprint §8 + §5 管道全景):
      - 首轮: fetch_markets -> warm-up price_history -> prune -> sleep
      - 次轮起: fetch_markets -> check_spike -> sort_candidates
               -> 逐个 spike 串行跑 RC gate -> Sonar -> LLM -> DB -> Bot A

    状态（闭包）:
      - price_history:  {market_id: {"price", "time"}}
      - cooldown_state: {market_id: {"last_sent_at", "last_delta"}}
      - rate_state:     [ts, ...] 滑动 1h 窗口内已成功 Bot A 送达的时间戳

    RC 记账点 (M4 起): 从 RC pass 迁移到 Bot A 发送成功后。
      - 中途任意终止 (cooldown / rate / sonar_error / llm_fail / upsert_fail /
        bot_a_fail) 都不占冷却槽, 不占每小时配额。
      - update_bot_a_sent / insert_articles 失败仅日志, 不影响 RC 记账。
      - 记账时间戳用 sent_ts = time.time() (真实送达时刻), 非扫描开始 now_ts,
        避免串行耗时 (30-120s) 使冷却窗口提前失效; RC gate 判定继续用 now_ts
        保持本轮快照一致 (同轮后发的 spike 看到的是同一时钟, 不会受前一个
        已送达 spike 的影响)。
    """
    price_history: dict[str, dict] = {}
    cooldown_state: dict[str, dict] = {}
    rate_state: list[float] = []
    scan = 0
    while True:
        scan += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] Scan #{scan}")

        try:
            markets = await fetch_markets()
        except Exception as e:
            print(f"  [scan_loop] fetch_markets crashed: {e}")
            markets = []

        current_ids = {m["id"] for m in markets}
        print(f"  Markets passed MG: {len(markets)}")
        if DEBUG_TRACE:
            for _m in markets:
                print(
                    f"     DEBUG MG pass id={_m['id']} cat={_m['category']} "
                    f"price={_m['price']:.3f} liq={_m['liquidity']:.0f} "
                    f"vol={_m['volume']:.0f} days_left={_m['days_left']}"
                )

        if scan == 1:
            # 首轮只写基线
            for m in markets:
                price_history[m["id"]] = {"price": m["price"], "time": time.time()}
            print("  Baseline established. Detection starts next scan.")
        else:
            spikes: list[dict] = []
            for m in markets:
                sp = check_spike(m, price_history)
                if sp:
                    spikes.append(sp)

            spikes = sort_candidates(spikes)
            print(f"  Spike candidates: {len(spikes)}")

            now_ts = time.time()
            rc_pass = 0
            rc_blocked = 0
            sent = 0
            terminated = 0
            for sp in spikes:
                tag = "noise" if sp["noise_band"] else "clean"
                header = (
                    f"  ⚡ [{sp['category']:>11}] {sp['question'][:58]} | "
                    f"{sp['price_was']:.1%} -> {sp['price_now']:.1%} "
                    f"({sp['delta']:+.2%}) | vol={sp['volume']:,.0f} "
                    f"days_left={sp['days_left']} band={tag} "
                    f"spike_id={sp['spike_id']}"
                )

                # 判定顺序: 冷却 -> rate limit -> pass
                blocked, reason = in_cooldown(
                    sp["market_id"], sp["delta"], now_ts, cooldown_state
                )
                if blocked:
                    print(header)
                    print(f"     RC ⏸ blocked [{reason}]")
                    rc_blocked += 1
                    if DEBUG_TRACE:
                        print(f"     DEBUG spike_dict={sp}")
                    continue

                if not rate_limit_allows(now_ts, rate_state):
                    print(header)
                    print(
                        f"     RC ⏸ blocked [rate_limit] "
                        f"used={len(rate_state)}/{SIGNALS_PER_HOUR} per 1h"
                    )
                    rc_blocked += 1
                    if DEBUG_TRACE:
                        print(f"     DEBUG spike_dict={sp}")
                    continue

                # RC gate passed; 记账推迟到 Bot A 成功后 (blueprint §8).
                print(header)
                print(f"     RC ✅ pass (slot {len(rate_state)+1}/{SIGNALS_PER_HOUR} pending)")
                rc_pass += 1
                if DEBUG_TRACE:
                    print(f"     DEBUG spike_dict={sp}")

                # ── M3: Sonar ───────────────────────────────────────────────
                try:
                    sonar_out = await search_news_sonar(sp)
                except Exception as e:
                    print(f"     Sonar ❌ crashed: {type(e).__name__}: {e}")
                    terminated += 1
                    continue

                s_status = sonar_out["status"]
                s_latency = sonar_out["request_latency_ms"]
                s_count = len(sonar_out["articles"])
                print(
                    f"     Sonar [{s_status}] articles={s_count} "
                    f"latency={s_latency}ms"
                )
                if DEBUG_TRACE:
                    print(
                        "     DEBUG sonar="
                        + json.dumps(sonar_out, ensure_ascii=False)
                    )
                else:
                    for a in sonar_out["articles"][:3]:
                        print(
                            f"       - [{a['source_name']}/{a['signal_type']}] "
                            f"{a['core_fact'][:120]}"
                        )
                    if sonar_out["error_msg"]:
                        print(f"       err: {sonar_out['error_msg']}")

                if s_status == "error":
                    print("     ⏭ terminated (sonar error, no Bot A, no RC)")
                    terminated += 1
                    continue

                # ── M3: LLM ─────────────────────────────────────────────────
                articles_arg = sonar_out["articles"] if s_status == "ok" else None
                try:
                    llm_out = await llm_judge(sp, articles_arg)
                except Exception as e:
                    print(f"     LLM ❌ crashed: {type(e).__name__}: {e}")
                    terminated += 1
                    continue

                if llm_out is None:
                    print("     ⏭ terminated (llm failure, no Bot A, no RC)")
                    terminated += 1
                    continue

                unc = llm_out["uncertainty"]
                print(
                    f"     LLM  judgment={llm_out['judgment']} "
                    f"chains={len(llm_out['causal_chains'])} "
                    f"latency={llm_out['request_latency_ms']}ms"
                )
                for ch in llm_out["causal_chains"][:3]:
                    print(f"       • {ch[:140]}")
                if unc:
                    print(f"       uncertainty: {unc[:140]}")
                if DEBUG_TRACE:
                    print(
                        "     DEBUG llm="
                        + json.dumps(llm_out, ensure_ascii=False)
                    )

                # ── M4: DB 持久化 (硬门槛) ─────────────────────────────────
                try:
                    ok_m = await upsert_market(sp)
                except Exception as e:
                    print(f"     DB ❌ upsert_market crashed: {type(e).__name__}: {e}")
                    terminated += 1
                    continue
                if not ok_m:
                    print("     ⏭ terminated (upsert_market failed, no Bot A, no RC)")
                    terminated += 1
                    continue

                try:
                    ok_s = await upsert_spike(sp, s_status, llm_out)
                except Exception as e:
                    print(f"     DB ❌ upsert_spike crashed: {type(e).__name__}: {e}")
                    terminated += 1
                    continue
                if not ok_s:
                    print("     ⏭ terminated (upsert_spike failed, no Bot A, no RC)")
                    terminated += 1
                    continue

                if DEBUG_TRACE:
                    print(
                        f"     DEBUG DB upsert ok spike={sp['spike_id']} "
                        f"market={sp['market_id']} sonar_status={s_status} "
                        f"judgment={llm_out['judgment']}"
                    )

                if s_status == "ok":
                    # best-effort; 失败仅日志, 不阻断
                    await insert_articles(sp["spike_id"], sonar_out["articles"])

                # ── M4: Bot A 发送 ──────────────────────────────────────────
                bot_a_articles = sonar_out["articles"] if s_status == "ok" else []
                if DEBUG_TRACE:
                    _bot_a_text = build_bot_a_html(sp, bot_a_articles)
                    _bot_a_kb = build_bot_a_keyboard(sp["spike_id"])
                    print(
                        f"     DEBUG Bot A payload spike={sp['spike_id']} "
                        f"articles={len(bot_a_articles)} "
                        f"text_len={len(_bot_a_text)} "
                        f"buttons={len(_bot_a_kb['inline_keyboard'][0])}"
                    )
                try:
                    message_id = await send_bot_a(sp, bot_a_articles)
                except Exception as e:
                    print(f"     Bot A ❌ crashed: {type(e).__name__}: {e}")
                    terminated += 1
                    continue

                if message_id is None:
                    print("     ⏭ terminated (bot_a send failed, no RC)")
                    terminated += 1
                    continue

                # Bot A 已送达 -> RC 记账 (blueprint §8 契约落地)
                # 使用 sent_ts (真实送达时刻) 而非 now_ts (扫描开始时刻),
                # 防止 Sonar+LLM+DB+Bot A 串行耗时 (30-120s) 使冷却窗口提前失效;
                # RC gate 判定仍用 now_ts 保持"本轮快照一致" (见 scan_loop 顶部).
                sent_ts = time.time()
                cooldown_state[sp["market_id"]] = {
                    "last_sent_at": sent_ts,
                    "last_delta": sp["delta"],
                }
                rate_state.append(sent_ts)
                sent += 1
                print(
                    f"     Bot A ✅ sent message_id={message_id} "
                    f"(rate {len(rate_state)}/{SIGNALS_PER_HOUR} per 1h)"
                )

                # best-effort 回填; 失败仅日志, 不影响 RC 记账
                await update_bot_a_sent(sp["spike_id"])

            if spikes:
                print(
                    f"  RC summary: pass={rc_pass} blocked={rc_blocked} | "
                    f"sent={sent} terminated={terminated}"
                )

        # 只有在实际拿到数据时才 prune，避免 fetch 失败把基线全清空
        if markets:
            prune_price_history(price_history, current_ids)

        await asyncio.sleep(SCAN_INTERVAL)


# ── M6: 结算任务 ─────────────────────────────────────────────────────────────
# 规则全量冻结于 observability plan §结算任务:
#   - 职责: 扫 markets WHERE resolution_status='open' AND end_date<today, 回填
#   - Endpoint: GET {GAMMA_API}/markets/{market_id} (v5 已在用, 不引新接口)
#   - 触发频率: 每 24h 一次 (RESOLUTION_INTERVAL_SEC), 非实时事件高频无意义
#   - 未结算处理: 若 Gamma 仍未显示结算结果, 保持 'open', 下次再查
#   - 写入语义: best-effort, 失败仅日志, 不阻断主链路
# 进程形态: 与 scan_loop / serve_webhook 并列, 由 amain 的 asyncio.gather 统一调度。


async def fetch_gamma_market(market_id: str) -> dict | None:
    """
    单市场 Gamma 查询 (observability plan §结算任务 / §API endpoint).

    返回:
      - dict: Gamma 原始 JSON (调用方自行派生 closed / outcomePrices / ...)
      - None: 网络/HTTP/JSON 任一失败 (仅日志; 上层跳过该市场本轮)
    """
    session = await get_session()
    timeout = aiohttp.ClientTimeout(total=GAMMA_RESOLVE_TIMEOUT_SEC)
    try:
        async with session.get(
            f"{GAMMA_API}/markets/{market_id}",
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            return await r.json()
    except asyncio.TimeoutError:
        _logger().warning(
            "[resolution] gamma timeout market=%s after %ss",
            market_id, GAMMA_RESOLVE_TIMEOUT_SEC,
        )
        return None
    except Exception as e:
        _logger().warning(
            "[resolution] gamma error market=%s err=%s: %s",
            market_id, type(e).__name__, e,
        )
        return None


def derive_resolution(gamma: dict) -> tuple[str, str | None] | None:
    """
    从 Gamma 原始响应派生 (resolution_status, resolution_result).

    判定逻辑 (保守, 未知 -> 保持 open):
      - gamma.closed != True: 市场未关盘 -> None (保持 open)
      - outcomePrices 可解析且 outcomePrices[0] == 1.0 -> ('resolved', 'YES')
      - outcomePrices 可解析且 outcomePrices[0] == 0.0 -> ('resolved', 'NO')
      - 其他 (价格模糊 / 解析失败) -> None (保持 open, 下次再查)

    observability plan §未结算处理: 到期但 Gamma 未显示结果 -> 保持 open。
    'cancelled' 当前无可靠信号 (Polymarket 暂未在 Gamma 公开标志), 保持 open。
    """
    if not bool(gamma.get("closed")):
        return None
    raw_prices = gamma.get("outcomePrices")
    try:
        prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        if not isinstance(prices, list) or not prices:
            return None
        p0 = float(prices[0])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

    if p0 == 1.0:
        return "resolved", "YES"
    if p0 == 0.0:
        return "resolved", "NO"
    return None


async def _resolve_once() -> dict:
    """
    执行一轮结算扫描 (被 resolution_loop 定期调用).

    返回统计字典 (供 stdout/日志汇总):
      {"candidates", "resolved_yes", "resolved_no", "kept_open",
       "gamma_fail", "db_fail"}

    - candidates 超过 RESOLUTION_MAX_PER_SCAN 时, 仅处理前 N 条, 剩余下轮
    - 每次 Gamma 调用之间 sleep RESOLUTION_GAMMA_DELAY_SEC, 轻量限流
    - 任何单市场异常被吞, 不影响后续市场
    """
    stats = {
        "candidates": 0,
        "resolved_yes": 0,
        "resolved_no": 0,
        "kept_open": 0,
        "gamma_fail": 0,
        "db_fail": 0,
    }
    rows = await fetch_unresolved_markets()
    stats["candidates"] = len(rows)
    if not rows:
        return stats

    targets = rows[:RESOLUTION_MAX_PER_SCAN]
    for idx, row in enumerate(targets):
        mid = row.get("market_id")
        if not mid:
            continue
        try:
            gamma = await fetch_gamma_market(mid)
            if gamma is None:
                stats["gamma_fail"] += 1
                continue

            decision = derive_resolution(gamma)
            if decision is None:
                stats["kept_open"] += 1
                continue

            status, result = decision
            ok = await update_market_resolution(mid, status, result)
            if not ok:
                stats["db_fail"] += 1
                continue
            if result == "YES":
                stats["resolved_yes"] += 1
            else:
                stats["resolved_no"] += 1
            _logger().info(
                "[resolution] market=%s -> status=%s result=%s", mid, status, result,
            )
        except Exception as e:
            _logger().exception(
                "[resolution] unexpected market=%s err=%s: %s",
                mid, type(e).__name__, e,
            )
        if idx + 1 < len(targets):
            await asyncio.sleep(RESOLUTION_GAMMA_DELAY_SEC)
    return stats


async def resolution_loop() -> None:
    """
    结算回填协程 (blueprint §8 + observability plan §结算任务).

    每 RESOLUTION_INTERVAL_SEC (24h) 跑一次 _resolve_once, 打印统计摘要。
    任何 _resolve_once 级异常仅日志, 不中断循环 (不影响 scan_loop / webhook)。
    """
    while True:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        print(f"\n[{ts}] Resolution scan")
        try:
            stats = await _resolve_once()
            print(
                f"  Resolution: candidates={stats['candidates']} "
                f"YES={stats['resolved_yes']} NO={stats['resolved_no']} "
                f"kept_open={stats['kept_open']} "
                f"gamma_fail={stats['gamma_fail']} db_fail={stats['db_fail']}"
            )
            _logger().info(
                "[resolution] scan done candidates=%d yes=%d no=%d kept_open=%d "
                "gamma_fail=%d db_fail=%d",
                stats["candidates"], stats["resolved_yes"], stats["resolved_no"],
                stats["kept_open"], stats["gamma_fail"], stats["db_fail"],
            )
        except Exception as e:
            _logger().exception(
                "[resolution] loop iteration crashed err=%s: %s",
                type(e).__name__, e,
            )
            print(f"  Resolution ❌ crashed: {type(e).__name__}: {e}")
        await asyncio.sleep(RESOLUTION_INTERVAL_SEC)


# ── §9 main ──────────────────────────────────────────────────────────────────

async def amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    print("=" * 60)
    print("Polymarket Monitor v6 — M6 milestone (full: scan + RC + Sonar/LLM + DB + Bot A + Webhook/Bot B + Resolution)")
    print(
        f"SPIKE_THRESHOLD={SPIKE_THRESHOLD:.0%} | "
        f"SCAN_INTERVAL={SCAN_INTERVAL}s | "
        f"MIN_LIQUIDITY={MIN_LIQUIDITY:,} | "
        f"MIN_SPIKE_VOLUME={MIN_SPIKE_VOLUME:,}"
    )
    print(
        f"Noise band: days_left<={NOISE_BAND_DAYS_LEFT_MAX} AND "
        f"{NOISE_BAND_PRICE_LOW:.2f}<=price<={NOISE_BAND_PRICE_HIGH:.2f}"
    )
    print(
        f"RC: cooldown_long={COOLDOWN_LONG_MINUTES}min | "
        f"cooldown_reversal={COOLDOWN_REVERSAL_MINUTES}min | "
        f"signals_per_hour={SIGNALS_PER_HOUR}"
    )
    print(
        f"Sonar: {SONAR_MODEL} (max_tokens={SONAR_MAX_TOKENS} "
        f"temp={SONAR_TEMPERATURE} timeout={SONAR_TIMEOUT_SEC}s)"
    )
    print(
        f"LLM:   {LLM_MODEL} (max_tokens={LLM_MAX_TOKENS} "
        f"temp={LLM_TEMPERATURE} timeout={LLM_TIMEOUT_SEC}s)"
    )
    print(
        f"Supabase: {SUPABASE_URL} (tables: markets / spikes / articles / human_clicks)"
    )
    print(
        f"Bot A: chat_id={TELEGRAM_CHAT_ID} parse_mode=HTML "
        f"timeout={BOT_A_TIMEOUT_SEC}s callback_data≤{CALLBACK_DATA_MAX_BYTES}B "
        f"(token=A)"
    )
    print(
        f"Bot B: chat_id={TELEGRAM_CHAT_ID} parse_mode=HTML "
        f"timeout={BOT_B_TIMEOUT_SEC}s (token=B, separate bot, 独立会话窗口)"
    )
    print(
        f"Webhook: listen={WEBHOOK_HOST}:{WEBHOOK_PORT} path={WEBHOOK_PATH} "
        f"public={WEBHOOK_URL} secret=<hidden>"
    )
    print(
        f"Resolution: every {RESOLUTION_INTERVAL_SEC}s "
        f"(max {RESOLUTION_MAX_PER_SCAN} markets/scan, "
        f"gamma timeout={GAMMA_RESOLVE_TIMEOUT_SEC}s)"
    )
    print("=" * 60)

    try:
        # M6: 启动序列 (blueprint §9 终态) -- setWebhook / startup 消息均 best-effort,
        # 失败不阻断三协程启动 (下次重启会再注册, 人工 curl 兜底亦可).
        print("\n[startup] sending startup messages to Bot A / Bot B ...")
        await send_startup()
        print("[startup] registering Telegram webhook for Bot A ...")
        await set_webhook()
        print("[startup] launching scan_loop / serve_webhook / resolution_loop ...")

        await asyncio.gather(
            scan_loop(),
            serve_webhook(),
            resolution_loop(),
        )
    finally:
        await close_session()


if __name__ == "__main__":
    asyncio.run(amain())
