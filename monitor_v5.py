"""
Polymarket Monitor v5
Changes from v4:
- Merged two LLM calls into one (raw articles → facts + chains in single pass)
- Immediate "unconfirmed" alert + 1 follow-up (replaced 3-retry queue)
- "No news" = unconfirmed signal, not conclusion
- Removed regex HL ticker extraction → static HL entry link
- Removed unused NEWS_WINDOW_HOURS
- Renamed send_telegram_alert → send_alert / send_followup for clarity
"""

import requests, time, json, re, os
import xml.etree.ElementTree as ET
import email.utils
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# ── Configuration ─────────────────────────────────
TELEGRAM_TOKEN   = "8625060874:AAHArLnIo1m6PAtSIsNPNp918okNAi-LoAg"
TELEGRAM_CHAT_ID = "8145872643"
TAVILY_API_KEY   = "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"
OPENROUTER_KEY   = "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"

# Phase 0: observability trace (debug only; no behavior changes)
DEBUG_TRACE = os.environ.get("DEBUG_TRACE", "false").lower() in ("1", "true", "yes", "y")
DEBUG_TRACE_PROMPT_MAX_CHARS = int(os.environ.get("DEBUG_TRACE_PROMPT_MAX_CHARS", "800"))
DEBUG_TRACE_TELEGRAM = os.environ.get("DEBUG_TRACE_TELEGRAM", "false").lower() in ("1", "true", "yes", "y")
# scope: "confirmed_only" (news["found"]==True) or "all_spike"
DEBUG_TRACE_TELEGRAM_SCOPE = os.environ.get("DEBUG_TRACE_TELEGRAM_SCOPE", "confirmed_only")
# chunking
DEBUG_TRACE_TELEGRAM_CHUNK_SIZE = int(os.environ.get("DEBUG_TRACE_TELEGRAM_CHUNK_SIZE", "3800"))
DEBUG_TRACE_TELEGRAM_MAX_CHUNKS = int(os.environ.get("DEBUG_TRACE_TELEGRAM_MAX_CHUNKS", "8"))

# ── Parameters ────────────────────────────────────
SCAN_INTERVAL    = 300          # seconds between scans
SPIKE_THRESHOLD  = 0.02         # minimum price move to trigger
MIN_LIQUIDITY    = 2_000        # minimum market liquidity
MIN_SPIKE_VOLUME = 20_000       # post-trigger volume filter for alerts
# Dynamic cooldown (plan A): same-direction long vs reversal short
COOLDOWN_LONG_MINUTES = 60
COOLDOWN_REVERSAL_MINUTES = 30
TRACKA_SIGNALS_PER_HOUR = int(os.environ.get("TRACKA_SIGNALS_PER_HOUR", "2"))
SEND_TRACKA_STATS_TO_TELEGRAM = True  # 4h Track A stats push
MIN_DAYS_LEFT    = 7            # minimum days to expiry
PRICE_MIN        = 0.10         # expanded from 0.20
PRICE_MAX        = 0.90         # expanded from 0.80

GAMMA_API = "https://gamma-api.polymarket.com"

# News time window by category (hours)
NEWS_WINDOW_HOURS = {
    "crypto":      1.0,
    "macro":       1.5,
    "geopolitics": 2.5,
}

HYPERLIQUID_ASSETS = [
    "BTC", "ETH", "SOL", "BNB", "AVAX", "DOGE", "LINK", "ARB", "OP",
    "MATIC", "APT", "SUI", "INJ", "TIA", "WIF", "PEPE", "NEAR", "ATOM",
    "DOT", "ADA", "XRP", "LTC", "BCH", "FIL", "ICP", "AAVE", "UNI",
    "MKR", "CRV", "JUP", "SEI", "BLUR", "GMX", "DYDX", "RUNE", "PYTH",
    "XAU", "XAG",
]

MARKET_KEYWORDS = {
    "crypto": [
        "bitcoin", "btc", "ethereum", "eth", "crypto", "sec", "etf",
        "solana", "coinbase", "binance", "stablecoin", "defi", "token"
    ],
    "geopolitics": [
        "war", "ceasefire", "sanction", "nato", "ukraine", "russia",
        "china", "iran", "israel", "taiwan", "military", "invasion",
        "nuclear", "missile", "regime", "coup"
    ],
    "macro": [
        "fed", "federal reserve", "interest rate", "inflation", "gdp",
        "recession", "treasury", "cpi", "unemployment", "tariff",
        "trade", "trump", "election", "president", "congress", "senate",
        "government", "minister", "policy", "vote", "referendum"
    ],
}

AUTHORITATIVE_SOURCES = [
    "reuters", "bloomberg", "ap ", "associated press", "bbc",
    "financial times", "wall street journal", "wsj", "ft.com",
    "coindesk", "cointelegraph", "axios", "politico"
]

# ── Track B: RSS monitoring ────────────────────────
RSS_ENABLED = os.environ.get("RSS_ENABLED", "false").lower() in ("1", "true", "yes", "y")
RSS_INTERVAL = 180   # seconds between RSS checks (3 min)

RSS_FEEDS = {
    "BBC":      "https://feeds.bbci.co.uk/news/rss.xml",
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
}

RSS_ENTITY_KEYWORDS = {
    "geopolitics": [
        "iran", "russia", "ukraine", "china", "taiwan", "israel", "hamas",
        "ceasefire", "sanction", "nato", "missile", "military", "war",
        "coup", "nuclear", "troops", "invasion", "strike", "middle east",
        "north korea", "kim", "putin", "zelensky", "netanyahu",
    ],
    "macro": [
        "federal reserve", "fed rate", "powell", "inflation", "cpi",
        "interest rate", "rate cut", "rate hike", "treasury", "gdp",
        "recession", "tariff", "trade war", "trade deal", "trump tariff",
        "jobs report", "unemployment", "fomc", "debt ceiling",
    ],
    "crypto": [
        "bitcoin", "btc", "ethereum", "crypto", "sec crypto",
        "bitcoin etf", "coinbase", "binance", "stablecoin", "tether",
        "crypto regulation", "digital asset", "blockchain",
    ],
}

price_history        = {}
pending_news_checks  = {}
seen_rss_articles    = set()   # URLs already alerted, avoids duplicates
tracka_last_signal   = {}        # market_id -> {"t": ts, "sgn": -1|0|1}
tracka_signal_send_times = []    # send_alert/send_combined timestamps (1h window)

tracka_stats = {
    "window_start": time.time(),
    "market_scans": 0,
    "markets_tracked_total": 0,
    "spike_candidates": 0,
    "filtered_min_volume": 0,
    "filtered_noise_band": 0,
    "spikes_passed_filters": 0,
    "filtered_cooldown": 0,
    "rate_limited_skips": 0,
    "alerts_sent_markets": 0,
    "alerts_sent_messages": 0,
    "combined_alert_events": 0,
    "chain_confirmed_with": 0,
    "chain_confirmed_without": 0,
}


def format_pub_date(pub: str) -> str:
    """News time: parsed -> raw string -> N/A."""
    dt = parse_pub_datetime(pub)
    if dt:
        return dt.strftime("%m-%d %H:%M")
    if pub and str(pub).strip():
        return str(pub).strip()[:48]
    return "N/A"


def _format_news_lines(articles: list, max_items: int = 2) -> str:
    """Up to max_items headlines with full article URL and time."""
    lines = []
    for a in articles[:max_items]:
        title = (a.get("title") or "").strip()
        if len(title) > 120:
            title = title[:117] + "..."
        url = (a.get("url") or "").strip()
        auth = a.get("authority") == "High"
        lines.append(
            f"{'🟢' if auth else '⚪'} {title}\n"
            f"   {url} · {format_pub_date(a.get('pub_date'))}"
        )
    return "\n".join(lines)

def parse_pub_datetime(pub: str) -> datetime | None:
    """
    Parse Tavily/RSS published date into timezone-aware datetime (UTC).
    Returns None if the date string format is unknown/unparseable.
    """
    if not pub:
        return None
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(pub)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url[:30]


FALLBACK_NO_CHAIN = "未匹配到可在 HL 落地且新闻中有依据的标的。"

# phrase (lower) -> HL ticker
ASSET_ALIAS_TO_TICKER = [
    ("bitcoin", "BTC"), ("btc", "BTC"), ("ethereum", "ETH"), ("ether", "ETH"),
    ("solana", "SOL"), ("gold", "XAU"), ("silver", "XAG"), ("xauusd", "XAU"),
]


def extract_allowed_endpoints(articles: list) -> set[str]:
    """Tickers in HYPERLIQUID_ASSETS that appear in title/content (word-boundary + aliases)."""
    allowed: set[str] = set()
    hl = set(HYPERLIQUID_ASSETS)
    for a in articles:
        blob = f"{a.get('title', '')} {a.get('content', '')}"
        blob_for_tickers = blob
        lower = blob.lower()
        for phrase, tick in ASSET_ALIAS_TO_TICKER:
            if phrase in lower and tick in hl:
                allowed.add(tick)
        for tick in HYPERLIQUID_ASSETS:
            pat = rf"(?<![A-Za-z0-9]){re.escape(tick)}(?![A-Za-z0-9])"
            if re.search(pat, blob_for_tickers, flags=re.IGNORECASE):
                allowed.add(tick.upper() if tick.isalpha() else tick)
    return {t for t in allowed if t in hl}


def _parse_chain_blocks(llm_text: str) -> list[dict]:
    """Split LLM output into chain blocks; extract star count and ticker from 资产： line."""
    if not llm_text or not llm_text.strip():
        return []
    lines = llm_text.strip().splitlines()
    blocks: list[list[str]] = []
    cur: list[str] = []
    for line in lines:
        if re.match(r"^Chain \d+:", line.strip()):
            if cur:
                blocks.append(cur)
            cur = [line]
        else:
            if cur is not None:
                cur.append(line)
    if cur:
        blocks.append(cur)
    out = []
    for blk in blocks:
        text = "\n".join(blk).strip()
        if not text:
            continue
        first = blk[0].strip()
        stars = first.count("★")
        ticker = None
        for line in blk:
            m = re.match(r"^[\s]*资产[：:]\s*([A-Za-z0-9]+)", line.strip())
            if m:
                ticker = m.group(1).upper()
                break
        out.append({"raw": text, "stars": stars, "ticker": ticker})
    return out


def _strip_star_markers_for_display(text: str) -> str:
    """Remove star rating from Chain headers; drop standalone 假设 lines' star refs optional."""
    out_lines = []
    for line in text.splitlines():
        line = re.sub(r"^(Chain \d+):\s*★+", r"\1:", line)
        out_lines.append(line)
    return "\n".join(out_lines).strip()


def post_validate_chain_analysis(raw_llm: str, allowed: set[str]) -> tuple[str, bool]:
    """
    Filter chains: endpoint in allowed, stars >= 4 (★★★★☆).
    Returns (display_text, had_at_least_one_chain).
    """
    if not raw_llm or not raw_llm.strip():
        return FALLBACK_NO_CHAIN, False
    blocks = _parse_chain_blocks(raw_llm)
    kept = []
    for b in blocks:
        if not b.get("ticker") or b["ticker"] not in allowed:
            continue
        if b["stars"] < 4:
            continue
        kept.append(b["raw"])
    if not kept:
        return FALLBACK_NO_CHAIN, False
    merged = "\n\n".join(kept[:2])
    merged = _strip_star_markers_for_display(merged)
    return merged, True

def _dbg_truncate(text: str, max_chars: int) -> str:
    if text is None:
        return ""
    s = str(text)
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + f"...[truncated {len(s) - max_chars} chars]"

_debug_trace_collector = None  # list[str] | None

def _dbg_line(msg: str):
    """
    Debug line printer:
    - always prints to stdout when DEBUG_TRACE is enabled
    - when collector is active, also appends to the current trace buffer
    """
    if DEBUG_TRACE:
        print(msg)
    global _debug_trace_collector
    if _debug_trace_collector is not None:
        try:
            _debug_trace_collector.append(str(msg))
        except Exception:
            pass

def _send_message_plain(text: str):
    """
    Telegram sender without Markdown parsing to avoid formatting issues.
    """
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        print(f"\n{'='*55}\n{text}\n{'='*55}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=10
        )
    except Exception as e:
        print(f"[Telegram][plain] Failed: {e}")

def _send_debug_trace_to_telegram(trace_text: str, header: str):
    """
    Send debug trace via Telegram in chunks (because LLM output can be very long).
    """
    if not DEBUG_TRACE_TELEGRAM:
        return
    if not trace_text.strip():
        return

    # Keep header small to avoid consuming the Telegram payload.
    full = f"{header}\n\n{trace_text}"
    chunks = [full[i:i + DEBUG_TRACE_TELEGRAM_CHUNK_SIZE] for i in range(0, len(full), DEBUG_TRACE_TELEGRAM_CHUNK_SIZE)]
    chunks = chunks[:DEBUG_TRACE_TELEGRAM_MAX_CHUNKS]

    for idx, chunk in enumerate(chunks, start=1):
        suffix = f"\n\n[chunk {idx}/{len(chunks)}]"
        _send_message_plain(chunk + suffix)


# ── Market scanning ────────────────────────────────

def classify_market(question: str) -> str:
    q = question.lower()
    for category, keywords in MARKET_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return category
    return "other"


def fetch_markets() -> list:
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={
            "limit":            500,
            "active":           "true",
            "closed":           "false",
            "order":            "volumeNum",
            "ascending":        "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        }, timeout=15)
        r.raise_for_status()

        now     = datetime.now(timezone.utc)
        markets = []

        for m in r.json():
            try:
                prices = json.loads(m.get("outcomePrices", "[]"))
                if not prices:
                    continue
                price = float(prices[0])
                if price < PRICE_MIN or price > PRICE_MAX:
                    continue

                end_str = m.get("endDate", "")
                if not end_str:
                    continue
                end_date  = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                days_left = (end_date - now).days
                if days_left < MIN_DAYS_LEFT:
                    continue

                question = m.get("question", "")
                category = classify_market(question)
                if category == "other":
                    continue

                token_ids = json.loads(m.get("clobTokenIds", "[]"))
                if not token_ids:
                    continue

                # Event-level slug gives correct Polymarket URL
                events     = m.get("events", [])
                event_slug = events[0].get("slug", "") if events else ""
                slug       = event_slug or m.get("slug", str(m["id"]))

                markets.append({
                    "id":        m["id"],
                    "slug":      slug,
                    "question":  question,
                    "price":     price,
                    "days_left": days_left,
                    "volume":    float(m.get("volume") or 0),
                    "end_date":  end_str[:10],
                    "category":  category,
                })
            except Exception:
                continue

        return markets

    except Exception as e:
        print(f"[Radar] Failed: {e}")
        return []


def check_spike(market: dict) -> dict | None:
    mid   = market["id"]
    now   = time.time()
    price = market["price"]

    if mid not in price_history:
        price_history[mid] = {"time": now, "price": price}
        return None

    last  = price_history[mid]
    delta = price - last["price"]
    price_history[mid] = {"time": now, "price": price}

    if abs(delta) >= SPIKE_THRESHOLD:
        tracka_stats["spike_candidates"] += 1
        if market["volume"] < MIN_SPIKE_VOLUME:
            tracka_stats["filtered_min_volume"] += 1
            return None
        # 3-sigma + 二八定律：近期到期且概率模糊区间，噪声占主导，跳过
        if market["days_left"] <= 12 and 0.40 <= price <= 0.60:
            tracka_stats["filtered_noise_band"] += 1
            return None
        tracka_stats["spikes_passed_filters"] += 1
        if DEBUG_TRACE:
            try:
                print("\n" + "=" * 70)
                print("[DEBUG_TRACE][TrackA] Spike candidate accepted by filters")
                print(f"market_id={market['id']} category={market['category']}")
                print(f"question={market['question']}")
                print(f"price_was={last['price']} price_now={price} delta={delta} (abs={abs(delta)})")
                print(f"days_left={market['days_left']} volume={market['volume']}")
                print("=" * 70)
            except Exception:
                # Debug logs must never break the main loop.
                pass
        return {
            "question":   market["question"],
            "price_now":  price,
            "price_was":  last["price"],
            "delta":      delta,
            "volume":     market["volume"],
            "end_date":   market["end_date"],
            "days_left":  market["days_left"],
            "market_id":  market["id"],
            "slug":       market["slug"],
            "category":   market["category"],
            "spike_time": now,
        }
    return None


def _delta_sign(d: float) -> int:
    if d > 0:
        return 1
    if d < 0:
        return -1
    return 0


def in_tracka_signal_cooldown(market_id: str | int, spike_delta: float, now_ts: float | None = None) -> bool:
    """Plan A: 60m same-direction repeat; 30m after sign reversal vs last signal."""
    now_ts = now_ts or time.time()
    prev = tracka_last_signal.get(market_id)
    if not prev:
        return False
    elapsed = now_ts - prev["t"]
    s_new = _delta_sign(spike_delta)
    s_old = prev["sgn"]
    reversal = (s_new != s_old) and (s_new != 0) and (s_old != 0)
    limit_sec = COOLDOWN_REVERSAL_MINUTES * 60 if reversal else COOLDOWN_LONG_MINUTES * 60
    return elapsed < limit_sec


def mark_tracka_signal_sent(market_id: str | int, spike_delta: float, now_ts: float | None = None):
    tracka_last_signal[market_id] = {
        "t": now_ts or time.time(),
        "sgn": _delta_sign(spike_delta),
    }


def tracka_signal_rate_limit_allows(now_ts: float | None = None) -> bool:
    global tracka_signal_send_times
    now_ts = now_ts or time.time()
    tracka_signal_send_times = [t for t in tracka_signal_send_times if now_ts - t < 3600]
    return len(tracka_signal_send_times) < TRACKA_SIGNALS_PER_HOUR


def record_tracka_signal_send(now_ts: float | None = None):
    tracka_signal_send_times.append(now_ts or time.time())


# ── News search ────────────────────────────────────

def search_news(spike: dict) -> dict:
    if not TAVILY_API_KEY or TAVILY_API_KEY == "YOUR_TAVILY_KEY":
        return {"found": False, "articles": []}

    # Extract keywords: remove prediction-market phrasing and noisy time tokens.
    stop_words = {"will", "would", "could", "should", "by", "in", "the", "a",
                  "an", "be", "to", "of", "or", "and", "before", "after",
                  "reach", "hit", "drop", "rise", "win", "lose", "end", "during",
                  "through", "until", "ever", "become", "get", "remain", "stay"}
    months = {
        "january", "february", "march", "april", "may", "june", "july",
        "august", "september", "sept", "october", "november", "december",
        "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    }
    now_dt = datetime.now(timezone.utc)
    tokens = re.sub(r"[?']", "", spike["question"]).split()
    kw_list = []
    seen_kw = set()
    for w in tokens:
        wl = w.lower()
        if wl in stop_words:
            continue
        if w.isdigit():
            continue
        # Normalize tokens like "U.S." -> "US" by stripping non-alphanumerics.
        w_norm = re.sub(r"[^a-zA-Z0-9]", "", w)
        if not w_norm:
            continue

        wl_norm = w_norm.lower()
        if wl_norm in months:
            continue
        if wl_norm in stop_words:
            continue
        if wl_norm.isdigit():
            continue
        # Keep tokens that contain letters (avoid pure symbols).
        if not re.search(r"[a-zA-Z]", w_norm):
            continue
        if len(w_norm) < 2:
            continue
        if wl_norm in seen_kw:
            continue
        seen_kw.add(wl_norm)
        kw_list.append(w_norm)

    keywords = " ".join(kw_list) if kw_list else spike["category"]
    query = f"{keywords} -polymarket -\"prediction market\""

    # Time window based on category
    window_h = NEWS_WINDOW_HOURS.get(spike["category"], 2.0)
    cutoff   = now_dt - timedelta(hours=window_h)

    try:
        if DEBUG_TRACE:
            _dbg_line("\n" + "=" * 70)
            _dbg_line("[DEBUG_TRACE][News] search_news()")
            _dbg_line(f"trace_key={spike.get('market_id', '')}")
            _dbg_line(f"category={spike['category']} window_h={window_h}")
            _dbg_line(f"query={query}")
            _dbg_line(f"cutoff_utc={cutoff.isoformat()}")
            _dbg_line("=" * 70)

        r = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key":      TAVILY_API_KEY,
                "query":        query,
                "search_depth": "basic",
                "max_results":  7,
                "days":         1,
            },
            timeout=15
        )
        r.raise_for_status()
        articles = []

        for a in r.json().get("results", []):
            title   = a.get("title", "")
            url     = a.get("url", "")
            content = a.get("content", "")[:400]
            pub     = a.get("published_date", "")

            # Client-side time filter:
            # - if we can parse the datetime, apply cutoff
            # - if unparseable, keep only for authoritative sources (reduces stale/unknown-time noise)
            is_auth = any(src in url.lower() or src in title.lower()
                          for src in AUTHORITATIVE_SOURCES)

            pub_dt = parse_pub_datetime(pub)
            if DEBUG_TRACE:
                try:
                    _dbg_line("\n[DEBUG_TRACE][News] Tavily article candidate")
                    _dbg_line(f"title={title}")
                    _dbg_line(f"url={url}")
                    _dbg_line(f"pub_date_raw={pub}")
                    _dbg_line(f"pub_date_parsed={(pub_dt.isoformat() if pub_dt else None)}")
                    _dbg_line(f"authority={'High' if is_auth else 'Standard'}")
                except Exception:
                    pass
            if pub_dt and pub_dt < cutoff:
                continue
            if (pub_dt is None) and (not is_auth):
                continue

            articles.append({
                "title":     title,
                "url":       url,
                "content":   content,
                "pub_date":  pub,
                "authority": "High" if is_auth else "Standard",
            })

        return {"found": len(articles) > 0, "articles": articles}

    except Exception as e:
        print(f"[News] Search failed: {e}")
        return {"found": False, "articles": []}


# ── LLM (single call) ──────────────────────────────

def call_llm(prompt: str, max_tokens: int = 600) -> str:
    if not OPENROUTER_KEY or OPENROUTER_KEY == "YOUR_OPENROUTER_KEY":
        return ""
    try:
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       "anthropic/claude-sonnet-4.6",
                "messages":    [{"role": "user", "content": prompt}],
                "max_tokens":  max_tokens,
                "temperature": 0.1,
            },
            timeout=20
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[LLM] Failed: {e}")
        return ""


def analyze_with_news(spike: dict, articles: list) -> tuple[str, bool]:
    """LLM + post-validate. Skips LLM if no HL evidence in news text."""
    articles_text = "\n\n".join([
        f"[{a['authority']}] {a['title']}\nSource: {a['url']}\n{a['content']}"
        for a in articles
    ])
    hl_list = ", ".join(HYPERLIQUID_ASSETS)
    allowed = extract_allowed_endpoints(articles)
    if not allowed:
        return FALLBACK_NO_CHAIN, False

    prompt = f"""你是一位跨市场传导链分析师。

Polymarket 预测市场刚刚出现显著波动：
市场：{spike['question']}
概率变化：{spike['price_was']:.1%} → {spike['price_now']:.1%}（{spike['delta']:+.1%}）

以下是刚刚发布的新闻文章：
{articles_text}

重要限制：只能使用上方文章中明确陈述的内容，不得引用训练数据中的背景知识或历史信息。

---

请列出置信度 ★★★★☆ 及以上的传导链条，最多输出2条，按置信度从高到低排列。

置信度由事件与资产之间的假设数量严格决定：
★★★★★ = 0个假设（事件直接涉及该资产）
★★★★☆ = 1个假设
（低于 ★★★★☆ 的链条不输出）

Hyperliquid 可交易资产（优先作为链条终点）：{hl_list}

格式：
Chain 1: ★★★★★
[事件] → [TICKER]
资产：[TICKER] [HL]
假设：无

Chain 2: ★★★★☆
[事件] → [假设节点] → [TICKER]
资产：[TICKER] [HL]
假设：[具体说明这一个假设]

规则：
- 不判断方向（涨/跌），由用户决定
- ticker 符号保持英文（BTC、ETH 等）
- 链条推理用中文输出"""

    if DEBUG_TRACE:
        try:
            _dbg_line("\n" + "=" * 70)
            _dbg_line("[DEBUG_TRACE][TrackA] analyze_with_news()")
            _dbg_line(f"trace_key={spike.get('market_id', '')}")
            _dbg_line(f"articles_count={len(articles)}")
            _dbg_line(f"allowed_endpoints={sorted(allowed)}")
            _dbg_line("[DEBUG_TRACE][LLM prompt (truncated)]")
            _dbg_line(_dbg_truncate(prompt, DEBUG_TRACE_PROMPT_MAX_CHARS))
            _dbg_line("=" * 70)
        except Exception:
            pass

    raw = call_llm(prompt, max_tokens=600)
    if DEBUG_TRACE:
        try:
            _dbg_line("\n" + "-" * 70)
            _dbg_line("[DEBUG_TRACE][LLM result]")
            _dbg_line(raw)
            _dbg_line("-" * 70)
        except Exception:
            pass
    return post_validate_chain_analysis(raw, allowed)


def _send_message(text: str):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        print(f"\n{'='*55}\n{text}\n{'='*55}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     text,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=10
        )
    except Exception as e:
        print(f"[Telegram] Failed: {e}")


def send_alert(spike: dict, news: dict, analysis: str = ""):
    """
    Primary alert. Two modes:
    - confirmed (news found):   full facts + chain analysis
    - unconfirmed (no news):    spike data only, follow-up queued
    """
    arrow     = "📈" if spike["delta"] > 0 else "📉"
    delta_str = f"+{spike['delta']:.1%}" if spike["delta"] > 0 else f"{spike['delta']:.1%}"
    cat_map   = {"crypto": "Crypto", "geopolitics": "Geopolitics", "macro": "Macro"}
    category  = cat_map.get(spike["category"], spike["category"])
    strategy  = "⚡ 近期到期 — Poly 均值回归优先" if spike["days_left"] <= 12 else "📡 适合 HL 传导链信号"

    confirmed = news["found"]
    header    = "⚡ *价格异动信号*" if confirmed else "⚡ *价格异动 — 待确认*"

    stats = (
        f"`{arrow} {delta_str}  ·  {spike['price_was']:.1%} → {spike['price_now']:.1%}`\n"
        f"`📅 {spike['days_left']}天  ·  {strategy}`\n\n"
    )

    if confirmed:
        news_lines = _format_news_lines(news["articles"], max_items=2)
        body = (
            f"*News:*\n{news_lines}\n\n"
            f"{analysis}\n\n"
            f"🔵 *Hyperliquid:* https://app.hyperliquid.xyz/trade"
        )
    else:
        body = (
            f"⚠️ *无新闻 — 信号待确认*\n"
            f"→ 可能是知情早盘或情绪驱动\n"
            f"→ {SCAN_INTERVAL // 60} 分钟后跟进"
        )

    msg = (
        f"{header}\n\n"
        f"*Market:* {spike['question'][:80]}\n"
        f"*Category:* {category}\n\n"
        f"{stats}"
        f"{body}\n\n"
        f"🔗 https://polymarket.com/event/{spike['slug']}\n"
        f"_{datetime.now().strftime('%H:%M:%S')}_"
    )

    _send_message(msg)
    print(f"[Telegram] Alert sent ({'confirmed' if confirmed else 'unconfirmed'})")


def send_followup(spike: dict, news: dict, analysis: str):
    """Follow-up message for previously unconfirmed spikes (sent once, 5 min later)"""
    if news["found"]:
        news_lines = _format_news_lines(news["articles"], max_items=2)
        body = (
            f"✅ *新闻确认*\n\n"
            f"*News:*\n{news_lines}\n\n"
            f"{analysis}\n\n"
            f"🔵 *Hyperliquid:* https://app.hyperliquid.xyz/trade"
        )
    else:
        body = (
            f"❌ *跟进无新闻*\n"
            f"→ 均值回归概率较高"
        )

    msg = (
        f"🔄 *Signal Update:* {spike['question'][:60]}\n\n"
        f"{body}\n\n"
        f"_{datetime.now().strftime('%H:%M:%S')}_"
    )

    _send_message(msg)
    print(f"[Telegram] Follow-up sent ({'confirmed' if news['found'] else 'no news'})")


# ── Combined (multi-category) event handling ───────

def search_news_combined(spikes: list) -> dict:
    """Single broad Tavily search covering all spiking markets."""
    stop_words = {"will", "would", "could", "should", "by", "in", "the", "a",
                  "an", "be", "to", "of", "or", "and", "before", "after",
                  "reach", "hit", "drop", "rise", "win", "lose", "end", "during",
                  "through", "until", "ever", "become", "get", "remain", "stay"}
    months = {
        "january", "february", "march", "april", "may", "june", "july",
        "august", "september", "sept", "october", "november", "december",
        "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    }
    now_dt = datetime.now(timezone.utc)
    cutoff     = now_dt - timedelta(hours=2.5)   # widest window for big events

    # Collect keywords from all spike titles (up to 4 per market, deduped)
    seen_kw, all_kw = set(), []
    for spike in spikes:
        words = []
        for w in re.sub(r"[?']", "", spike["question"]).split():
            wl = w.lower()
            if wl in stop_words:
                continue
            if w.isdigit():
                continue
            w_norm = re.sub(r"[^a-zA-Z0-9]", "", w)
            if not w_norm:
                continue
            wl_norm = w_norm.lower()
            if wl_norm in months or wl_norm in stop_words:
                continue
            if wl_norm.isdigit():
                continue
            if not re.search(r"[a-zA-Z]", w_norm):
                continue
            if len(w_norm) < 2:
                continue
            words.append(w_norm)
        for w in words[:4]:
            wl_norm = w.lower()
            if wl_norm not in seen_kw:
                seen_kw.add(wl_norm)
                all_kw.append(w)

    query = f"{' '.join(all_kw[:10])} -polymarket -\"prediction market\""

    try:
        if DEBUG_TRACE:
            try:
                _dbg_line("\n" + "=" * 70)
                _dbg_line("[DEBUG_TRACE][News] search_news_combined()")
                _dbg_line(f"spikes_count={len(spikes)}")
                _dbg_line(f"trace_keys={[s.get('market_id') for s in spikes][:5]}")
                _dbg_line(f"query={query}")
                _dbg_line(f"cutoff_utc={cutoff.isoformat()}")
                _dbg_line("=" * 70)
            except Exception:
                pass

        r = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key":      TAVILY_API_KEY,
                "query":        query,
                "search_depth": "basic",
                "max_results":  7,
                "days":         1,
            },
            timeout=15
        )
        r.raise_for_status()
        articles = []
        for a in r.json().get("results", []):
            pub = a.get("published_date", "")
            is_auth = any(src in a.get("url", "").lower() or src in a.get("title", "").lower()
                         for src in AUTHORITATIVE_SOURCES)
            pub_dt = parse_pub_datetime(pub)
            if DEBUG_TRACE:
                try:
                    _dbg_line("\n[DEBUG_TRACE][News] Tavily combined article candidate")
                    _dbg_line(f"title={a.get('title', '')}")
                    _dbg_line(f"url={a.get('url', '')}")
                    _dbg_line(f"pub_date_raw={pub}")
                    _dbg_line(f"pub_date_parsed={(pub_dt.isoformat() if pub_dt else None)}")
                    _dbg_line(f"authority={'High' if is_auth else 'Standard'}")
                except Exception:
                    pass
            if pub_dt and pub_dt < cutoff:
                continue
            if (pub_dt is None) and (not is_auth):
                continue
            articles.append({
                "title":     a.get("title", ""),
                "url":       a.get("url", ""),
                "content":   a.get("content", "")[:400],
                "pub_date":  pub,
                "authority": "High" if is_auth else "Standard",
            })
        return {"found": len(articles) > 0, "articles": articles}
    except Exception as e:
        print(f"[News] Combined search failed: {e}")
        return {"found": False, "articles": []}


def analyze_combined(spikes: list, articles: list) -> tuple[str, bool]:
    """LLM + post-validate. Skips LLM if no HL evidence in news text."""
    spikes_text = "\n".join([
        f"• {s['question']} | {s['price_was']:.1%}→{s['price_now']:.1%}（{s['delta']:+.1%}）[{s['category']}]"
        for s in spikes
    ])
    articles_text = "\n\n".join([
        f"[{a['authority']}] {a['title']}\nSource: {a['url']}\n{a['content']}"
        for a in articles
    ])
    hl_list = ", ".join(HYPERLIQUID_ASSETS)
    allowed = extract_allowed_endpoints(articles)
    if not allowed:
        return FALLBACK_NO_CHAIN, False

    prompt = f"""你是一位跨市场传导链分析师。

以下多个 Polymarket 预测市场同时出现显著波动：
{spikes_text}

这些波动涉及多个类别，可能由同一事件驱动。

以下是刚刚发布的新闻文章：
{articles_text}

重要限制：只能使用上方文章中明确陈述的内容，不得引用训练数据中的背景知识或历史信息。

---

请识别最可能的根本原因事件，并列出置信度 ★★★★☆ 及以上的跨市场传导链条，最多输出2条，按置信度从高到低排列。

置信度由事件与资产之间的假设数量严格决定：
★★★★★ = 0个假设（事件直接涉及该资产）
★★★★☆ = 1个假设
（低于 ★★★★☆ 的链条不输出）

Hyperliquid 可交易资产（优先作为链条终点）：{hl_list}

格式：
Chain 1: ★★★★★
[事件] → [TICKER]
资产：[TICKER] [HL]
假设：无

Chain 2: ★★★★☆
[事件] → [假设节点] → [TICKER]
资产：[TICKER] [HL]
假设：[具体说明这一个假设]

规则：
- 不判断方向（涨/跌），由用户决定
- ticker 符号保持英文（BTC、ETH 等）
- 链条推理用中文输出"""

    if DEBUG_TRACE:
        try:
            _dbg_line("\n" + "=" * 70)
            _dbg_line("[DEBUG_TRACE][TrackA] analyze_combined()")
            _dbg_line(f"spikes_count={len(spikes)} articles_count={len(articles)}")
            _dbg_line(f"allowed_endpoints={sorted(allowed)}")
            _dbg_line("[DEBUG_TRACE][LLM prompt (truncated)]")
            _dbg_line(_dbg_truncate(prompt, DEBUG_TRACE_PROMPT_MAX_CHARS))
            _dbg_line("=" * 70)
        except Exception:
            pass

    raw = call_llm(prompt, max_tokens=700)
    if DEBUG_TRACE:
        try:
            _dbg_line("\n" + "-" * 70)
            _dbg_line("[DEBUG_TRACE][LLM result]")
            _dbg_line(raw)
            _dbg_line("-" * 70)
        except Exception:
            pass
    return post_validate_chain_analysis(raw, allowed)


def send_combined_alert(spikes: list, news: dict, analysis: str):
    """Single merged alert for multi-category spike events."""
    categories = sorted(set(s["category"] for s in spikes))
    cat_map    = {"crypto": "Crypto", "geopolitics": "Geopolitics", "macro": "Macro"}
    cat_str    = " · ".join(cat_map.get(c, c) for c in categories)

    markets_text = "\n".join([
        f"• {s['question'][:55]}  `{'+'if s['delta']>0 else ''}{s['delta']:.1%}` "
        f"({s['price_was']:.1%}→{s['price_now']:.1%})"
        for s in spikes
    ])
    links = "\n".join([
        f"🔗 https://polymarket.com/event/{s['slug']}" for s in spikes[:3]
    ])

    if news["found"]:
        news_lines = _format_news_lines(news["articles"], max_items=2)
        body = (
            f"*News:*\n{news_lines}\n\n"
            f"{analysis}\n\n"
            f"🔵 *Hyperliquid:* https://app.hyperliquid.xyz/trade"
        )
    else:
        body = "⚠️ *暂无新闻确认* — 可能为知情资金或跨市场情绪联动"

    msg = (
        f"🌐 *多市场信号 — {cat_str}*\n\n"
        f"*Markets:*\n{markets_text}\n\n"
        f"{body}\n\n"
        f"{links}\n"
        f"_{datetime.now().strftime('%H:%M:%S')}_"
    )
    _send_message(msg)
    print(f"[Telegram] Combined alert: {len(spikes)} markets, categories: {cat_str}")


def handle_combined_event(spikes: list):
    now_ts = time.time()
    before = len(spikes)
    spikes = [s for s in spikes if not in_tracka_signal_cooldown(s["market_id"], s["delta"], now_ts)]
    tracka_stats["filtered_cooldown"] += (before - len(spikes))
    if not spikes:
        print("  [Cooldown] Combined event skipped (all markets cooling down)")
        return

    if not tracka_signal_rate_limit_allows(now_ts):
        tracka_stats["rate_limited_skips"] += 1
        print(f"  [Rate limit] Combined event skipped (cap {TRACKA_SIGNALS_PER_HOUR}/h)")
        return

    cats = sorted(set(s["category"] for s in spikes))
    print(f"  🌐 Multi-category event: {', '.join(cats)} ({len(spikes)} markets)")
    global _debug_trace_collector
    collector_started = False
    analysis, has_chain = "", False
    try:
        if DEBUG_TRACE and DEBUG_TRACE_TELEGRAM:
            _debug_trace_collector = []
            collector_started = True
            _dbg_line("\n[DEBUG_TRACE][TrackA][TelegramTrace] Combined spike received")
            _dbg_line(f"trace_market_ids={[s.get('market_id') for s in spikes]}")
            _dbg_line(f"categories={cats}")

        news = search_news_combined(spikes)
        if news["found"]:
            analysis, has_chain = analyze_combined(spikes, news["articles"])
        send_combined_alert(spikes, news, analysis)
        record_tracka_signal_send(now_ts)
        if news["found"]:
            if has_chain:
                tracka_stats["chain_confirmed_with"] += 1
            else:
                tracka_stats["chain_confirmed_without"] += 1

        if news["found"] and DEBUG_TRACE_TELEGRAM_SCOPE == "confirmed_only":
            trace_text = "\n".join(_debug_trace_collector or [])
            header = f"[DEBUG_TRACE][Telegram][confirmed] Multi-Market ({len(spikes)} markets)"
            _send_debug_trace_to_telegram(trace_text, header=header)
    finally:
        if collector_started:
            _debug_trace_collector = None

    tracka_stats["alerts_sent_markets"] += len(spikes)
    tracka_stats["alerts_sent_messages"] += 1
    tracka_stats["combined_alert_events"] += 1
    for s in spikes:
        mark_tracka_signal_sent(s["market_id"], s["delta"], now_ts)


# ── Spike handling ─────────────────────────────────

def handle_spike(spike: dict):
    if in_tracka_signal_cooldown(spike["market_id"], spike["delta"]):
        tracka_stats["filtered_cooldown"] += 1
        print(f"  [Cooldown] Skip repeat alert: {spike['question'][:50]}")
        return

    if not tracka_signal_rate_limit_allows():
        tracka_stats["rate_limited_skips"] += 1
        print(f"  [Rate limit] Skip signal (cap {TRACKA_SIGNALS_PER_HOUR}/h)")
        return

    print(f"  ⚡ Spike: {spike['question'][:50]} | {spike['delta']:+.1%}")
    global _debug_trace_collector
    collector_started = False
    try:
        if DEBUG_TRACE and DEBUG_TRACE_TELEGRAM:
            _debug_trace_collector = []
            collector_started = True
            _dbg_line("\n[DEBUG_TRACE][TrackA][TelegramTrace] Spike received")
            _dbg_line(f"market_id={spike.get('market_id')}")
            _dbg_line(f"category={spike.get('category')}")
            _dbg_line(f"question={spike.get('question')}")
            _dbg_line(f"price_was={spike.get('price_was')} price_now={spike.get('price_now')} delta={spike.get('delta')} (abs={abs(spike.get('delta', 0))})")
            _dbg_line(f"days_left={spike.get('days_left')} volume={spike.get('volume')}")

        news = search_news(spike)

        if news["found"]:
            analysis, has_chain = analyze_with_news(spike, news["articles"])
            send_alert(spike, news, analysis)
            tracka_stats["alerts_sent_markets"] += 1
            tracka_stats["alerts_sent_messages"] += 1
            record_tracka_signal_send()
            mark_tracka_signal_sent(spike["market_id"], spike["delta"])
            if has_chain:
                tracka_stats["chain_confirmed_with"] += 1
            else:
                tracka_stats["chain_confirmed_without"] += 1

            if DEBUG_TRACE_TELEGRAM_SCOPE == "confirmed_only":
                trace_text = "\n".join(_debug_trace_collector or [])
                header = f"[DEBUG_TRACE][Telegram][confirmed] {spike.get('question','')[:60]}"
                _send_debug_trace_to_telegram(trace_text, header=header)
        else:
            send_alert(spike, news)  # unconfirmed, no analysis
            tracka_stats["alerts_sent_markets"] += 1
            tracka_stats["alerts_sent_messages"] += 1
            record_tracka_signal_send()
            mark_tracka_signal_sent(spike["market_id"], spike["delta"])
            if spike["market_id"] not in pending_news_checks:
                print(f"  [News] Not found — queued for 1 follow-up")
                pending_news_checks[spike["market_id"]] = {
                    "spike":      spike,
                    "next_check": time.time() + SCAN_INTERVAL,
                }
            else:
                print(f"  [News] Already pending follow-up — skipped")
    finally:
        if collector_started:
            _debug_trace_collector = None


def process_pending_checks():
    now       = time.time()
    to_remove = []

    for mid, pending in pending_news_checks.items():
        if now < pending["next_check"]:
            continue

        spike = pending["spike"]
        print(f"  [News] Follow-up: {spike['question'][:45]}")
        global _debug_trace_collector
        collector_started = False
        try:
            if DEBUG_TRACE and DEBUG_TRACE_TELEGRAM:
                _debug_trace_collector = []
                collector_started = True
                _dbg_line("\n[DEBUG_TRACE][TrackA][TelegramTrace] Follow-up spike received")
                _dbg_line(f"market_id={spike.get('market_id')}")
                _dbg_line(f"category={spike.get('category')}")
                _dbg_line(f"question={spike.get('question')}")

            news = search_news(spike)
            if news["found"]:
                analysis, has_chain = analyze_with_news(spike, news["articles"])
                if has_chain:
                    tracka_stats["chain_confirmed_with"] += 1
                else:
                    tracka_stats["chain_confirmed_without"] += 1
            else:
                analysis, has_chain = "", False
            send_followup(spike, news, analysis)

            if news["found"] and DEBUG_TRACE_TELEGRAM_SCOPE == "confirmed_only":
                trace_text = "\n".join(_debug_trace_collector or [])
                header = f"[DEBUG_TRACE][Telegram][confirmed][Follow-up] {spike.get('question','')[:60]}"
                _send_debug_trace_to_telegram(trace_text, header=header)
        finally:
            if collector_started:
                _debug_trace_collector = None

        to_remove.append(mid)

    for mid in to_remove:
        del pending_news_checks[mid]


# ── Track B: RSS monitoring ────────────────────────

def fetch_rss(source: str, url: str) -> list:
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        root = ET.fromstring(r.text)

        def local_name(tag: str) -> str:
            # Support namespaced XML tags like {ns}item / {ns}title.
            return tag.split("}", 1)[-1] if "}" in tag else tag

        def first_child_text(node, *names: str) -> str:
            for child in list(node):
                if local_name(child.tag) in names:
                    return (child.text or "").strip()
            return ""

        articles = []

        # RSS style: <channel><item>...
        # Atom style: <feed><entry>...
        for node in root.iter():
            tag = local_name(node.tag)
            if tag not in ("item", "entry"):
                continue

            title = first_child_text(node, "title")
            pub_date = first_child_text(node, "pubDate", "published", "updated")

            link = ""
            for child in list(node):
                if local_name(child.tag) != "link":
                    continue
                # Atom link is often in href attr; RSS link is text node.
                link = (child.attrib.get("href") or (child.text or "")).strip()
                if link:
                    break

            if title and link:
                articles.append({
                    "title": title,
                    "url": link,
                    "pub_date": pub_date,
                    "source": source,
                })
        return articles
    except Exception as e:
        print(f"  [RSS] {source} failed: {e}")
        return []


def detect_rss_categories(title: str) -> list:
    t = title.lower()
    return [cat for cat, kws in RSS_ENTITY_KEYWORDS.items() if any(kw in t for kw in kws)]


def send_rss_alert(article: dict, categories: list):
    # Header based on content category, not source authority
    if len(categories) >= 2:
        header = "Breaking News"
    elif "crypto" in categories:
        header = "Crypto Update"
    elif "geopolitics" in categories:
        header = "Geopolitics Flash"
    elif "macro" in categories:
        header = "Policy Alert"
    else:
        header = "News"

    src   = article["source"]
    title = article["title"][:80]
    pub   = format_pub_date(article["pub_date"]) if article["pub_date"] else "?"

    msg = (
        f"📰 *{header}*\n\n"
        f"{src} · {pub}\n"
        f"{title}\n\n"
        f"🔗 {article['url']}"
    )
    _send_message(msg)
    print(f"  [RSS] Alert: {title[:50]}")


def check_rss_feeds(baseline: bool = False):
    """
    baseline=True: first run, populate seen set without alerting.
    baseline=False: alert only on new articles with matching keywords.
    """
    print(f"  [RSS] Checking {len(RSS_FEEDS)} feeds{'  (baseline)' if baseline else ''}")
    for source, url in RSS_FEEDS.items():
        articles = fetch_rss(source, url)
        for a in articles:
            if a["url"] in seen_rss_articles:
                continue
            seen_rss_articles.add(a["url"])
            if baseline:
                continue
            cats = detect_rss_categories(a["title"])
            if cats:
                send_rss_alert(a, cats)


# ── Startup & main loop ────────────────────────────

def send_startup():
    msg = (
        f"🚀 *Polymarket Monitor v5*\n"
        f"Categories: Crypto / Geopolitics / Macro\n"
        f"Price range: {PRICE_MIN:.0%} – {PRICE_MAX:.0%}\n"
        f"Spike threshold: {SPIKE_THRESHOLD:.0%}\n"
        f"Spike min volume: {MIN_SPIKE_VOLUME:,.0f}\n"
        f"Signal cooldown: {COOLDOWN_LONG_MINUTES}m same-dir / {COOLDOWN_REVERSAL_MINUTES}m reversal\n"
        f"Track A signals/hour cap: {TRACKA_SIGNALS_PER_HOUR}\n"
        f"RSS (Track B): {'on' if RSS_ENABLED else 'off'}\n"
        f"Min days to expiry: {MIN_DAYS_LEFT}\n"
        f"Single-pass analysis (facts + chains)\n"
        f"Scan interval: {SCAN_INTERVAL // 60} min"
    )
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception:
        pass


def maybe_report_tracka_stats(now_ts: float):
    window_seconds = 4 * 3600
    if now_ts - tracka_stats["window_start"] < window_seconds:
        return

    scans = tracka_stats["market_scans"]
    avg_markets = (tracka_stats["markets_tracked_total"] / scans) if scans else 0.0
    cw = tracka_stats["chain_confirmed_with"]
    cwo = tracka_stats["chain_confirmed_without"]
    chain_total = cw + cwo
    pct_s = f"{100.0 * cw / chain_total:.0f}%" if chain_total else "n/a"

    line = (
        f"Track A 近4h | 平均市场数={avg_markets:.1f} | "
        f"候选={tracka_stats['spike_candidates']} | 通过筛选={tracka_stats['spikes_passed_filters']} | "
        f"过滤:成交量={tracka_stats['filtered_min_volume']}/噪声={tracka_stats['filtered_noise_band']}/"
        f"冷却={tracka_stats['filtered_cooldown']}/限速跳过={tracka_stats['rate_limited_skips']} | "
        f"信号:市场={tracka_stats['alerts_sent_markets']}/消息={tracka_stats['alerts_sent_messages']}/"
        f"合并={tracka_stats['combined_alert_events']} | "
        f"新闻确认链条 有链={cw} 无链={cwo} 有链占比={pct_s}"
    )

    print("\n[Track A 统计] " + line)

    if SEND_TRACKA_STATS_TO_TELEGRAM:
        msg = (
            "📊 *Track A 统计（近4小时）*\n\n"
            f"`{line}`\n\n"
            f"_{datetime.now().strftime('%H:%M:%S')}_"
        )
        _send_message(msg)

    tracka_stats["window_start"] = now_ts
    tracka_stats["market_scans"] = 0
    tracka_stats["markets_tracked_total"] = 0
    tracka_stats["spike_candidates"] = 0
    tracka_stats["filtered_min_volume"] = 0
    tracka_stats["filtered_noise_band"] = 0
    tracka_stats["spikes_passed_filters"] = 0
    tracka_stats["filtered_cooldown"] = 0
    tracka_stats["rate_limited_skips"] = 0
    tracka_stats["alerts_sent_markets"] = 0
    tracka_stats["alerts_sent_messages"] = 0
    tracka_stats["combined_alert_events"] = 0
    tracka_stats["chain_confirmed_with"] = 0
    tracka_stats["chain_confirmed_without"] = 0

def run():
    print("=" * 55)
    print("Polymarket Monitor v5" + ("  (+RSS Track B)" if RSS_ENABLED else ""))
    rss_part = f" | RSS: {RSS_INTERVAL}s" if RSS_ENABLED else ""
    print(f"Threshold: {SPIKE_THRESHOLD:.0%} | Market scan: {SCAN_INTERVAL}s{rss_part}")
    print("=" * 55)
    send_startup()

    # Initialise timers so first iteration triggers both immediately
    last_market_scan = 0
    last_rss_check   = 0
    scan             = 0
    rss_baseline_done = False

    while True:
        now = time.time()
        ts  = datetime.now().strftime('%H:%M:%S')

        # ── Track B: RSS check ──────────────────────
        if RSS_ENABLED and (now - last_rss_check >= RSS_INTERVAL):
            print(f"\n[{ts}] RSS check")
            check_rss_feeds(baseline=not rss_baseline_done)
            rss_baseline_done = True
            last_rss_check    = now

        # ── Track A: Market scan ────────────────────
        if now - last_market_scan >= SCAN_INTERVAL:
            scan += 1
            print(f"\n[{ts}] Scan #{scan}")

            markets = fetch_markets()
            print(f"  Markets tracked: {len(markets)}")
            tracka_stats["market_scans"] += 1
            tracka_stats["markets_tracked_total"] += len(markets)

            if scan == 1:
                print("  Baseline established. Detection starts next scan.")
                for m in markets:
                    price_history[m["id"]] = {"time": time.time(), "price": m["price"]}
                    print(f"  [{m['category']:>12}] {m['question'][:50]} | {m['price']:.1%}")
            else:
                spikes = []
                for m in markets:
                    spike = check_spike(m)
                    if spike:
                        spikes.append(spike)

                if spikes:
                    categories = set(s["category"] for s in spikes)
                    if len(categories) >= 2:
                        handle_combined_event(spikes)
                    else:
                        for spike in spikes:
                            handle_spike(spike)

                process_pending_checks()

                if pending_news_checks:
                    print(f"  Pending follow-ups: {len(pending_news_checks)}")

            last_market_scan = now

        maybe_report_tracka_stats(now)
        time.sleep(60)  # heartbeat — both tracks use their own timers


if __name__ == "__main__":
    run()
