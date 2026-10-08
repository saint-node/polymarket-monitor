"""
Polymarket Monitor v4
New in v4: Transmission chain analysis
- After news summary, AI generates potential affected assets in traditional markets
- You filter and decide which chain is real and worth acting on
- Signal must be real (news-backed) before chain analysis runs
"""

import requests, time, json, re
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────
TELEGRAM_TOKEN   = "YOUR_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"
TAVILY_API_KEY   = "YOUR_TAVILY_KEY"
OPENROUTER_KEY   = "YOUR_OPENROUTER_KEY"

# ── Parameters ────────────────────────────────────
SCAN_INTERVAL    = 300
SPIKE_THRESHOLD  = 0.02
MIN_LIQUIDITY    = 5_000
MIN_DAYS_LEFT    = 15
PRICE_MIN        = 0.10
PRICE_MAX        = 0.90
NEWS_MAX_RETRIES = 3

GAMMA_API = "https://gamma-api.polymarket.com"

# Hyperliquid available perp assets (for signal display)
HYPERLIQUID_ASSETS = [
    "BTC", "ETH", "SOL", "BNB", "AVAX", "DOGE", "LINK", "ARB", "OP",
    "MATIC", "APT", "SUI", "INJ", "TIA", "WIF", "PEPE", "NEAR", "ATOM",
    "DOT", "ADA", "XRP", "LTC", "BCH", "FIL", "ICP", "AAVE", "UNI",
    "MKR", "CRV", "JUP", "SEI", "BLUR", "GMX", "DYDX", "RUNE", "PYTH",
    "XAU", "XAG",
]
HL_ASSET_SET = set(HYPERLIQUID_ASSETS)

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

NEWS_WINDOW_HOURS = {
    "crypto":      2,
    "geopolitics": 8,
    "macro":       4,
}

AUTHORITATIVE_SOURCES = [
    "reuters", "bloomberg", "ap ", "associated press", "bbc",
    "financial times", "wall street journal", "wsj", "ft.com",
    "coindesk", "cointelegraph", "axios", "politico"
]

price_history = {}
pending_news_checks = {}


def classify_market(question: str) -> str:
    q = question.lower()
    for category, keywords in MARKET_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return category
    return "other"


def fetch_markets() -> list:
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={
            "limit": 100, "active": "true", "closed": "false",
            "order": "volumeNum", "ascending": "false",
            "liquidity_num_min": MIN_LIQUIDITY,
        }, timeout=15)
        r.raise_for_status()

        now = datetime.now(timezone.utc)
        markets = []

        for m in r.json():
            try:
                prices    = json.loads(m.get("outcomePrices", "[]"))
                if not prices: continue
                price     = float(prices[0])
                if price < PRICE_MIN or price > PRICE_MAX: continue

                end_str   = m.get("endDate", "")
                if not end_str: continue
                end_date  = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                days_left = (end_date - now).days
                if days_left < MIN_DAYS_LEFT: continue

                question  = m.get("question", "")
                category  = classify_market(question)
                if category == "other": continue

                token_ids = json.loads(m.get("clobTokenIds", "[]"))
                if not token_ids: continue

                markets.append({
                    "id":        m["id"],
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
        return {
            "question":   market["question"],
            "price_now":  price,
            "price_was":  last["price"],
            "delta":      delta,
            "volume":     market["volume"],
            "end_date":   market["end_date"],
            "days_left":  market["days_left"],
            "market_id":  market["id"],
            "category":   market["category"],
            "spike_time": now,
        }
    return None


def search_news(spike: dict, retry: int = 0) -> dict:
    if not TAVILY_API_KEY or TAVILY_API_KEY == "YOUR_TAVILY_KEY":
        return {"found": False, "articles": [], "retry": retry}

    try:
        r = requests.post(
            "https://api.tavily.com/search",
            json={
                "api_key":      TAVILY_API_KEY,
                "query":        spike["question"],
                "search_depth": "basic",
                "max_results":  5,
                "days":         1,
            },
            timeout=15
        )
        r.raise_for_status()
        results  = r.json().get("results", [])
        articles = []

        for a in results:
            title   = a.get("title", "")
            url     = a.get("url", "")
            content = a.get("content", "")[:400]
            is_auth = any(src in url.lower() or src in title.lower()
                         for src in AUTHORITATIVE_SOURCES)
            articles.append({
                "title":     title,
                "url":       url,
                "content":   content,
                "pub_date":  a.get("published_date", ""),
                "authority": "High" if is_auth else "Standard",
            })

        return {"found": len(articles) > 0, "articles": articles, "retry": retry}

    except Exception as e:
        print(f"[News] Search failed: {e}")
        return {"found": False, "articles": [], "retry": retry}


def call_llm(prompt: str, max_tokens: int = 300) -> str:
    """Shared LLM call via OpenRouter"""
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
                "model":       "anthropic/claude-haiku-4.5",
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


def summarize_news(spike: dict, news_result: dict) -> str:
    """Extract key facts from news, no evaluation"""
    if not news_result["found"]:
        return "No relevant news found."

    articles_text = "\n\n".join([
        f"Title: {a['title']}\nSource: {a['url']}\nContent: {a['content']}"
        for a in news_result["articles"]
    ])

    prompt = f"""You are a news fact extractor.

Market question: {spike['question']}

News articles:
{articles_text}

Extract key facts strictly in this format:

Fact 1: [one sentence, only what happened, no evaluation]
Fact 2: [one sentence, only what happened, no evaluation]
Fact 3: [if there is a third key fact]

Rules:
- State facts only, no "this means", "could lead to", "favorable for"
- No evaluative words like "major", "breakthrough", "deterioration"
- If multiple articles cover the same event, write only one fact
- Keep proper nouns, numbers, and dates exactly as they appear"""

    result = call_llm(prompt, max_tokens=200)
    if not result:
        return "\n".join([f"- {a['title']}" for a in news_result["articles"]])
    return result


def analyze_transmission_chain(spike: dict, news_summary: str) -> str:
    """
    Generate potential transmission chain to traditional markets.
    AI provides breadth, user filters and decides.
    Only runs when news is confirmed real.
    """
    direction = "increases" if spike["delta"] > 0 else "decreases"

    hl_list = ", ".join(HYPERLIQUID_ASSETS)
    prompt = f"""You are a cross-market transmission analyst.

A Polymarket prediction market just moved — informed money is repricing event probability.
Your job: map all plausible logical chains from this event to affected assets. Cast wide net.

Market: {spike['question']}
Probability move: {spike['price_was']:.1%} → {spike['price_now']:.1%} ({spike['delta']:+.1%})
News: {news_summary}

Hyperliquid assets (prefer these): {hl_list}

For each chain, explicitly list every assumption node between the event and the asset.
Confidence is determined STRICTLY by number of assumptions — fewer = higher, never subjective.

Confidence scale:
★★★★★ = 0 assumptions (event directly involves this asset)
★★★★☆ = 1 assumption
★★★☆☆ = 2 assumptions
★★☆☆☆ = 3 assumptions
★☆☆☆☆ = 4+ assumptions

Format each chain strictly as:

Chain N: ★[confidence]
[Event] → [assumption 1 if any] → [assumption 2 if any] → [TICKER]
Asset: [TICKER] [HL if on Hyperliquid]
Assumptions: [list each assumption explicitly, or "none" if direct]

Rules:
- List ALL plausible chains, not just obvious ones. Include indirect and sector chains.
- Do NOT state direction (long/short) — that is the user's judgment
- Each chain must end in a specific asset ticker
- Prefer Hyperliquid tickers at chain endpoints
- Max 6 chains total, ordered highest confidence first
- If two chains share intermediate nodes, list them separately"""

    result = call_llm(prompt, max_tokens=500)
    if not result:
        return "Unable to generate transmission analysis."
    return result


def extract_hl_tickers(chain_text: str) -> list:
    """Find Hyperliquid assets mentioned in chain analysis output"""
    found = []
    for asset in HYPERLIQUID_ASSETS:
        if re.search(r'\b' + asset + r'\b', chain_text, re.IGNORECASE):
            found.append(asset)
    return found


def assess_time_relation(spike: dict, news_result: dict) -> str:
    if not news_result["found"]:
        return "No news found"
    retry = news_result.get("retry", 0)
    if retry == 0:
        return "News found immediately"
    return f"News found after ~{retry * 5} min delay"


def send_telegram_alert(spike: dict, news_result: dict, summary: str, chain: str):
    """Send full alert with news summary + transmission chain"""
    direction = "📈 Up" if spike["delta"] > 0 else "📉 Down"
    delta_str = f"+{spike['delta']:.1%}" if spike["delta"] > 0 else f"{spike['delta']:.1%}"
    time_rel  = assess_time_relation(spike, news_result)
    cat_map   = {"crypto": "Crypto", "geopolitics": "Geopolitics", "macro": "Macro"}
    category  = cat_map.get(spike["category"], spike["category"])

    if news_result["found"]:
        news_lines = "\n".join([
            f"[{a['authority']}] {a['title'][:55]}"
            for a in news_result["articles"][:3]
        ])
        news_section = f"*News:*\n{news_lines}\n\n*Facts:*\n{summary}"

        hl_tickers = extract_hl_tickers(chain)
        if hl_tickers:
            hl_links = "  ".join(
                f"[{t}](https://app.hyperliquid.xyz/trade/{t})"
                for t in hl_tickers
            )
            hl_section = f"\n\n🔵 *Hyperliquid:* {hl_links}"
        else:
            hl_section = ""

        chain_section = f"\n\n📊 *Logic chains (high → low confidence):*\n{chain}{hl_section}\n\n❓ *Which chains hold? What direction? Is it priced in on HL?*"
    else:
        retry = news_result.get("retry", 0)
        news_section = (
            f"*News:* No relevant news after {retry + 1} attempt(s)\n"
            f"→ Likely emotion-driven. Mean reversion probability higher."
        )
        chain_section = ""  # No chain analysis without confirmed news

    msg = (
        f"⚡ *Price Spike Alert*\n\n"
        f"*Market:* {spike['question'][:80]}\n"
        f"*Category:* {category}\n\n"
        f"```\n"
        f"Direction : {direction}\n"
        f"Spike     : {delta_str}\n"
        f"Now       : {spike['price_now']:.1%}\n"
        f"Before    : {spike['price_was']:.1%}\n"
        f"Days left : {spike['days_left']}\n"
        f"News      : {time_rel}\n"
        f"```\n\n"
        f"{news_section}"
        f"{chain_section}\n\n"
        f"🔗 https://polymarket.com/event/{spike['market_id']}\n"
        f"_{datetime.now().strftime('%H:%M:%S')}_"
    )

    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        print(f"\n{'='*55}\n{msg}\n{'='*55}")
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     msg,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=10
        )
        print("[Telegram] Alert sent")
    except Exception as e:
        print(f"[Telegram] Failed: {e}")


def handle_spike(spike: dict):
    print(f"  ⚡ Spike: {spike['question'][:50]} | {spike['delta']:+.1%}")
    news = search_news(spike, retry=0)

    if news["found"]:
        summary = summarize_news(spike, news)
        chain   = analyze_transmission_chain(spike, summary)
        send_telegram_alert(spike, news, summary, chain)
        return

    print(f"  [News] Not found, queued for retry")
    pending_news_checks[spike["market_id"]] = {
        "spike":      spike,
        "retries":    0,
        "next_check": time.time() + SCAN_INTERVAL,
    }


def process_pending_checks():
    now       = time.time()
    to_remove = []

    for mid, pending in pending_news_checks.items():
        if now < pending["next_check"]:
            continue

        pending["retries"] += 1
        retry = pending["retries"]
        spike = pending["spike"]

        print(f"  [News] Retry {retry}: {spike['question'][:45]}")
        news = search_news(spike, retry=retry)

        if news["found"] or retry >= NEWS_MAX_RETRIES - 1:
            summary = summarize_news(spike, news)
            # Only generate chain if news was actually found
            chain = analyze_transmission_chain(spike, summary) if news["found"] else ""
            send_telegram_alert(spike, news, summary, chain)
            to_remove.append(mid)
        else:
            pending["next_check"] = now + SCAN_INTERVAL

    for mid in to_remove:
        del pending_news_checks[mid]


def send_startup():
    msg = (
        f"🚀 *Polymarket Monitor v4*\n"
        f"Categories: Crypto / Geopolitics / Macro\n"
        f"Price range: {PRICE_MIN:.0%} - {PRICE_MAX:.0%}\n"
        f"Spike threshold: {SPIKE_THRESHOLD:.0%}\n"
        f"Min days to expiry: {MIN_DAYS_LEFT}\n"
        f"News + Transmission chain analysis enabled\n"
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


def run():
    print("=" * 55)
    print("Polymarket Monitor v4")
    print(f"Threshold: {SPIKE_THRESHOLD:.0%} | Interval: {SCAN_INTERVAL}s")
    print("=" * 55)
    send_startup()

    scan = 0
    while True:
        scan += 1
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"\n[{ts}] Scan #{scan}")

        markets = fetch_markets()
        print(f"  Markets tracked: {len(markets)}")

        if scan == 1:
            print("  Baseline established. Detection starts next scan.")
            for m in markets:
                price_history[m["id"]] = {"time": time.time(), "price": m["price"]}
                print(f"  [{m['category']:>12}] {m['question'][:50]} | {m['price']:.1%}")
        else:
            for m in markets:
                spike = check_spike(m)
                if spike:
                    handle_spike(spike)

            process_pending_checks()

            if pending_news_checks:
                print(f"  Pending retries: {len(pending_news_checks)}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run()
