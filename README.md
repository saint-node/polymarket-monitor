# Polymarket Monitor v5

An AI-powered monitoring system that detects price spikes on [Polymarket](https://polymarket.com) prediction markets and delivers real-time analysis via Telegram.

## What it does

- Scans hundreds of active Polymarket prediction markets every 5 minutes
- Detects significant price movements (≥2%) across crypto, geopolitics, and macro categories
- Filters noise with volume thresholds, cooldown periods, and a near-expiry noise band
- Searches for related news via Tavily to confirm whether spikes are news-driven
- Uses Claude (via OpenRouter) to summarize articles and identify market implications in a single LLM pass
- Pushes structured alerts to Telegram with Hyperliquid entry links
- Monitors RSS feeds (BBC, CoinDesk) for breaking headlines independent of market activity
- Reports Track A statistics to Telegram every 4 hours

## How it works

The system runs two parallel tracks:

**Track A — Market scanning**: Fetches top Polymarket markets by volume, filters by liquidity and days to expiry, and detects price spikes against a rolling baseline. When a spike is detected:
- If news is found → immediate alert with AI analysis (facts + causal chains in a single LLM pass)
- If no news is found → "unconfirmed signal" alert, queued for one follow-up check

The core insight: *no news found* suggests mean-reversion opportunity or informed money; *news found* guides positioning in Hyperliquid or traditional markets.

**Track B — RSS monitoring**: Independently monitors news feeds for geopolitical, macro, and crypto keywords, alerting on breaking headlines regardless of market activity.

## Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install requests
   ```
3. Set environment variables:
   ```bash
   export TELEGRAM_TOKEN=your_bot_token
   export TELEGRAM_CHAT_ID=your_chat_id
   export TAVILY_API_KEY=your_tavily_key
   export OPENROUTER_KEY=your_openrouter_key
   ```
4. Run:
   ```bash
   python monitor_v5.py
   ```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `SPIKE_THRESHOLD` | 2% | Minimum price move to trigger |
| `MIN_LIQUIDITY` | $2,000 | Minimum market liquidity |
| `MIN_SPIKE_VOLUME` | $20,000 | Post-trigger volume filter |
| `SPIKE_COOLDOWN_MINUTES` | 30 min | Suppresses repeat alerts per market |
| `MIN_DAYS_LEFT` | 7 days | Minimum days to expiry |
| `PRICE_MIN / PRICE_MAX` | 10%–90% | Price range filter |
| `SCAN_INTERVAL` | 300s | Seconds between market scans |
| `RSS_INTERVAL` | 180s | Seconds between RSS checks |

## Changelog

**v5 vs v4**
- Merged two LLM calls into one (raw articles → facts + causal chains in a single pass)
- Immediate "unconfirmed" alert + 1 follow-up (replaced 3-retry queue)
- "No news" treated as unconfirmed signal, not a conclusion
- Added volume filter (`MIN_SPIKE_VOLUME`) and cooldown (`SPIKE_COOLDOWN_MINUTES`)
- Added Track A statistics reporting every 4 hours
- Removed regex Hyperliquid ticker extraction → static entry link
- Improved RSS parser to handle both RSS and Atom feed formats

## Research motivation

Prediction markets are efficient aggregators of distributed information. Price spikes on Polymarket often precede or coincide with significant real-world events. This system explores whether prediction market signals can serve as leading indicators for positioning in crypto perpetuals (Hyperliquid) and traditional assets.

This project is part of ongoing research into consensus formation mechanisms and information aggregation in decentralized markets.

## Tech stack

- Python 3.10+
- Polymarket Gamma API
- Tavily Search API
- Claude via OpenRouter
- Telegram Bot API
