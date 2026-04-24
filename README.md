# Polymarket Monitor v6

An AI-powered Polymarket spike monitoring system with a three-role architecture (`rules-only` / `llm-only` / `human-only`), Supabase persistence, and dual Telegram bot workflow.

## What it does

- Scans active Polymarket markets every 5 minutes and maintains rolling price history.
- Detects short-interval price spikes (default threshold: `>= 2%` per scan interval).
- Applies deterministic risk controls: liquidity filter, spike volume threshold, cooldown windows, and hourly signal cap.
- Retrieves related news with Sonar and performs LLM judgment via OpenRouter.
- Persists market/spike/news/human-feedback data into Supabase (`markets`, `spikes`, `articles`, `human_clicks`).
- Sends Bot A alert messages with inline judgment buttons.
- Handles webhook callbacks from Bot A, then sends Bot B read-only contextual echo messages.
- Runs a periodic resolution loop to backfill settled market outcomes.

## How it works

The system runs in a three-role pipeline:

### 1) rules-only (deterministic)

- Fetches market data from Polymarket Gamma API.
- Applies deterministic market gating and spike detection.
- Enforces RC policies:
  - per-market cooldown
  - reversal cooldown
  - max signals per hour
- Only candidates that pass all deterministic checks proceed to AI analysis.

### 2) llm-only (analysis)

- Searches related information using Sonar.
- Calls Claude (via OpenRouter) for structured judgment.
- Produces machine-readable outputs for downstream notification and storage.

### 3) human-only (interaction)

- Bot A delivers actionable alerts with inline buttons.
- User clicks are received through webhook callbacks.
- Bot B posts read-only context echoes for each click.
- Every click is append-only logged into `human_clicks`.

## Runtime model

`monitor_v6.py` starts three concurrent coroutines in one event loop:

- `scan_loop` — market scan + signal generation
- `serve_webhook` — Telegram callback endpoint
- `resolution_loop` — periodic settlement backfill

## Setup

### 1) Clone repository

```bash
git clone https://github.com/<your-username>/polymarket-monitor.git
cd polymarket-monitor
```

### 2) Install dependencies

```bash
pip install aiohttp python-dotenv supabase
```

### 3) Configure environment variables

Create a `.env` file in project root:

```env
OPENROUTER_API_KEY=your_openrouter_key

TELEGRAM_BOT_A_TOKEN=your_bot_a_token
TELEGRAM_BOT_B_TOKEN=your_bot_b_token
TELEGRAM_CHAT_ID=your_chat_id

SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key

WEBHOOK_URL=https://your-domain.com/telegram/webhook
WEBHOOK_SECRET_TOKEN=your_random_secret
```

### 4) Run

```bash
python monitor_v6.py
```

## Configuration

| Parameter | Default | Description |
|---|---:|---|
| `SCAN_INTERVAL` | `300s` | Seconds between market scans |
| `SPIKE_THRESHOLD` | `0.02` | Minimum price change to trigger a spike candidate |
| `MIN_LIQUIDITY` | `2000` | Minimum market liquidity |
| `MIN_SPIKE_VOLUME` | `20000` | Minimum post-trigger volume |
| `COOLDOWN_LONG_MINUTES` | `60` | Cooldown for same-direction repeated signal |
| `COOLDOWN_REVERSAL_MINUTES` | `30` | Cooldown for reversal-direction signal |
| `SIGNALS_PER_HOUR` | `2` | Global max alerts per hour |
| `NOISE_BAND_DAYS_LEFT_MAX` | `12` | Near-expiry noise-band day threshold |
| `NOISE_BAND_PRICE_LOW` | `0.40` | Noise-band lower price bound |
| `NOISE_BAND_PRICE_HIGH` | `0.60` | Noise-band upper price bound |
| `SONAR_MODEL` | `perplexity/sonar` | News retrieval model |
| `LLM_MODEL` | `anthropic/claude-sonnet-4.6` | LLM judgment model |

## Database schema (Supabase)

Core tables:

- `markets` — market metadata snapshots (upserted)
- `spikes` — detected spike events + rule/LLM outcomes
- `articles` — related news items linked to spikes
- `human_clicks` — append-only human judgment actions

## Telegram workflow

- **Bot A**: outbound alerts + inbound webhook callback handling
- **Bot B**: outbound read-only context echo messages
- Both bots send to the same `TELEGRAM_CHAT_ID`

## Changelog

### v6 vs v5

- Introduced three-role architecture: `rules-only / llm-only / human-only`.
- Upgraded Telegram integration to dual-bot model (Bot A interactive, Bot B read-only).
- Added webhook server (`aiohttp`) with secret-token verification.
- Added Supabase persistence with four core tables.
- Added resolution backfill loop for post-close market outcome updates.
- Migrated RC accounting to Bot A successful-send checkpoint.
- Improved observability and replayability through persistent event records.

## Research motivation

Prediction markets aggregate distributed information in real time.  
This project explores whether short-horizon Polymarket price spikes, combined with AI-assisted news interpretation and explicit human feedback loops, can improve actionable signal quality for discretionary or systematic workflows.

## Tech stack

- Python 3.10+
- Polymarket Gamma API
- OpenRouter API (Claude + Sonar)
- Supabase
- Telegram Bot API
- aiohttp
- python-dotenv
