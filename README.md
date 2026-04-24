# Polymarket Monitor v6
An AI-powered Polymarket spike monitoring system with a three-role architecture (`rules-only` / `llm-only` / `human-only), persistent storage, and dual Telegram bot workflow.

## What it does
- Scans active Polymarket markets every 5 minutes and tracks rolling price history
- Detects significant short-interval price spikes (default `>= 2%` over scan interval)
- Applies market gating and risk controls (liquidity filter, spike volume threshold, cooldown, hourly cap)
- Runs news retrieval via Sonar and performs LLM judgment in a dedicated analysis step
- Persists signals and context into Supabase (`markets`, `spikes`, `articles`, `human_clicks`)
- Sends **Bot A** alert messages with inline judgment buttons for fast human feedback
- Receives webhook callbacks from Bot A, then sends **Bot B** read-only context echo for each click
- Runs a background resolution loop to backfill market outcomes after close

## How it works
The system is organized into three roles:

### 1) rules-only (deterministic pipeline)
- Fetches market data from Polymarket Gamma API
- Runs deterministic filters and spike detection
- Applies RC policies:
  - per-market cooldown
  - reversal cooldown
  - max signals per hour
- Only candidates that pass rules proceed to AI stage

### 2) llm-only (news + judgment)
- Searches related news with Sonar
- Uses Claude (via OpenRouter) to classify and explain the spike
- Produces structured judgment payload used by downstream notification/storage

### 3) human-only (Telegram interaction)
- Bot A sends actionable alert cards with inline buttons
- User click triggers webhook callback
- Bot B posts a read-only confirmation/context message for the specific click
- Every human action is append-only logged in Supabase (`human_clicks`)

## Runtime model
`monitor_v6.py` starts three concurrent coroutines in one event loop:
- `scan_loop` (market scan and signal generation)
- `serve_webhook` (Bot A callback endpoint)
- `resolution_loop` (periodic settlement backfill)

## Setup

### 1) Clone repository
```bash
git clone https://github.com/<your-username>/polymarket-monitor.git
cd polymarket-monitor
