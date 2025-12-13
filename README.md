# Swing Systems (RSI pipeline)

This repo builds daily Finviz watchlists, fetches OHLCV from Yahoo Finance, runs four RSI-based strategies, and writes small CSVs you can hand to GPT. Outputs are also copied to `docs/` for easy download or GitHub Pages.

## What happens each run (5pm ET via workflow or manual)
1) Scrape Finviz per strategy -> `data/watchlists/<strategy>.csv`.
2) Download recent OHLCV (default 200 days) for the union of all watchlist tickers -> `data/prices.csv`.
3) Run strategies:
   - `rsi_reversal_long`: RSI <= 30 + bullish engulfing; stop = prior 3-day low; target = 1.25R.
   - `rsi_reversal_short`: RSI >= 70 + bearish engulfing; stop = prior 3-day high; target = 1.25R.
   - `rsi_continuation_long`: RSI > 70 and rising (momentum continuation); same stop/target logic.
   - `rsi_continuation_short`: RSI < 30 and falling (downside momentum); symmetric stop/target.
   - `ma_momentum_long`: price > $30, trend (50>200; close > 200); triggers on pullback near EMA21/SMA50, fresh EMA9/EMA21 bullish crossover, or within ~3% of 20d high. GPT sizes stops/targets.
   - `ma_momentum_short`: price > $30, downtrend (50<200; close < 200); triggers on pullback near EMA21/SMA50, fresh EMA9/EMA21 bearish crossover, or within ~3% of 20d low. GPT sizes stops/targets.
   - `macd_long`: price > $30, close > 200, MACD (12-26) crosses above signal (9) in an uptrend. GPT sizes stops/targets.
   - `macd_short`: price > $30, close < 200, MACD (12-26) crosses below signal (9) in a downtrend. GPT sizes stops/targets.
   - `bull_flag_long`: Finviz bull flag pattern (optionable, >$30). GPT sizes stops/targets.
   - `bear_flag_short`: Finviz bear flag pattern (optionable, >$30). GPT sizes stops/targets.
   - `channel_up_long`: Finviz channel up pattern (optionable, >$30). GPT sizes stops/targets.
   - `channel_down_short`: Finviz channel down pattern (optionable, >$30). GPT sizes stops/targets.
   Signals require R >= 1.25; grade is A+/A/B+ from R buckets; includes a short reason.
4) Write per-strategy signals -> `outputs/<strategy>/signals.csv`.
5) Write combined signals -> `outputs/combined_signals.csv` (has a `Strategy` column).
6) Mirror outputs (and watchlists) to `docs/` for download/Pages.

## Detailed workflow (who calls what)
1) You or the GitHub Action invoke `python -m core.runner`.
2) `core.runner` loads settings from `configs/settings.yaml` (paths, Finviz URLs, lookback windows).
3) For each strategy module in `src/strategies/`, runner reads its Finviz URLs and calls `core.finviz.fetch_watchlist` -> saves `data/watchlists/<strategy>.csv`.
4) Runner unions all tickers and calls `core.data.fetch_prices` -> writes `data/prices.csv` (Yahoo OHLCV).
5) Runner sends the per-strategy slice of prices into each strategy’s `generate`:
   - Strategies use `core.indicators` to add RSI, engulfing flags, 3-day high/low.
   - They enforce their rules (RSI bands, engulfing or rising/falling RSI), require R >= 1.25, grade by R bucket, and build rows with stops/targets/reasons.
6) Runner writes each strategy’s signals to `outputs/<strategy>/signals.csv`.
7) Runner stacks all signals into `outputs/combined_signals.csv` (adds `Strategy` column).
8) Runner mirrors outputs (and watchlists) into `docs/` so you can download from Pages/artifacts.
9) The GitHub Action `.github/workflows/daily.yml` (cron 0 22 * * 1-5) installs deps, runs the runner, mirrors to docs, uploads artifacts, and commits/pushes changes.

## Quick start (manual run)
```bash
python -m venv .venv
.\.venv\Scripts\activate            # on mac/linux: source .venv/bin/activate
pip install -r requirements.txt
set PYTHONPATH=src                  # on PowerShell; mac/linux: export PYTHONPATH=src
python -m core.runner
```
Outputs: `outputs/**` and mirrored copies in `docs/`. Watchlists live in `data/watchlists/`.

## Configuration (configs/settings.yaml)
- `lookback_days`: price history window for OHLCV downloads (default 60 calendar days).
- `signal_history_days`: keep this many days of signals in the CSVs (default 10).
- `finviz.throttle_seconds`: delay between Finviz page requests (be polite).
- `finviz.user_agent`: UA string for Finviz requests.
- `paths`: where data/outputs/docs live (defaults match repo layout).
- `strategies.<name>.urls`: Finviz screens per strategy; change to tighten/loosen universes.
- `max_union_tickers`: cap total tickers downloaded (0 = no cap; default 0 since Stooq is primary).
- `data_source`: `stooq` by default; or `auto` (try Yahoo then Stooq) / force `yahoo`.
- `generate_signals`: true to run built-in strategies; false to skip signals and only emit GPT features.
- `features_window_days`: number of recent days to keep in `outputs/price_history_all.csv` for GPT context.

## Repo layout (new code)
- `configs/settings.yaml` — knobs for lookback, history window, Finviz throttles, URLs.
- `src/core/config.py` — loads settings, ensures folders exist.
- `src/core/finviz.py` — scrapes tickers from Finviz screens and saves watchlists.
- `src/core/data.py` — pulls OHLCV from Yahoo for the union of tickers.
- `src/core/indicators.py` — RSI/engulfing/3-day high-low helpers.
- `src/core/strategy_base.py` — simple Strategy interface + R-based grading helper.
- `src/core/runner.py` — pipeline orchestrator; auto-discovers strategies; writes outputs + mirrors to docs.
- `src/strategies/*.py` — the four RSI strategies (reversal/continuation long/short).
- `.github/workflows/daily.yml` — 5pm ET schedule: install → run → mirror → commit/push outputs.
- `requirements.txt` — minimal deps.

Legacy content is under `old/` and is not used by this pipeline.

## Adding a strategy
Create `src/strategies/<name>.py` with a `strategy` object implementing `Strategy.generate(prices, history_days)`. Return a DataFrame with columns:
`Date, Ticker, Strategy, Setup, Side, EntryTrigger, Stop, Target, R, Grade, GradeBasis, Reason`.
The runner will auto-discover and include it; add Finviz URLs in `configs/settings.yaml`.

## Handing files to GPT
- Primary upload: `outputs/combined_signals.csv` (has `Strategy` column). Keeps last N days (N = `signal_history_days`).
- Per-strategy uploads: `outputs/<strategy>/signals.csv` if you want to focus on one strategy.
- Raw OHLCV is not needed by GPT; it stays internal in `data/prices.csv`.
- If you disable signals (`generate_signals: false`), use `outputs/price_history_all.csv` for GPT: includes recent OHLCV + RSI/L3/H3 + engulfing flags for the last `features_window_days`.
- A ready-made GPT prompt lives at `docs/gpt_prompt.md` (use with `combined_signals.csv`, optional `price_history_all.csv`).

## Troubleshooting
- Empty outputs: check `data/watchlists/*` (Finviz may have returned nothing) and internet access.
- Finviz blocks: increase `finviz.throttle_seconds` or adjust UA; try fewer URLs.
- Yahoo gaps: tickers delisted/renamed; verify tickers in watchlists.
- Workflow timing: cron is `0 22 * * 1-5` (5pm ET). Adjust in `.github/workflows/daily.yml` if needed.
