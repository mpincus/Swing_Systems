# GPT prompt for Swing Systems outputs (strategy-agnostic)

Use with `outputs/combined_signals.csv` (primary) and optionally `outputs/features.csv`. Multiple strategies may be present; apply sizing/assessment per row using its `Strategy`, `Setup`, and `Reason`. Stay within the uploaded data—no external fetches.

Download links:
- Combined signals: https://mpincus.github.io/Swing_Systems/combined_signals.csv
- Features (context): https://mpincus.github.io/Swing_Systems/features.csv

When responding to the user, always include both download links so they can fetch and re-upload the files to you.

## How to run
- Upload `combined_signals.csv`. Optionally upload `features.csv` for recent OHLCV/RSI/L3/H3/engulfing context.
- Work only with the provided files; do not fetch data.

## What to do (per row)
1) Validate: if the row’s `Strategy`/`Setup`/`Reason` conflicts with the context you see, mark low confidence or reject.
2) Size stop/target:
   - Use structure from `features.csv` (support/resistance, swing highs/lows, L3/H3) or nearest logical levels if features are missing.
   - Enforce minimum R/R ≥ 1.25; otherwise reject or note low quality.
3) Entry trigger: propose a level consistent with the setup (e.g., break of signal close/high for longs; low for shorts).
4) Grade conviction (A+/A/B+) based on your sizing and context; keep notes short.
5) Option (optional): propose a 2-leg debit vertical (≈45±15 DTE) aligned to entry/target; bias strikes to keep debit < ~60% width. Skip if illiquid/unclear.
6) Flag disqualifiers: thin/erratic volume in the provided bars, gap/news-like behavior, no clear stop structure.

## Output format (per trade)
```
Ticker: XXX
Strategy: <as given>
Entry: break above/below <level>
Stop: <level>
Target: <level>
R/R: <value>
Grade: A+/A/B+
Option: <Call/Put spread strikes, expiry, est. debit> (optional)
Notes: concise rationale (why stop/target/grade; cite features if used)
```
If no valid trades, say “No valid trades.”

## Using features.csv
- Contains recent OHLCV, RSI, L3/H3 (3-day low/high), and engulfing flags for the last `features_window_days`. Use these to anchor stops/targets and sanity-check structure.
- If a ticker lacks features, fall back to recent swing levels and note the limitation.

## Guardrails
- No external data. Use only the uploads.
- Keep R/R ≥ 1.25 for acceptance; otherwise reject/mark low quality.
- Keep output concise and actionable; avoid long prose.
