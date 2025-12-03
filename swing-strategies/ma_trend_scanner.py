"""
ma_trend_scanner.py

Simple, self-contained scanner for your moving-average trend swing strategy.

- Reads tickers from: universe.txt   (one symbol per line)
- Downloads ~6 months of daily data via yfinance
- Computes:
    * 21 EMA
    * 50 / 100 / 200 SMA
    * MACD-style momentum proxy (fast EMA - slow EMA + signal)
- Applies your rules:

  Long-only ENTRY conditions:
    1) Trend:
         ema21 > sma50 > sma100 > sma200
         close > sma50
    2) Pullback and reclaim:
         yesterday close < ema21
         today close > ema21
    3) Momentum cross up:
         today mom > mom_signal
         yesterday mom <= mom_signal
    4) Not extended:
         close <= ema21 * 1.03

  Signals:
    ENTRY  - valid swing entry candidate
    WATCH  - trend + pullback present, but momentum not yet crossed up
    NONE   - ignore for this strategy

- Writes results to: outputs/ma_trend_signals.csv
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf


# ----------------- basic settings -----------------

LOOKBACK_DAYS = 180       # ~6 months
MIN_BARS = 200            # ensure enough data for 200 SMA
MAX_EXTENSION = 0.03      # 3% above ema21 allowed

# Paths are relative to this file's directory
REPO_ROOT = Path(__file__).resolve().parent
UNIVERSE_FILE = REPO_ROOT / "universe.txt"
OUTPUTS_DIR = REPO_ROOT / "outputs"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"


# ----------------- universe loading -----------------

def load_universe(path: Path = UNIVERSE_FILE) -> List[str]:
    """
    Read tickers from a text file (one per line).
    Empty lines and lines starting with '#' are ignored.
    """
    if not path.exists():
        print(f"[WARN] Universe file not found: {path}")
        return []

    tickers: List[str] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        tickers.append(line.upper())

    # remove duplicates and invalids
    tickers = [t for t in tickers if t and " " not in t]
    return sorted(set(tickers))


# ----------------- data fetching -----------------

def fetch_history(ticker: str) -> Optional[pd.DataFrame]:
    """
    Download daily OHLCV for the last LOOKBACK_DAYS using yfinance.
    """
    end = datetime.utcnow().date()
    start = end - timedelta(days=LOOKBACK_DAYS)

    try:
        df = yf.download(
            ticker,
            start=start,
            end=end + timedelta(days=1),
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        print(f"[ERR] {ticker}: error fetching data: {e}")
        return None

    if df.empty:
        print(f"[INFO] {ticker}: no data returned")
        return None

    df = df.rename(columns=str.capitalize)  # make sure 'Close' exists
    if "Close" not in df.columns:
        print(f"[WARN] {ticker}: no 'Close' column after normalization")
        return None

    return df


# ----------------- indicators -----------------

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 21 EMA, 50/100/200 SMA, and MACD-style momentum columns.
    """
    close = df["Close"]

    df["ema21"] = close.ewm(span=21, adjust=False).mean()
    df["sma50"] = close.rolling(50).mean()
    df["sma100"] = close.rolling(100).mean()
    df["sma200"] = close.rolling(200).mean()

    # MACD-style momentum proxy for your oscillator
    fast = close.ewm(span=10, adjust=False).mean()
    slow = close.ewm(span=21, adjust=False).mean()
    df["mom"] = fast - slow
    df["mom_signal"] = df["mom"].ewm(span=5, adjust=False).mean()

    return df


# ----------------- strategy logic -----------------

def classify_latest(df: pd.DataFrame) -> Optional[Dict]:
    """
    Classify the latest bar as ENTRY / WATCH / NONE based on MA trend rules.
    """
    if len(df) < MIN_BARS:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    # Trend filter: ema21 > sma50 > sma100 > sma200 and close > sma50
    trend_ok = (
        pd.notna(last[["ema21", "sma50", "sma100", "sma200"]]).all()
        and last["Close"] > last["sma50"]
        and last["ema21"] > last["sma50"]
        and last["sma50"] > last["sma100"] > last["sma200"]
    )

    if not trend_ok:
        signal = "NONE"
    else:
        # Pullback into ema21 yesterday, reclaim today
        pulled_back = prev["Close"] < prev["ema21"]
        reclaimed_21 = last["Close"] > last["ema21"]

        # Not extended: within MAX_EXTENSION above ema21
        not_extended = last["Close"] <= last["ema21"] * (1 + MAX_EXTENSION)

        # Momentum cross up today
        mom_cross_up = (
            last["mom"] > last["mom_signal"]
            and prev["mom"] <= prev["mom_signal"]
        )

        if pulled_back and reclaimed_21 and mom_cross_up and not_extended:
            signal = "ENTRY"
        elif pulled_back and not mom_cross_up:
            signal = "WATCH"
        else:
            signal = "NONE"

    return {
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": round(float(last["Close"]), 2),
        "ema21": round(float(last["ema21"]), 2),
        "sma50": round(float(last["sma50"]), 2),
        "sma100": round(float(last["sma100"]), 2),
        "sma200": round(float(last["sma200"]), 2),
        "trend_ok": bool(trend_ok),
        "signal": signal,
    }


# ----------------- main runner -----------------

def main() -> None:
    tickers = load_universe()
    if not tickers:
        print(f"[WARN] No tickers found in {UNIVERSE_FILE}")
        return

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict] = []

    for t in tickers:
        hist = fetch_history(t)
        if hist is None:
            continue

        hist = add_indicators(hist)
        row = classify_latest(hist)
        if row is None:
            continue

        row["ticker"] = t
        rows.append(row)

    if not rows:
        print("[INFO] No signals generated.")
        return

    df_out = pd.DataFrame(rows)

    # Sort: ENTRY first, then WATCH, then NONE, then by ticker
    signal_order = {"ENTRY": 0, "WATCH": 1, "NONE": 2}
    df_out["signal_rank"] = df_out["signal"].map(signal_order).fillna(3)
    df_out = df_out.sort_values(["signal_rank", "ticker"]).drop(columns=["signal_rank"])

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Wrote {len(df_out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
