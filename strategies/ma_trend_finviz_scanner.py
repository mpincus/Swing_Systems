from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf

# ================= CONFIG =================

LOOKBACK_DAYS = 180          # ~6 months
MIN_BARS = 120               # enough history for 100 SMA
MAX_EXTENSION = 0.05         # 5% max from ema21

REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "output"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"

# Simple fixed universe for debugging. We can plug Finviz back in later.
TICKERS = ["AAPL", "MSFT", "NVDA", "META", "TSLA", "AMZN", "GOOGL", "NFLX", "AVGO", "AMD"]


# ================= HELPERS =================

def fetch_history(ticker: str) -> Optional[pd.DataFrame]:
    """Download ~6 months of daily data for ticker. Return None if unusable."""
    end = datetime.utcnow().date()
    start = end - timedelta(days=LOOKBACK_DAYS)

    df = yf.download(
        ticker,
        start=start,
        end=end + timedelta(days=1),
        interval="1d",
        auto_adjust=True,
        progress=False,
    )

    if df is None or df.empty:
        print(f"[DEBUG] {ticker}: empty history")
        return None

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns:
        print(f"[DEBUG] {ticker}: no 'Close' column")
        return None

    df = df[pd.notna(df["Close"])]
    if df.empty:
        print(f"[DEBUG] {ticker}: all Close values NaN")
        return None

    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()

    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]

    df["ema21"] = close.ewm(span=21, adjust=False).mean()
    df["sma50"] = close.rolling(50).mean()
    df["sma100"] = close.rolling(100).mean()
    df["sma200"] = close.rolling(200).mean()

    fast = close.ewm(span=10, adjust=False).mean()
    slow = close.ewm(span=21, adjust=False).mean()
    df["mom"] = fast - slow
    df["mom_signal"] = df["mom"].ewm(span=5, adjust=False).mean()

    return df


def classify_long(df: pd.DataFrame) -> Dict:
    last = df.iloc[-1].copy()

    close = float(last["Close"])
    ema21 = float(last["ema21"])
    sma50 = float(last["sma50"])
    sma100 = float(last["sma100"])
    sma200 = float(last["sma200"]) if not pd.isna(last["sma200"]) else None

    trend_ok = False
    if len(df) >= MIN_BARS:
        trend_ok = close > sma50 and ema21 > sma50 and sma50 > sma100

    signal = "NONE"

    if trend_ok:
        # Pullback under ema21 in last 3 bars
        if len(df) >= 4:
            pulled_back = (df["Close"].iloc[-4:-1] < df["ema21"].iloc[-4:-1]).any()
        else:
            pulled_back = (df["Close"] < df["ema21"]).any()

        reclaimed = close > ema21
        not_extended = close <= ema21 * (1 + MAX_EXTENSION)

        bull_now = float(last["mom"]) > float(last["mom_signal"])
        if len(df) >= 4:
            prev_m = df["mom"].iloc[-4:-1]
            prev_s = df["mom_signal"].iloc[-4:-1]
            crossed = (prev_m <= prev_s).any() and bull_now
        else:
            crossed = bull_now

        if pulled_back and reclaimed and crossed and not_extended:
            signal = "ENTRY"
        elif pulled_back and reclaimed:
            signal = "WATCH"
        elif pulled_back:
            signal = "WATCH"

    return {
        "direction": "LONG",
        "trend_ok": trend_ok,
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": close,
        "ema21": ema21,
        "sma50": sma50,
        "sma100": sma100,
        "sma200": sma200,
    }


def classify_short(df: pd.DataFrame) -> Dict:
    last = df.iloc[-1].copy()

    close = float(last["Close"])
    ema21 = float(last["ema21"])
    sma50 = float(last["sma50"])
    sma100 = float(last["sma100"])
    sma200 = float(last["sma200"]) if not pd.isna(last["sma200"]) else None

    trend_ok = False
    if len(df) >= MIN_BARS:
        trend_ok = close < sma50 and ema21 < sma50 and sma50 < sma100

    signal = "NONE"

    if trend_ok:
        # Rally above ema21 in last 3 bars
        if len(df) >= 4:
            rallied = (df["Close"].iloc[-4:-1] > df["ema21"].iloc[-4:-1]).any()
        else:
            rallied = (df["Close"] > df["ema21"]).any()

        rejected = close < ema21
        not_extended = close >= ema21 * (1 - MAX_EXTENSION)

        bear_now = float(last["mom"]) < float(last["mom_signal"])
        if len(df) >= 4:
            prev_m = df["mom"].iloc[-4:-1]
            prev_s = df["mom_signal"].iloc[-4:-1]
            crossed = (prev_m >= prev_s).any() and bear_now
        else:
            crossed = bear_now

        if rallied and rejected and crossed and not_extended:
            signal = "ENTRY"
        elif rallied and rejected:
            signal = "WATCH"
        elif rallied:
            signal = "WATCH"

    return {
        "direction": "SHORT",
        "trend_ok": trend_ok,
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": close,
        "ema21": ema21,
        "sma50": sma50,
        "sma100": sma100,
        "sma200": sma200,
    }


# ================= MAIN =================

def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict] = []

    print(f"[DEBUG] Universe: {TICKERS}")

    for t in TICKERS:
        print(f"[DEBUG] {t}: start")
        df = fetch_history(t)
        if df is None:
            print(f"[DEBUG] {t}: skipped (no usable history)")
            continue

        if len(df) < MIN_BARS:
            print(f"[DEBUG] {t}: skipped (too few bars: {len(df)})")
            continue

        df = add_indicators(df)

        required_cols = ["ema21", "sma50", "sma100"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[DEBUG] {t}: skipped (missing cols: {missing})")
            continue

        df = df.dropna(subset=required_cols)
        if df.empty:
            print(f"[DEBUG] {t}: skipped (indicators all NaN)")
            continue

        long_row = classify_long(df)
        short_row = classify_short(df)

        long_row["ticker"] = t
        short_row["ticker"] = t

        rows.append(long_row)
        rows.append(short_row)

        print(f"[DEBUG] {t}: OK (long={long_row['signal']}, short={short_row['signal']})")

    if not rows:
        # Hard fail so you see it clearly in GitHub Actions
        raise RuntimeError("No rows generated for any ticker. See [DEBUG] logs above.")

    out = pd.DataFrame(rows)

    # Optional: sort with stronger signals first
    signal_rank = {"ENTRY": 0, "WATCH": 1, "NONE": 2}
    dir_rank = {"LONG": 0, "SHORT": 1}
    out["sr"] = out["signal"].map(signal_rank).fillna(9)
    out["dr"] = out["direction"].map(dir_rank).fillna(9)
    out = out.sort_values(["sr", "dr", "ticker"]).drop(columns=["sr", "dr"])

    out.to_csv(OUTPUT_CSV, index=False)
    print(f"[DEBUG] Wrote {len(out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()