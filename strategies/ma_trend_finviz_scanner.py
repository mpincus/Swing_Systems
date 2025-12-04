from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd
import yfinance as yf

# =============== CONFIG =================

LOOKBACK_DAYS = 180              # ~6 months
MIN_TREND_BARS = 100             # bars needed to call it a "trend"
MAX_EXTENSION = 0.05             # 5% above/below ema21 allowed

# Adjust these paths to match your repo structure if needed
REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "output"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"

# Simple universe so this actually runs reliably
TICKERS = ["AAPL", "MSFT", "NVDA", "META", "TSLA"]


# =============== DATA FETCH =================

def fetch_history(ticker: str) -> Optional[pd.DataFrame]:
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
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns:
        return None

    # Drop rows where Close is NaN
    df = df[pd.notna(df["Close"])]
    if df.empty:
        return None

    return df


# =============== INDICATORS =================

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]

    df["ema21"] = close.ewm(span=21, adjust=False).mean()
    df["sma50"] = close.rolling(50, min_periods=1).mean()
    df["sma100"] = close.rolling(100, min_periods=1).mean()
    df["sma200"] = close.rolling(200, min_periods=1).mean()

    fast = close.ewm(span=10, adjust=False).mean()
    slow = close.ewm(span=21, adjust=False).mean()
    df["mom"] = fast - slow
    df["mom_signal"] = df["mom"].ewm(span=5, adjust=False).mean()

    return df


# =============== STRATEGY LOGIC =================

def classify_long(df: pd.DataFrame) -> Dict:
    last = df.iloc[-1].copy()

    close = float(last["Close"])
    ema21 = float(last["ema21"])
    sma50 = float(last["sma50"])
    sma100 = float(last["sma100"])
    sma200 = float(last["sma200"])

    trend_ok = False
    if len(df) >= MIN_TREND_BARS:
        trend_ok = close > sma50 and ema21 > sma50 and sma50 > sma100

    signal = "NONE"

    if trend_ok:
        # Pullback below ema21 in last 3 bars
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
    sma200 = float(last["sma200"])

    trend_ok = False
    if len(df) >= MIN_TREND_BARS:
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


def no_data_rows(ticker: str) -> List[Dict]:
    base = {
        "ticker": ticker,
        "signal": "NO_DATA",
        "as_of": "",
        "close": None,
        "ema21": None,
        "sma50": None,
        "sma100": None,
        "sma200": None,
        "trend_ok": False,
    }
    long_row = base.copy()
    long_row["direction"] = "LONG"
    short_row = base.copy()
    short_row["direction"] = "SHORT"
    return [long_row, short_row]


# =============== MAIN =================

def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict] = []

    for t in TICKERS:
        df = fetch_history(t)
        if df is None:
            rows.extend(no_data_rows(t))
            continue

        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

        df = add_indicators(df)

        long_row = classify_long(df)
        short_row = classify_short(df)

        long_row["ticker"] = t
        short_row["ticker"] = t

        rows.append(long_row)
        rows.append(short_row)

    # Absolute guarantee: never write only headers
    if not rows:
        rows.append({
            "ticker": "DEBUG",
            "direction": "LONG",
            "signal": "TEST",
            "as_of": datetime.utcnow().strftime("%Y-%m-%d"),
            "close": 0.0,
            "ema21": 0.0,
            "sma50": 0.0,
            "sma100": 0.0,
            "sma200": 0.0,
            "trend_ok": False,
        })

    out = pd.DataFrame(rows)
    out.to_csv(OUTPUT_CSV, index=False)


if __name__ == "__main__":
    main()