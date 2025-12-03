"""
ma_trend_finviz_scanner.py

- Scrapes ALL tickers from a Finviz screener (across pages)
- Downloads ~6 months of daily data via yfinance
- Computes:
    * 21 EMA
    * 50 / 100 / 200 SMA
    * MACD-style momentum proxy (fast EMA - slow EMA + signal)
- Applies MA trend rules for BOTH directions:

  LONG ENTRY:
    1) Trend:
         ema21 > sma50 > sma100 > sma200
         close > sma50
    2) Pullback and reclaim:
         yesterday close < ema21
         today    close > ema21
    3) Momentum cross up:
         today mom > mom_signal
         yesterday mom <= mom_signal
    4) Not extended:
         close <= ema21 * 1.03

  SHORT ENTRY (mirror):
    1) Trend:
         ema21 < sma50 < sma100 < sma200
         close < sma50
    2) Rally and reject:
         yesterday close > ema21
         today    close < ema21
    3) Momentum cross down:
         today mom < mom_signal
         yesterday mom >= mom_signal
    4) Not extended:
         close >= ema21 * (1 - 0.03)

  Signals:
    ENTRY  - valid swing entry candidate
    WATCH  - pullback/rally present, trend ok, momentum not yet crossed
    NONE   - not interesting for that direction

- Writes: strategies/ma_trend/output/ma_trend_signals.csv
  (always created, even if there are 0 signals)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup


# ---------- CONFIG ----------

# REPLACE THIS with your actual Finviz screener URL (v=111 view works best)
FINVIZ_BASE_URL = (
    "https://finviz.com/screener.ashx?v=111&f=exch_nasd,sh_avgvol_o500,sh_price_o10"
)

LOOKBACK_DAYS = 180       # ~6 months
MIN_BARS = 200            # enough history for 200 SMA
MAX_EXTENSION = 0.03      # 3% from ema21

# This script is in strategies/ma_trend/, outputs live in strategies/ma_trend/output/
REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "output"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


# ---------- FINVIZ SCRAPER ----------

def _set_r_param(url: str, r_value: int) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["r"] = [str(r_value)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_finviz_tickers(base_url: str, max_pages: int = 50) -> List[str]:
    tickers: List[str] = []
    seen: set[str] = set()
    start = 1

    while True:
        if len(tickers) >= max_pages * 20:
            break

        page_url = _set_r_param(base_url, start)
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"[WARN] Finviz HTTP error at r={start}: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        anchors = soup.select("a.screener-link-primary")

        page_symbols: List[str] = []
        for a in anchors:
            sym = (a.text or "").strip().upper()
            if not sym or " " in sym:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            page_symbols.append(sym)

        if not page_symbols:
            break

        tickers.extend(page_symbols)
        print(f"[INFO] Finviz page r={start}: {len(page_symbols)} symbols")

        start += 20

    tickers.sort()
    print(f"[OK] Collected {len(tickers)} tickers from Finviz screener")
    return tickers


# ---------- DATA FETCH ----------

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
    except Exception as e:
        print(f"[ERR] {ticker}: error fetching data: {e}")
        return None

    if df.empty:
        print(f"[INFO] {ticker}: no data returned")
        return None

    df = df.rename(columns=str.capitalize)
    if "Close" not in df.columns:
        print(f"[WARN] {ticker}: missing Close column after normalize")
        return None

    return df


# ---------- INDICATORS ----------

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


# ---------- STRATEGY LOGIC ----------

def classify_long(df: pd.DataFrame) -> Optional[Dict]:
    if len(df) < MIN_BARS:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    trend_ok = (
        pd.notna(last[["ema21", "sma50", "sma100", "sma200"]]).all()
        and last["Close"] > last["sma50"]
        and last["ema21"] > last["sma50"]
        and last["sma50"] > last["sma100"] > last["sma200"]
    )

    signal = "NONE"

    if trend_ok:
        pulled_back = prev["Close"] < prev["ema21"]
        reclaimed_21 = last["Close"] > last["ema21"]
        not_extended = last["Close"] <= last["ema21"] * (1 + MAX_EXTENSION)

        mom_cross_up = (
            last["mom"] > last["mom_signal"]
            and prev["mom"] <= prev["mom_signal"]
        )

        if pulled_back and reclaimed_21 and mom_cross_up and not_extended:
            signal = "ENTRY"
        elif pulled_back and not mom_cross_up:
            signal = "WATCH"

    return {
        "direction": "LONG",
        "trend_ok": bool(trend_ok),
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": round(float(last["Close"]), 2),
        "ema21": round(float(last["ema21"]), 2),
        "sma50": round(float(last["sma50"]), 2),
        "sma100": round(float(last["sma100"]), 2),
        "sma200": round(float(last["sma200"]), 2),
    }


def classify_short(df: pd.DataFrame) -> Optional[Dict]:
    if len(df) < MIN_BARS:
        return None

    last = df.iloc[-1]
    prev = df.iloc[-2]

    trend_ok = (
        pd.notna(last[["ema21", "sma50", "sma100", "sma200"]]).all()
        and last["Close"] < last["sma50"]
        and last["ema21"] < last["sma50"]
        and last["sma50"] < last["sma100"] < last["sma200"]
    )

    signal = "NONE"

    if trend_ok:
        rallied = prev["Close"] > prev["ema21"]
        rejected_21 = last["Close"] < last["ema21"]
        not_extended = last["Close"] >= last["ema21"] * (1 - MAX_EXTENSION)

        mom_cross_down = (
            last["mom"] < last["mom_signal"]
            and prev["mom"] >= prev["mom_signal"]
        )

        if rallied and rejected_21 and mom_cross_down and not_extended:
            signal = "ENTRY"
        elif rallied and not mom_cross_down:
            signal = "WATCH"

    return {
        "direction": "SHORT",
        "trend_ok": bool(trend_ok),
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": round(float(last["Close"]), 2),
        "ema21": round(float(last["ema21"]), 2),
        "sma50": round(float(last["sma50"]), 2),
        "sma100": round(float(last["sma100"]), 2),
        "sma200": round(float(last["sma200"]), 2),
    }


# ---------- MAIN ----------

def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    tickers = get_finviz_tickers(FINVIZ_BASE_URL)
    print(f"[INFO] Total tickers from Finviz: {len(tickers)}")

    rows: List[Dict] = []

    for t in tickers:
        hist = fetch_history(t)
        if hist is None:
            continue

        hist = add_indicators(hist)

        long_row = classify_long(hist)
        short_row = classify_short(hist)

        if long_row and long_row["signal"] != "NONE":
            row = long_row.copy()
            row["ticker"] = t
            rows.append(row)

        if short_row and short_row["signal"] != "NONE":
            row = short_row.copy()
            row["ticker"] = t
            rows.append(row)

    # ALWAYS write the CSV, even if rows is empty
    if rows:
        df_out = pd.DataFrame(rows)
        signal_order = {"ENTRY": 0, "WATCH": 1}
        direction_order = {"LONG": 0, "SHORT": 1}
        df_out["signal_rank"] = df_out["signal"].map(signal_order).fillna(99)
        df_out["dir_rank"] = df_out["direction"].map(direction_order).fillna(99)
        df_out = (
            df_out.sort_values(["signal_rank", "dir_rank", "ticker"])
            .drop(columns=["signal_rank", "dir_rank"])
        )
        print(f"[OK] Generated {len(df_out)} signals.")
    else:
        # create empty dataframe with expected columns
        df_out = pd.DataFrame(
            columns=[
                "ticker",
                "direction",
                "signal",
                "as_of",
                "close",
                "ema21",
                "sma50",
                "sma100",
                "sma200",
                "trend_ok",
            ]
        )
        print("[INFO] No ENTRY/WATCH signals; writing empty CSV.")

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Wrote CSV to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()