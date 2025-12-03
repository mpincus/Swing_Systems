from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

FINVIZ_BASE_URL = "https://finviz.com/screener.ashx?v=111"

LOOKBACK_DAYS = 180
MIN_BARS = 120
MAX_EXTENSION = 0.05

REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "output"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

def _set_r_param(url: str, r_value: int) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["r"] = [str(r_value)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def get_finviz_tickers(base_url: str, max_pages: int = 50) -> List[str]:
    tickers = []
    seen = set()
    start = 1

    while True:
        if len(tickers) >= max_pages * 20:
            break

        page_url = _set_r_param(base_url, start)
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"[WARN] Finviz error at r={start}: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        anchors = soup.select("a.screener-link-primary")

        page_syms = []
        for a in anchors:
            sym = (a.text or "").strip().upper()
            if not sym or " " in sym:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            page_syms.append(sym)

        if not page_syms:
            break

        tickers.extend(page_syms)
        print(f"[INFO] page r={start}: {len(page_syms)} tickers")
        start += 20

    tickers.sort()
    print(f"[OK] {len(tickers)} total tickers from Finviz")
    return tickers

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
        print(f"[ERR] {ticker} fetch error: {e}")
        return None

    if df.empty:
        print(f"[INFO] {ticker}: no data")
        return None

    df = df.rename(columns=str.capitalize)
    if "Close" not in df.columns:
        return None

    return df

def add_indicators(df):
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

def classify_long(df):
    if len(df) < MIN_BARS:
        return None
    last = df.iloc[-1]

    trend_ok = (
        pd.notna(last[["ema21","sma50","sma100"]]).all() and
        last["Close"] > last["sma50"] and
        last["ema21"] > last["sma50"] and
        last["sma50"] > last["sma100"]
    )

    signal = "NONE"

    if trend_ok:
        if len(df) >= 4:
            pulled_back = (df["Close"].iloc[-4:-1] < df["ema21"].iloc[-4:-1]).any()
        else:
            pulled_back = (df["Close"] < df["ema21"]).any()

        reclaimed = last["Close"] > last["ema21"]
        not_ext = last["Close"] <= last["ema21"] * (1 + MAX_EXTENSION)

        bull_now = last["mom"] > last["mom_signal"]
        if len(df) >= 4:
            prev_mom = df["mom"].iloc[-4:-1]
            prev_sig = df["mom_signal"].iloc[-4:-1]
            crossed = (prev_mom <= prev_sig).any() and bull_now
        else:
            crossed = bull_now

        if pulled_back and reclaimed and crossed and not_ext:
            signal = "ENTRY"
        elif pulled_back and reclaimed:
            signal = "WATCH"
        elif pulled_back:
            signal = "WATCH"

    return {
        "direction": "LONG",
        "trend_ok": bool(trend_ok),
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": float(last["Close"]),
        "ema21": float(last["ema21"]),
        "sma50": float(last["sma50"]),
        "sma100": float(last["sma100"]),
        "sma200": float(last["sma200"]) if not pd.isna(last["sma200"]) else None,
    }

def classify_short(df):
    if len(df) < MIN_BARS:
        return None
    last = df.iloc[-1]

    trend_ok = (
        pd.notna(last[["ema21","sma50","sma100"]]).all() and
        last["Close"] < last["sma50"] and
        last["ema21"] < last["sma50"] and
        last["sma50"] < last["sma100"]
    )

    signal = "NONE"

    if trend_ok:
        if len(df) >= 4:
            rallied = (df["Close"].iloc[-4:-1] > df["ema21"].iloc[-4:-1]).any()
        else:
            rallied = (df["Close"] > df["ema21"]).any()

        rejected = last["Close"] < last["ema21"]
        not_ext = last["Close"] >= last["ema21"] * (1 - MAX_EXTENSION)

        bear_now = last["mom"] < last["mom_signal"]
        if len(df) >= 4:
            prev_mom = df["mom"].iloc[-4:-1]
            prev_sig = df["mom_signal"].iloc[-4:-1]
            crossed = (prev_mom >= prev_sig).any() and bear_now
        else:
            crossed = bear_now

        if rallied and rejected and crossed and not_ext:
            signal = "ENTRY"
        elif rallied and rejected:
            signal = "WATCH"
        elif rallied:
            signal = "WATCH"

    return {
        "direction": "SHORT",
        "trend_ok": bool(trend_ok),
        "signal": signal,
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": float(last["Close"]),
        "ema21": float(last["ema21"]),
        "sma50": float(last["sma50"]),
        "sma100": float(last["sma100"]),
        "sma200": float(last["sma200"]) if not pd.isna(last["sma200"]) else None,
    }

def main():
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    tickers = get_finviz_tickers(FINVIZ_BASE_URL)
    print(f"[INFO] Tickers: {len(tickers)}")

    rows = []
    hist_ok = 0

    for t in tickers:
        df = fetch_history(t)
        if df is None:
            continue
        hist_ok += 1

        df = add_indicators(df)

        long_row = classify_long(df)
        short_row = classify_short(df)

        for row in (long_row, short_row):
            if row is None:
                continue
            entry = row.copy()
            entry["ticker"] = t
            rows.append(entry)

    print(f"[INFO] History OK for {hist_ok} tickers. Rows: {len(rows)}")

    if rows:
        df_out = pd.DataFrame(rows)
        signal_rank = {"ENTRY": 0, "WATCH": 1, "NONE": 2}
        dir_rank = {"LONG": 0, "SHORT": 1}
        df_out["sr"] = df_out["signal"].map(signal_rank)
        df_out["dr"] = df_out["direction"].map(dir_rank)
        df_out = df_out.sort_values(["sr", "dr", "ticker"]).drop(columns=["sr","dr"])
    else:
        df_out = pd.DataFrame(columns=[
            "ticker","direction","signal","as_of",
            "close","ema21","sma50","sma100","sma200","trend_ok"
        ])

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Wrote {len(df_out)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()