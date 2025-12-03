from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

# ================= CONFIG =================

# Put your actual Finviz screener URL here (with v=111 view)
FINVIZ_BASE_URL = "https://finviz.com/screener.ashx?v=111"

LOOKBACK_DAYS = 180
MIN_BARS = 120
MAX_EXTENSION = 0.05

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

FALLBACK_TICKERS = ["AAPL", "MSFT", "NVDA", "META", "TSLA"]


# ================= FINVIZ SCRAPER =================

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
        except Exception:
            break

        soup = BeautifulSoup(resp.text, "lxml")
        anchors = soup.select("a.screener-link-primary")

        page_syms: List[str] = []
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
        start += 20

    tickers.sort()
    return tickers


# ================= DATA FETCH =================

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

    if df.empty:
        return None

    df = df.rename(columns=str.capitalize)
    if "Close" not in df.columns:
        return None

    return df


# ================= INDICATORS =================

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


# ================= STRATEGY LOGIC =================

def classify_long(df: pd.DataFrame) -> Dict:
    last = df.iloc[-1].copy()

    close = float(last["Close"])
    ema21 = float(last["ema21"])
    sma50 = float(last["sma50"])
    sma100 = float(last["sma100"])
    sma200 = float(last["sma200"]) if not pd.isna(last["sma200"]) else None

    if len(df) >= MIN_BARS:
        trend_ok = close > sma50 and ema21 > sma50 and sma50 > sma100
    else:
        trend_ok = False

    signal = "NONE"

    if trend_ok:
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
        "trend_ok": bool(trend_ok),
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

    if len(df) >= MIN_BARS:
        trend_ok = close < sma50 and ema21 < sma50 and sma50 < sma100
    else:
        trend_ok = False

    signal = "NONE"

    if trend_ok:
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
        "trend_ok": bool(trend_ok),
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


# ================= MAIN =================

def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    tickers = get_finviz_tickers(FINVIZ_BASE_URL)
    if not tickers:
        tickers = FALLBACK_TICKERS.copy()

    rows: List[Dict] = []

    for t in tickers:
        df = fetch_history(t)
        if df is None:
            rows.extend(no_data_rows(t))
            continue

        df = add_indicators(df)

        df = df[~df.index.duplicated(keep="last")]
        df = df.dropna(subset=["ema21", "sma50", "sma100"])

        if df.empty or len(df) < MIN_BARS:
            rows.extend(no_data_rows(t))
            continue

        long_row = classify_long(df)
        short_row = classify_short(df)

        long_row["ticker"] = t
        short_row["ticker"] = t

        rows.append(long_row)
        rows.append(short_row)

    if rows:
        df_out = pd.DataFrame(rows)
        signal_rank = {"ENTRY": 0, "WATCH": 1, "NONE": 2, "NO_DATA": 3}
        dir_rank = {"LONG": 0, "SHORT": 1}
        df_out["sr"] = df_out["signal"].map(signal_rank).fillna(9)
        df_out["dr"] = df_out["direction"].map(dir_rank).fillna(9)
        df_out = df_out.sort_values(["sr", "dr", "ticker"]).drop(columns=["sr", "dr"])
    else:
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

    df_out.to_csv(OUTPUT_CSV, index=False)


if __name__ == "__main__":
    main()