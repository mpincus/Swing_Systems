from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

# ================== CONFIG ==================

# TODO: replace this with YOUR actual Finviz screener URL (v=111 view recommended)
FINVIZ_BASE_URL = "https://finviz.com/screener.ashx?v=111"

LOOKBACK_DAYS = 180
MIN_BARS = 120          # enough for 100 SMA; 200 is optional info
MAX_EXTENSION = 0.05    # 5% above/below ema21 allowed

# Script lives in strategies/ma_trend/, output in strategies/ma_trend/output/
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

# ================== FINVIZ SCRAPER ==================

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

        from bs4 import BeautifulSoup  # already imported, but keep local clarity
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

# ================== DATA FETCH ==================

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

# ================== INDICATORS ==================

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"]

    df["ema21"] = close.ewm(span=21, adjust=False).mean()
    df["sma50"] = close.rolling(50).mean()
    df["sma100"] = close.rolling(100).mean()
    df["sma200"] = close.rolling(200).mean()  # info only, not required for trend

    fast = close.ewm(span=10, adjust=False).mean()
    slow = close.ewm(span=21, adjust=False).mean()
    df["mom"] = fast - slow
    df["mom_signal"] = df["mom"].ewm(span=5, adjust=False).mean()

    return df

# ================== STRATEGY LOGIC ==================
# Loosened version:
# - Trend: ema21 > sma50 > sma100 (LONG) or reverse (SHORT), ignore strict 200 ordering.
# - Pullback window: last 3 bars vs ema21.
# - Reclaim/reject: still on the most recent bar.
# - Momentum: bullish/bearish now AND crossed within last 3 bars.
# - Extension: 5%.

def _recent_any(series: pd.Series, window: int) -> bool:
    if len(series) < window:
        return series.any()
    return series.iloc[-window:].any()

def classify_long(df: pd.DataFrame) -> Optional[Dict]:
    if len(df) < MIN_BARS:
        return None

    last = df.iloc[-1]
    prev3 = df.iloc[-4:-1]  # previous up to 3 bars (if available)

    # Trend: clean uptrend but not obsessing over 200
    trend_ok = (
        pd.notna(last[["ema21", "sma50", "sma100"]]).all()
        and last["Close"] > last["sma50"]
        and last["ema21"] > last["sma50"]
        and last["sma50"] > last["sma100"]
    )

    signal = "NONE"

    if trend_ok:
        # Pullback within last 3 bars: close below ema21 at least once
        pulled_back_recent = _recent_any(
            df["Close"] < df["ema21"], window=4
        ) and (df["Close"].iloc[-1] >= df["ema21"].iloc[-1] or True)
        # More strictly, check prior bars only:
        if len(df) >= 4:
            pulled_back_recent = (df["Close"].iloc[-4:-1] < df["ema21"].iloc[-4:-1]).any()
        else:
            pulled_back_recent = (df["Close"] < df["ema21"]).any()

        # Reclaim on latest bar
        reclaimed_21 = last["Close"] > last["ema21"]

        # Not too extended from 21
        not_extended = last["Close"] <= last["ema21"] * (1 + MAX_EXTENSION)

        # Momentum: bullish now and crossed within last 3 bars
        bull_now = last["mom"] > last["mom_signal"]
        if len(df) >= 4:
            prev_mom = df["mom"].iloc[-4:-1]
            prev_sig = df["mom_signal"].iloc[-4:-1]
            crossed_recent = (prev_mom <= prev_sig).any() and bull_now
        else:
            crossed_recent = bull_now

        if pulled_back_recent and reclaimed_21 and crossed_recent and not_extended:
            signal = "ENTRY"
        elif pulled_back_recent and not bull_now:
            signal = "WATCH"
        elif pulled_back_recent and reclaimed_21:
            # loosen: good reclaim but momentum not perfect
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
        "sma200": round(float(last["sma200"]), 2) if not pd.isna(last["sma200"]) else None,
    }

def classify_short(df: pd.DataFrame) -> Optional[Dict]:
    if len(df) < MIN_BARS:
        return None

    last = df.iloc[-1]

    # Bear trend: 21 < 50 < 100
    trend_ok = (
        pd.notna(last[["ema21", "sma50", "sma100"]]).all()
        and last["Close"] < last["sma50"]
        and last["ema21"] < last["sma50"]
        and last["sma50"] < last["sma100"]
    )

    signal = "NONE"

    if trend_ok:
        # Rally within last 3 bars: close above ema21
        if len(df) >= 4:
            rallied_recent = (df["Close"].iloc[-4:-1] > df["ema21"].iloc[-4:-1]).any()
        else:
            rallied_recent = (df["Close"] > df["ema21"]).any()

        # Reject on latest bar
        rejected_21 = last["Close"] < last["ema21"]

        # Not too extended below 21
        not_extended = last["Close"] >= last["ema21"] * (1 - MAX_EXTENSION)

        # Momentum: bearish now and crossed within last 3 bars
        bear_now = last["mom"] < last["mom_signal"]
        if len(df) >= 4:
            prev_mom = df["mom"].iloc[-4:-1]
            prev_sig = df["mom_signal"].iloc[-4:-1]
            crossed_recent = (prev_mom >= prev_sig).any() and bear_now
        else:
            crossed_recent = bear_now

        if rallied_recent and rejected_21 and crossed_recent and not_extended:
            signal = "ENTRY"
        elif rallied_recent and not bear_now:
            signal = "WATCH"
        elif rallied_recent and rejected_21:
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
        "sma200": round(float(last["sma200"]), 2) if not pd.isna(last["sma200"]) else None,
    }

# ================== MAIN ==================

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
        print("[INFO] No signals; writing empty CSV with headers.")

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Wrote {len(df_out)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()