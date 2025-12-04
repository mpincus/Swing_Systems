from __future__ import annotations

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

# ================= CONFIG =================

# <<< INSERT YOUR FINVIZ SCREENER URL HERE >>>
FINVIZ_URL = "https://finviz.com/screener.ashx?v=111"  # default view

LOOKBACK_DAYS = 180

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True, parents=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0"
}


# ================= FINVIZ SCRAPER =================

def _set_r_param(url: str, r_value: int) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs["r"] = [str(r_value)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_finviz_tickers(base_url: str, max_pages: int = 200) -> list[str]:
    tickers = []
    seen = set()
    start = 1

    while True:
        page_url = _set_r_param(base_url, start)
        resp = requests.get(page_url, headers=HEADERS, timeout=20)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        anchors = soup.select("a.screener-link-primary")

        page_syms = []
        for a in anchors:
            sym = (a.text or "").strip().upper()
            if sym and " " not in sym and sym not in seen:
                seen.add(sym)
                page_syms.append(sym)

        if not page_syms:
            break

        tickers.extend(page_syms)
        start += 20

    tickers.sort()
    return tickers


# ================= DATA DOWNLOAD =================

def fetch_ohlcv(ticker: str, days: int) -> pd.DataFrame | None:
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)

    try:
        df = yf.download(
            ticker,
            start=start,
            end=end + timedelta(days=1),
            interval="1d",
            auto_adjust=False,
            progress=False
        )
    except Exception:
        return None

    if df is None or df.empty:
        return None

    # Fix multi-index
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    if "Close" not in df.columns:
        return None

    df = df.reset_index()
    df.insert(1, "Ticker", ticker)
    df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

    return df


# ================= MAIN =================

def main():
    print("[INFO] Fetching tickers from Finviz...")
    try:
        tickers = get_finviz_tickers(FINVIZ_URL)
    except Exception:
        print("[ERROR] Failed Finviz scrape. Exiting.")
        raise

    if not tickers:
        raise RuntimeError("No tickers returned from Finviz screener.")

    print(f"[INFO] Found {len(tickers)} tickers.")

    frames = []
    for t in tickers:
        print(f"[INFO] {t}: downloading data...")
        df = fetch_ohlcv(t, LOOKBACK_DAYS)
        if df is None:
            print(f"[WARN] {t}: no usable data.")
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("No OHLCV data could be downloaded for any ticker.")

    final = pd.concat(frames, ignore_index=True)

    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"{today_str}.csv"

    final.to_csv(out_path, index=False)

    print(f"[INFO] Saved OHLCV dataset → {out_path}")
    print("[INFO] Complete.")


if __name__ == "__main__":
    main()