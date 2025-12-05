from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ========= CONFIG =========

# Your MA long Finviz screener (price > 30, avg vol > 750k, 50 & 200 MA rising)
FINVIZ_URL = (
    "https://finviz.com/screener.ashx"
    "?v=111&f=sh_avgvol_o750,sh_price_o30,ta_sma200_pb,ta_sma50_pb"
)

LOOKBACK_DAYS = 180   # ~6 months calendar
REQUEST_DELAY = 1.0   # seconds between Finviz pages

DATA_DIR = Path("data")
OUTPUT_CSV = DATA_DIR / "daily_ohlcv.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ========= FINVIZ SCRAPING (ROBUST, RSI-STYLE) =========

def extract_tickers(html: str) -> List[str]:
    """
    Extracts all symbols from quote links like: quote.ashx?t=NVDA
    Does not depend on any particular table structure.
    """
    soup = BeautifulSoup(html, "html.parser")
    tickers: List[str] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "quote.ashx?t=" not in href:
            continue
        t = (a.text or "").strip().upper()
        if not t:
            continue
        # Basic ticker pattern: letters + optional dot, up to 5 chars
        if not re.fullmatch(r"[A-Z.]{1,5}", t):
            continue
        if t in seen:
            continue
        seen.add(t)
        tickers.append(t)

    return tickers


def paged_url(base_url: str, page_index: int) -> str:
    """
    Finviz uses &r=1,21,41,... for pagination.
    page_index is 0-based.
    """
    start = page_index * 20 + 1
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}r={start}"


def fetch_finviz_tickers(max_pages: int = 75) -> List[str]:
    """
    Scrape all tickers from your MA Finviz screener.
    Uses the robust quote-link method. Raises if nothing is found.
    """
    tickers: List[str] = []
    page = 0

    while page < max_pages:
        url = paged_url(FINVIZ_URL, page)
        print(f"[FINVIZ] Page {page + 1}: {url}")
        resp = SESSION.get(url, timeout=25)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Finviz HTTP error {resp.status_code} on page {page + 1}. "
                "Check the screener URL or try again later."
            )

        page_tickers = extract_tickers(resp.text)
        print(f"[FINVIZ] Page {page + 1}: found {len(page_tickers)} tickers")

        if page > 0 and not page_tickers:
            # No more pages
            break

        # Append and de-dupe in order
        for t in page_tickers:
            if t not in tickers:
                tickers.append(t)

        page += 1
        time.sleep(REQUEST_DELAY)

        # If less than 20 tickers on this page, it's the last page
        if len(page_tickers) < 20:
            break

    if not tickers:
        raise RuntimeError(
            "Finviz screener produced zero tickers. "
            "Either the filters returned nothing, HTML changed, or access is blocked."
        )

    print(f"[FINVIZ] Total MA universe tickers: {len(tickers)}")
    return tickers


# ========= OHLCV DOWNLOAD =========

def download_ohlcv(tickers: List[str], lookback_days: int) -> pd.DataFrame:
    """
    Download daily OHLCV for each ticker via yfinance.
    Returns a long DataFrame with columns:
    Date, Ticker, Open, High, Low, Close, Volume
    """
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=lookback_days)

    frames = []

    for idx, ticker in enumerate(tickers, start=1):
        print(f"[YF] ({idx}/{len(tickers)}) {ticker}...")
        try:
            df = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=False,
                progress=False,
            )
        except Exception as e:
            print(f"[YF WARN] {ticker}: {type(e).__name__}")
            continue

        if df is None or df.empty:
            print(f"[YF WARN] {ticker}: empty dataframe")
            continue

        # Flatten multi-index columns if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        if "Close" not in df.columns:
            print(f"[YF WARN] {ticker}: missing 'Close' column")
            continue

        df = df.reset_index()
        # Ensure date column is named Date
        if df.columns[0].lower() not in ("date", "index"):
            print(f"[YF WARN] {ticker}: unrecognized date column, skipping")
            continue

        df.rename(columns={df.columns[0]: "Date"}, inplace=True)
        df.insert(1, "Ticker", ticker)
        df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

        frames.append(df)
        time.sleep(0.1)

    if not frames:
        raise RuntimeError(
            "yfinance returned no usable data for any ticker. "
            "Check network / yfinance availability and try again."
        )

    all_data = pd.concat(frames, ignore_index=True)
    return all_data


# ========= MAIN =========

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[INFO] Scraping MA universe from Finviz...")
    print(f"[INFO] Screener URL: {FINVIZ_URL}")
    tickers = fetch_finviz_tickers()

    print("[INFO] Downloading OHLCV via yfinance...")
    df = download_ohlcv(tickers, LOOKBACK_DAYS)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[INFO] Saved {len(df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()