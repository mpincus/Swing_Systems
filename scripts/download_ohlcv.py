from __future__ import annotations

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

# ========= CONFIG =========

# Your exact Finviz screener URL (MA trend, price > 30, liquid)
FINVIZ_URL = (
    "https://finviz.com/screener.ashx"
    "?v=111&f=sh_avgvol_o750,sh_price_o30,ta_sma200_pb,ta_sma50_pb"
)

LOOKBACK_DAYS = 180  # ~6 months
DATA_DIR = Path("data")
OUTPUT_CSV = DATA_DIR / "daily_ohlcv.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


# ========= FINVIZ SCRAPER =========

def fetch_finviz_tickers(base_url: str, max_pages: int = 50) -> List[str]:
    """
    Scrape tickers from the given Finviz screener URL.

    Raises RuntimeError with a clear message if:
    - HTTP status is not 200
    - Result table is missing (likely blocked or HTML changed)
    - No tickers are returned at all
    """
    tickers: List[str] = []
    seen = set()

    page = 0
    while True:
        page += 1
        if page > max_pages:
            break

        start_row = 1 + (page - 1) * 20
        page_url = f"{base_url}&r={start_row}"

        resp = requests.get(page_url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Finviz HTTP error on page {page}: status {resp.status_code}. "
                "Check screener URL or try again later."
            )

        soup = BeautifulSoup(resp.text, "lxml")

        table = soup.find("table", class_="table-light")
        if table is None:
            # No results table at all: either screener empty or HTML changed / blocked
            if page == 1 and "No results found" in resp.text:
                raise RuntimeError(
                    "Finviz screener returned 'No results found'. "
                    "Your filters may be too strict."
                )
            raise RuntimeError(
                "Finviz results table not found. HTML may have changed or "
                "access may be blocked. Inspect the page manually in a browser."
            )

        rows = table.find_all("tr")
        if len(rows) <= 1:
            # Only header row
            if page == 1:
                raise RuntimeError(
                    "Finviz screener produced an empty results table. "
                    "Either the screener has no matches or HTML changed."
                )
            break

        # Skip header row
        data_rows = rows[1:]
        added_this_page = 0

        for row in data_rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            sym = (cols[1].get_text() or "").strip().upper()
            if not sym or " " in sym:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            tickers.append(sym)
            added_this_page += 1

        # If less than 20 rows of data, this was the last page
        if added_this_page < 20:
            break

        # Tiny pause to be less aggressive
        time.sleep(0.3)

    if not tickers:
        raise RuntimeError(
            "Finviz returned zero tickers for this screener. "
            "Check the URL, filters, or whether Finviz is blocking automated access."
        )

    tickers.sort()
    return tickers


# ========= OHLCV DOWNLOAD =========

def download_ohlcv(tickers: List[str], days: int) -> pd.DataFrame:
    """
    Download daily OHLCV for the given tickers over the given lookback window.

    Raises RuntimeError if every single ticker fails.
    """
    end = datetime.utcnow().date()
    start = end - timedelta(days=days)

    frames = []

    for i, ticker in enumerate(tickers, start=1):
        print(f"[INFO] ({i}/{len(tickers)}) Downloading {ticker}...")
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end + timedelta(days=1),
                interval="1d",
                auto_adjust=False,
                progress=False,
            )
        except Exception as e:
            print(f"[WARN] {ticker}: yfinance error {type(e).__name__}")
            continue

        if df is None or df.empty:
            print(f"[WARN] {ticker}: empty data from yfinance")
            continue

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        if "Close" not in df.columns:
            print(f"[WARN] {ticker}: no 'Close' column in data")
            continue

        df = df.reset_index()
        # Ensure Date column exists and is named consistently
        if "Date" not in df.columns:
            # Sometimes it's 'index' if the index name was lost
            if df.columns[0].lower() in ("date", "index"):
                df.rename(columns={df.columns[0]: "Date"}, inplace=True)
            else:
                print(f"[WARN] {ticker}: could not identify Date column")
                continue

        df.insert(1, "Ticker", ticker)
        df = df[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]]

        frames.append(df)

        # light throttle
        time.sleep(0.1)

    if not frames:
        raise RuntimeError(
            "OHLCV download produced zero usable dataframes. "
            "Check network/yfinance or try again later."
        )

    final = pd.concat(frames, ignore_index=True)
    return final


# ========= MAIN =========

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[INFO] Fetching tickers from Finviz screener...")
    print(f"[INFO] Screener URL: {FINVIZ_URL}")
    tickers = fetch_finviz_tickers(FINVIZ_URL)
    print(f"[INFO] Retrieved {len(tickers)} tickers from Finviz.")

    print("[INFO] Downloading OHLCV data from yfinance...")
    df = download_ohlcv(tickers, LOOKBACK_DAYS)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[INFO] Saved {len(df)} rows of OHLCV data to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()