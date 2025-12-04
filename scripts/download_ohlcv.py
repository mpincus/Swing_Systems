import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timedelta

FINVIZ_URL = "https://finviz.com/screener.ashx?v=111&f=exch_nyse,exch_nasdaq,exch_amex,sh_avgvol_o750,sh_price_o5"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "daily_ohlcv.csv")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_finviz_tickers(url, max_pages=50):
    tickers = []

    for page in range(1, max_pages + 1):
        page_url = url + f"&r={1 + (page - 1) * 20}"
        resp = requests.get(page_url, headers=HEADERS)

        if resp.status_code != 200:
            time.sleep(1)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="table-light")

        if not table:
            continue

        rows = table.find_all("tr")[1:]

        if not rows:
            continue

        for r in rows:
            cols = r.find_all("td")
            if len(cols) >= 2:
                tickers.append(cols[1].text.strip())

        if len(rows) < 20:
            break

        time.sleep(0.25)

    tickers = list(dict.fromkeys(tickers))

    if len(tickers) == 0:
        raise RuntimeError("Finviz returned ZERO tickers. Scraper blocked or URL wrong.")

    return tickers

def download_ohlcv(tickers, days=200):
    end = datetime.now()
    start = end - timedelta(days=days)

    frames = []

    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start, end=end, progress=False)
            if df.empty:
                continue

            df["Ticker"] = ticker
            df.reset_index(inplace=True)
            frames.append(df)

            time.sleep(0.05)
        except Exception:
            continue

    if not frames:
        raise RuntimeError("OHLCV download produced zero dataframes.")

    final = pd.concat(frames, ignore_index=True)
    return final

def main():
    print("[INFO] Fetching tickers from Finviz…")
    tickers = fetch_finviz_tickers(FINVIZ_URL)
    print(f"[INFO] Retrieved {len(tickers)} tickers.")

    print("[INFO] Downloading OHLCV… This may take a few minutes.")
    df = download_ohlcv(tickers)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[INFO] Saved OHLCV to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()