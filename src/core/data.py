"""
Download utilities: fetch OHLCV for the union of watchlist tickers and normalize
into a CSV used by all strategies. Tries Yahoo first (if enabled) and can fall
back to Stooq when Yahoo is rate-limited.
"""
import datetime as dt
import io
import pathlib
import time
from typing import Iterable, List, Optional

import pandas as pd
import requests
import yfinance as yf


PRICE_COLUMNS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]


def fetch_prices(
    tickers: Iterable[str],
    lookback_days: int,
    out_path: str,
    data_source: str = "auto",
) -> pd.DataFrame:
    tickers = [t.strip().upper() for t in tickers if t.strip()]
    if not tickers:
        raise ValueError("No tickers provided for price fetch.")

    start = dt.date.today() - dt.timedelta(days=lookback_days)
    end = dt.date.today() + dt.timedelta(days=1)
    existing = _load_existing(out_path, start)

    frames: List[pd.DataFrame] = []
    chunk_size = 10  # keep chunks small to reduce rate limits

    def maybe_yahoo() -> None:
        for i in range(0, len(tickers), chunk_size):
            chunk = tickers[i : i + chunk_size]
            df_part = _download_chunk_yahoo(chunk, start, end)
            if df_part is not None and not df_part.empty:
                frames.append(df_part)
            time.sleep(2.0)  # throttle between chunks

    def stooq_all() -> None:
        for sym in tickers:
            df_part = _download_stooq(sym, start, end)
            if df_part is not None and not df_part.empty:
                frames.append(df_part)
            time.sleep(0.5)

    if data_source == "yahoo":
        maybe_yahoo()
    elif data_source == "stooq":
        stooq_all()
    else:  # auto
        maybe_yahoo()
        if not frames:
            print("[INFO] Yahoo yielded no data; falling back to Stooq.")
            stooq_all()

    if not frames and existing is not None and not existing.empty:
        df = existing
    elif not frames:
        raise ValueError("No price data downloaded; all sources failed or empty.")
    else:
        df = pd.concat(frames, ignore_index=True)
        df = _normalize(df)

    if existing is not None and not existing.empty and not df.empty:
        df = pd.concat([existing, df], ignore_index=True)
        df = df.drop_duplicates(subset=["Ticker", "Date"], keep="last")
        df = df[df["Date"] >= start]

    path_obj = pathlib.Path(out_path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_obj, index=False)
    return df


def _to_long(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw

    if isinstance(raw.columns, pd.MultiIndex):
        frames: List[pd.DataFrame] = []
        for ticker in raw.columns.levels[1]:
            sub = raw.xs(ticker, axis=1, level=1, drop_level=False)
            sub = sub.droplevel(1, axis=1)
            sub = sub.reset_index()
            sub["Ticker"] = ticker
            frames.append(sub)
        return pd.concat(frames, ignore_index=True)

    df = raw.reset_index()
    df["Ticker"] = df.columns.name or ""
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"Datetime": "Date", "Adj Close": "Adj Close"})
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    if "Adj Close" not in df.columns and "Close" in df.columns:
        df["Adj Close"] = df["Close"]

    ordered_cols = [c for c in PRICE_COLUMNS if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + remaining]
    df = df.sort_values(["Ticker", "Date"])
    return df


def _download_chunk_yahoo(chunk: List[str], start: dt.date, end: dt.date) -> Optional[pd.DataFrame]:
    attempts = 2
    for attempt in range(attempts):
        try:
            if len(chunk) == 1:
                raw = yf.download(
                    tickers=chunk[0],
                    start=start,
                    end=end,
                    progress=False,
                    auto_adjust=False,
                )
                df_part = raw.reset_index()
                df_part["Ticker"] = chunk[0]
            else:
                raw = yf.download(
                    tickers=" ".join(chunk),
                    start=start,
                    end=end,
                    group_by="ticker",
                    progress=False,
                    auto_adjust=False,
                    threads=True,
                )
                df_part = _to_long(raw)
            if df_part is not None and not df_part.empty:
                return df_part
        except Exception as exc:
            print(f"[WARN] Yahoo chunk {chunk[0]}..{chunk[-1]} failed (attempt {attempt+1}/{attempts}): {exc}")
            time.sleep(3.0)
    return None


def _download_stooq(ticker: str, start: dt.date, end: dt.date) -> Optional[pd.DataFrame]:
    # Stooq API: https://stooq.pl/q/d/l/?s=aapl.us&i=d
    url = f"https://stooq.pl/q/d/l/?s={ticker.lower()}.us&i=d"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        if not resp.text:
            return None
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            return None
        df["Ticker"] = ticker.upper()
        df = df.rename(
            columns={
                "Data": "Date",
                "Otwarcie": "Open",
                "Najwyzszy": "High",
                "Najnizszy": "Low",
                "Zamkniecie": "Close",
                "Wolumen": "Volume",
            }
        )
        if "Date" not in df.columns:
            return None
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df = df[(df["Date"] >= start) & (df["Date"] <= end)]
        if df.empty:
            return None
        if "Adj Close" not in df.columns and "Close" in df.columns:
            df["Adj Close"] = df["Close"]
        return df
    except Exception as exc:
        print(f"[WARN] Stooq download failed for {ticker}: {exc}")
        return None


def _load_existing(path: str, min_date: dt.date) -> Optional[pd.DataFrame]:
    path_obj = pathlib.Path(path)
    if not path_obj.exists():
        return None
    try:
        df = pd.read_csv(path_obj, parse_dates=["Date"])
        df["Date"] = df["Date"].dt.date
        df = df[df["Date"] >= min_date]
        if df.empty:
            return None
        return df
    except Exception as exc:
        print(f"[WARN] Could not load existing prices from {path_obj}: {exc}")
        return None
