"""
Yahoo Finance download utilities: fetch OHLCV for the union of watchlist tickers
and normalize into a CSV used by all strategies.
"""
import datetime as dt
import pathlib
from typing import Iterable, List

import pandas as pd
import yfinance as yf


PRICE_COLUMNS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj Close", "Volume"]


def fetch_prices(tickers: Iterable[str], lookback_days: int, out_path: str) -> pd.DataFrame:
    tickers = [t.strip().upper() for t in tickers if t.strip()]
    if not tickers:
        raise ValueError("No tickers provided for price fetch.")

    start = dt.date.today() - dt.timedelta(days=lookback_days)
    end = dt.date.today() + dt.timedelta(days=1)

    if len(tickers) == 1:
        raw = yf.download(
            tickers=tickers[0],
            start=start,
            end=end,
            progress=False,
            auto_adjust=False,
        )
        df = raw.reset_index()
        df["Ticker"] = tickers[0]
    else:
        raw = yf.download(
            tickers=" ".join(tickers),
            start=start,
            end=end,
            group_by="ticker",
            progress=False,
            auto_adjust=False,
            threads=True,
        )
        df = _to_long(raw)

    df = _normalize(df)

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

    ordered_cols = [c for c in PRICE_COLUMNS if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered_cols]
    df = df[ordered_cols + remaining]
    df = df.sort_values(["Ticker", "Date"])
    return df
