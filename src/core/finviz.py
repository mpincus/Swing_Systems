"""
Finviz scraping helpers: fetch tickers from configured screen URLs and save them as
per-strategy watchlists.
"""
import time
from typing import Iterable, List, Sequence

import requests
from bs4 import BeautifulSoup


def fetch_watchlist(
    urls: Sequence[str],
    throttle_seconds: float = 1.0,
    user_agent: str = "Mozilla/5.0",
    max_pages: int = 10,
) -> List[str]:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    seen: List[str] = []
    for base_url in urls:
        start = 1
        pages = 0
        while True:
            page_url = _with_page(base_url, start)
            resp = session.get(page_url, timeout=30)
            resp.raise_for_status()
            tickers = _extract_tickers(resp.text)
            if not tickers:
                break
            for t in tickers:
                if t not in seen:
                    seen.append(t)
            pages += 1
            if pages >= max_pages:
                break
            start += 20
            time.sleep(throttle_seconds)
    return seen


def _with_page(url: str, start: int) -> str:
    if "r=" in url:
        return url
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}r={start}"


def _extract_tickers(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    tickers = []
    for link in soup.select("a.screener-link-primary"):
        text = link.text.strip().upper()
        if text and text.isalpha():
            tickers.append(text)
    return tickers


def save_watchlist(tickers: Iterable[str], path: str) -> None:
    import csv
    import pathlib

    path_obj = pathlib.Path(path)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with path_obj.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["Ticker"])
        for t in tickers:
            writer.writerow([t])
