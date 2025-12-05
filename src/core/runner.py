"""
Pipeline orchestrator: load config, scrape Finviz watchlists, download OHLCV,
run all discovered strategies, write per-strategy and combined signals, and
mirror outputs to docs/.
"""
import pathlib
import shutil
from importlib import import_module
from typing import Dict, List

import pandas as pd

from core import config, data, finviz
from core.strategy_base import Strategy


def main() -> None:
    settings = config.load_settings()
    strategies = _load_strategies(settings)

    watchlists = _build_watchlists(strategies, settings)
    union = sorted({t for tickers in watchlists.values() for t in tickers})
    prices = data.fetch_prices(
        tickers=union,
        lookback_days=int(settings.get("lookback_days", 200)),
        out_path=pathlib.Path(settings["paths"]["data_dir"]) / "prices.csv",
    )

    outputs_dir = pathlib.Path(settings["paths"]["outputs_dir"])
    outputs_dir.mkdir(parents=True, exist_ok=True)
    history_days = int(settings.get("signal_history_days", 10))

    all_frames: List[pd.DataFrame] = []
    for strat in strategies:
        tickers = watchlists.get(strat.name, [])
        if not tickers:
            continue
        subset = prices[prices["Ticker"].isin(tickers)].copy()
        signals = strat.generate(subset, history_days=history_days)
        if signals.empty:
            continue
        out_path = outputs_dir / strat.name / "signals.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        signals.to_csv(out_path, index=False)
        all_frames.append(signals)

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined_path = outputs_dir / "combined_signals.csv"
        combined.to_csv(combined_path, index=False)
        _mirror_to_docs(combined_path, settings)
        for strat in strategies:
            per_path = outputs_dir / strat.name / "signals.csv"
            if per_path.exists():
                _mirror_to_docs(per_path, settings, subdir=strat.name)


def _load_strategies(settings: Dict) -> List[Strategy]:
    discovered: List[Strategy] = []
    strategies_dir = pathlib.Path(__file__).resolve().parent.parent / "strategies"
    for path in strategies_dir.glob("*.py"):
        if path.name.startswith("__"):
            continue
        module_name = f"strategies.{path.stem}"
        module = import_module(module_name)
        strat = getattr(module, "strategy", None)
        if strat:
            discovered.append(strat)

    cfg_strats = settings.get("strategies", {})
    for strat in discovered:
        urls = cfg_strats.get(strat.name, {}).get("urls")
        if urls:
            strat.urls = urls
    return discovered


def _build_watchlists(strategies: List[Strategy], settings: Dict) -> Dict[str, List[str]]:
    data_dir = pathlib.Path(settings["paths"]["data_dir"]) / "watchlists"
    throttle = float(settings.get("finviz", {}).get("throttle_seconds", 1.0))
    ua = settings.get("finviz", {}).get("user_agent", "Mozilla/5.0")

    results: Dict[str, List[str]] = {}
    for strat in strategies:
        tickers = finviz.fetch_watchlist(
            strat.urls,
            throttle_seconds=throttle,
            user_agent=ua,
        )
        finviz.save_watchlist(tickers, data_dir / f"{strat.name}.csv")
        results[strat.name] = tickers
    return results


def _mirror_to_docs(path: pathlib.Path, settings: Dict, subdir: str | None = None) -> None:
    docs_dir = pathlib.Path(settings["paths"]["docs_dir"])
    target = docs_dir / subdir if subdir else docs_dir
    target.mkdir(parents=True, exist_ok=True)
    dest = target / path.name
    shutil.copyfile(path, dest)


if __name__ == "__main__":
    main()
