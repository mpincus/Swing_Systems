"""
Config loader: reads configs/settings.yaml, merges defaults, and ensures data/outputs/docs
directories exist for the pipeline run.
"""
import pathlib
from typing import Any, Dict

import yaml


DEFAULTS = {
    "timezone": "America/New_York",
    "lookback_days": 200,
    "signal_history_days": 10,
    "finviz": {"throttle_seconds": 1.2},
    "paths": {"data_dir": "data", "outputs_dir": "outputs", "docs_dir": "docs"},
    "strategies": {},
}


def load_settings(path: str = "configs/settings.yaml") -> Dict[str, Any]:
    cfg_path = pathlib.Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}

    settings = _merge(DEFAULTS, loaded)
    _ensure_directories(settings)
    return settings


def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in base.items():
        if key in override:
            if isinstance(value, dict) and isinstance(override[key], dict):
                result[key] = _merge(value, override[key])
            else:
                result[key] = override[key]
        else:
            result[key] = value

    for key, value in override.items():
        if key not in result:
            result[key] = value
    return result


def _ensure_directories(settings: Dict[str, Any]) -> None:
    paths = settings.get("paths", {})
    for key in ("data_dir", "outputs_dir", "docs_dir"):
        path = pathlib.Path(paths.get(key, ""))
        if path:
            path.mkdir(parents=True, exist_ok=True)
    (pathlib.Path(paths.get("data_dir", ".")) / "watchlists").mkdir(
        parents=True, exist_ok=True
    )
