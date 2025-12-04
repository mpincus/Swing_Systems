from pathlib import Path
from datetime import datetime
import csv

# Minimal, no external deps, no finance logic.
# It just writes a CSV with 3 rows.

REPO_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = REPO_ROOT / "output"
OUTPUT_CSV = OUTPUTS_DIR / "ma_trend_signals.csv"

def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    header = [
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

    today = datetime.utcnow().strftime("%Y-%m-%d")

    rows = [
        ["DEBUG1", "LONG",  "TEST", today, 100.0,  99.0,  95.0,  90.0,  80.0, True],
        ["DEBUG1", "SHORT", "TEST", today, 100.0, 101.0, 105.0, 110.0, 120.0, True],
        ["DEBUG2", "LONG",  "TEST", today,  50.0,  49.0,  48.0,  47.0,  46.0, False],
    ]

    with OUTPUT_CSV.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()