"""
RSI Reversal Short strategy: RSI >= 70 with bearish engulfing; stop at prior 3-day high,
target set to 1.25R or better.
"""
import datetime as dt
import pandas as pd

from core import indicators
from core.strategy_base import Strategy


class RSIReversalShort(Strategy):
    name = "rsi_reversal_short"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=sh_opt_option,sh_price_o10,sh_avgvol_o1500,ta_rsi_overbought,ta_pattern_bearishengulfing"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = indicators.add_common_indicators(prices)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)

        mask = (
            (df["Date"] >= cutoff)
            & (df["RSI"] >= 70)
            & (df["BearishEngulfing"])
            & df["H3"].notna()
        )

        candidates = df.loc[mask].copy()
        candidates["Stop"] = candidates["H3"]
        candidates["Target"] = candidates["Close"] - 1.25 * (candidates["Stop"] - candidates["Close"])
        candidates["R"] = (candidates["Close"] - candidates["Target"]) / (candidates["Stop"] - candidates["Close"])
        candidates = candidates[candidates["R"] >= 1.25]

        if candidates.empty:
            return pd.DataFrame()

        candidates["Strategy"] = self.name
        candidates["Setup"] = "Reversal Short"
        candidates["Side"] = "short"
        candidates["EntryTrigger"] = "< signal candle close"
        candidates["Grade"] = candidates["R"].apply(self.grade_from_r)
        candidates["GradeBasis"] = "rr_fallback"
        candidates["Reason"] = "RSI>=70 + bearish engulfing; stop=prior 3d high"

        cols = [
            "Date",
            "Ticker",
            "Strategy",
            "Setup",
            "Side",
            "EntryTrigger",
            "Stop",
            "Target",
            "R",
            "Grade",
            "GradeBasis",
            "Reason",
        ]
        return candidates[cols].reset_index(drop=True)


strategy = RSIReversalShort()
