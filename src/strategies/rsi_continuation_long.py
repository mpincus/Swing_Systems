"""
RSI Continuation Long: momentum continuation when RSI > 70 and rising; stop at prior
3-day low, target at 1.25R or better.
"""
import datetime as dt
import pandas as pd

from core import indicators
from core.strategy_base import Strategy


class RSIContinuationLong(Strategy):
    name = "rsi_continuation_long"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=sh_opt_option,sh_price_o10,sh_avgvol_o2000,ta_highlow52w_nh"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = indicators.add_common_indicators(prices)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)

        prev_rsi = df.groupby("Ticker")["RSI"].shift(1)
        mask = (
            (df["Date"] >= cutoff)
            & (df["RSI"] > 70)
            & (prev_rsi.notna())
            & (df["RSI"] > prev_rsi)
            & df["L3"].notna()
        )

        candidates = df.loc[mask].copy()
        candidates["Stop"] = candidates["L3"]
        candidates["Target"] = candidates["Close"] + 1.25 * (candidates["Close"] - candidates["Stop"])
        candidates["R"] = (candidates["Target"] - candidates["Close"]) / (candidates["Close"] - candidates["Stop"])
        candidates = candidates[candidates["R"] >= 1.25]

        if candidates.empty:
            return pd.DataFrame()

        candidates["Strategy"] = self.name
        candidates["Setup"] = "RSI Continuation Long"
        candidates["Side"] = "long"
        candidates["EntryTrigger"] = "> signal candle close"
        candidates["Grade"] = candidates["R"].apply(self.grade_from_r)
        candidates["GradeBasis"] = "rr_fallback"
        candidates["Reason"] = "RSI>70 and rising; stop=prior 3d low"

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


strategy = RSIContinuationLong()
