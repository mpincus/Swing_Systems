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
        candidates["Strategy"] = self.name
        candidates["Setup"] = "RSI Continuation Long"
        candidates["Side"] = "long"
        candidates["EntryTrigger"] = "> signal candle close"
        candidates["Stop"] = ""
        candidates["Target"] = ""
        candidates["R"] = ""
        candidates["Grade"] = ""
        candidates["GradeBasis"] = "gpt_to_size"
        candidates["Reason"] = "RSI>70 and rising; GPT to set stop/target"

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
