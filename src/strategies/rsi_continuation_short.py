"""
RSI Continuation Short: downside momentum when RSI < 30 and falling; stop at prior
3-day high, target at 1.25R or better.
"""
import datetime as dt
import pandas as pd

from core import indicators
from core.strategy_base import Strategy


class RSIContinuationShort(Strategy):
    name = "rsi_continuation_short"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=sh_opt_option,sh_price_o10,sh_avgvol_o2000,ta_highlow52w_nl"
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
            & (df["RSI"] < 30)
            & (prev_rsi.notna())
            & (df["RSI"] < prev_rsi)
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
        candidates["Setup"] = "RSI Continuation Short"
        candidates["Side"] = "short"
        candidates["EntryTrigger"] = "< signal candle close"
        candidates["Grade"] = candidates["R"].apply(self.grade_from_r)
        candidates["GradeBasis"] = "rr_fallback"
        candidates["Reason"] = "RSI<30 and falling; stop=prior 3d high"

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


strategy = RSIContinuationShort()
