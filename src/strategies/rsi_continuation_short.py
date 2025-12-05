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
        candidates["Strategy"] = self.name
        candidates["Setup"] = "RSI Continuation Short"
        candidates["Side"] = "short"
        candidates["EntryTrigger"] = "< signal candle close"
        candidates["SignalOpen"] = candidates["Open"]
        candidates["SignalHigh"] = candidates["High"]
        candidates["SignalLow"] = candidates["Low"]
        candidates["SignalClose"] = candidates["Close"]
        candidates["Stop"] = ""
        candidates["Target"] = ""
        candidates["R"] = ""
        candidates["Grade"] = ""
        candidates["GradeBasis"] = "gpt_to_size"
        candidates["Reason"] = "RSI<30 and falling; GPT to set stop/target"

        cols = [
            "Date",
            "Ticker",
            "Strategy",
            "Setup",
            "Side",
            "EntryTrigger",
            "SignalOpen",
            "SignalHigh",
            "SignalLow",
            "SignalClose",
            "Stop",
            "Target",
            "R",
            "Grade",
            "GradeBasis",
            "Reason",
        ]
        return candidates[cols].reset_index(drop=True)


strategy = RSIContinuationShort()
