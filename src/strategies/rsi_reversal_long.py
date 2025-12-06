"""
RSI Reversal Long: look for RSI <= 30 with a bullish engulfing bar, then require a trigger
within the next 3 bars. Stops/targets left for GPT; signal bar OHLC included.
"""
import datetime as dt
import pandas as pd

from core import indicators
from core.strategy_base import Strategy


class RSIReversalLong(Strategy):
    name = "rsi_reversal_long"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=sh_opt_option,sh_price_o10,sh_avgvol_o1500,ta_rsioversold,ta_pattern_bullishengulfing"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = indicators.add_common_indicators(prices)
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)

        rows = []
        for ticker, group in df.groupby("Ticker"):
            group = group.sort_values("Date").reset_index(drop=True)
            if len(group) < 15:
                continue
            for i in range(1, len(group)):
                curr = group.loc[i]
                if curr["Date"] < cutoff:
                    continue
                if not (curr["RSI"] <= 30 and curr["BullishEngulfing"]):
                    continue
                signal_close = curr["Close"]
                trigger_date = None
                for j in range(i, min(i + 4, len(group))):
                    bar = group.loc[j]
                    if bar["High"] > signal_close:
                        trigger_date = bar["Date"]
                        break
                if trigger_date is None:
                    continue
                rows.append(
                    {
                        "Date": trigger_date,
                        "SignalDate": curr["Date"],
                        "Ticker": ticker,
                        "Strategy": self.name,
                        "Setup": "Reversal Long",
                        "Side": "long",
                        "EntryTrigger": "> signal close",
                        "EntryTriggerPrice": signal_close,
                        "SignalOpen": curr["Open"],
                        "SignalHigh": curr["High"],
                        "SignalLow": curr["Low"],
                        "SignalClose": curr["Close"],
                        "Stop": "",
                        "Target": "",
                        "R": "",
                        "Grade": "",
                        "GradeBasis": "gpt_to_size",
                        "Reason": "RSI<=30 + bullish engulfing; GPT to set stop/target",
                    }
                )

        if not rows:
            return pd.DataFrame()

        cols = [
            "Date",
            "SignalDate",
            "Ticker",
            "Strategy",
            "Setup",
            "Side",
            "EntryTrigger",
            "EntryTriggerPrice",
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
        return pd.DataFrame(rows)[cols].reset_index(drop=True)


strategy = RSIReversalLong()
