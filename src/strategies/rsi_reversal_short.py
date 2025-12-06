"""
RSI Reversal Short: look for RSI >= 70 with a bearish engulfing bar, then require a trigger
within the next 3 bars. Stops/targets left for GPT; signal bar OHLC included.
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

        # Mark the most recent RSI cross below 70 after being >= 70
        rows = []
        for ticker, group in df.groupby("Ticker"):
            group = group.sort_values("Date").reset_index(drop=True)
            if len(group) < 15:
                continue
            for i in range(1, len(group)):
                curr = group.loc[i]
                if curr["Date"] < cutoff:
                    continue
                if not (curr["RSI"] >= 70 and curr["BearishEngulfing"]):
                    continue
                signal_close = curr["Close"]
                trigger_date = None
                for j in range(i, min(i + 4, len(group))):
                    bar = group.loc[j]
                    if bar["Low"] < signal_close:
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
                        "Setup": "Reversal Short",
                        "Side": "short",
                        "EntryTrigger": "< signal close",
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
                        "Reason": "RSI>=70 + bearish engulfing; GPT to set stop/target",
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


strategy = RSIReversalShort()
