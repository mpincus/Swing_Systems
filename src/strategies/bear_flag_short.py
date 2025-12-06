"""
Bear Flag Short: Finviz bear flag pattern. Assumes pattern from screener; uses latest bar per ticker.
Stops/targets left for GPT; signal bar OHLC included.
"""
import datetime as dt
import pandas as pd

from core.strategy_base import Strategy


class BearFlagShort(Strategy):
    name = "bear_flag_short"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=cap_midover,geo_usa,sh_opt_option,sh_price_o30,sh_avgvol_o1000,ta_pattern_bearflag"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = prices.copy()
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)
        df = df[df["Date"] >= cutoff]
        if df.empty:
            return pd.DataFrame()

        latest = df.sort_values(["Ticker", "Date"]).groupby("Ticker").tail(1)

        latest["Strategy"] = self.name
        latest["Setup"] = "Bear Flag Short"
        latest["Side"] = "short"
        latest["EntryTrigger"] = "< signal candle low"
        latest["SignalOpen"] = latest["Open"]
        latest["SignalHigh"] = latest["High"]
        latest["SignalLow"] = latest["Low"]
        latest["SignalClose"] = latest["Close"]
        latest["Stop"] = ""
        latest["Target"] = ""
        latest["R"] = ""
        latest["Grade"] = ""
        latest["GradeBasis"] = "gpt_to_size"
        latest["Reason"] = "Finviz bear flag pattern; GPT to set stop/target"

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
        return latest[cols].reset_index(drop=True)


strategy = BearFlagShort()
