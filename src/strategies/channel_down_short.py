"""
Channel Down Short: Finviz channel down pattern. Assumes pattern from screener; uses latest bar per ticker.
Stops/targets left for GPT; signal bar OHLC included.
"""
import datetime as dt
import pandas as pd

from core.strategy_base import Strategy


class ChannelDownShort(Strategy):
    name = "channel_down_short"
    TOP_N = 10  # keep only the top 10 by dollar volume to avoid noise
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=cap_midover,geo_usa,sh_opt_option,sh_price_o30,sh_avgvol_o3000,ta_sma50_pb,ta_pattern_channeldown"
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

        latest = df.sort_values(["Ticker", "Date"]).groupby("Ticker").tail(1).copy()
        if "Volume" in latest.columns:
            latest["DollarVolume"] = latest["Close"] * latest["Volume"]
            latest = latest.sort_values("DollarVolume", ascending=False).head(self.TOP_N)
            latest = latest.drop(columns=["DollarVolume"])

        latest["Strategy"] = self.name
        latest["Setup"] = "Channel Down Short"
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
        latest["Reason"] = "Finviz channel down pattern; filtered by top dollar volume; GPT to set stop/target"

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


strategy = ChannelDownShort()
