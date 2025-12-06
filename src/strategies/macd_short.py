"""
MACD Short: bearish MACD line cross below signal in a downtrend.
Filters:
- Price > 30
- Trend: Close < SMA50
- Trigger: MACD (12-26) crosses below signal (9) today; yesterday MACD >= signal.
Stops/targets left for GPT; signal bar OHLC included.
"""
import datetime as dt
import pandas as pd

from core.strategy_base import Strategy


class MACDShort(Strategy):
    name = "macd_short"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=cap_midover,geo_usa,sh_opt_option,sh_price_o30,sh_avgvol_o1000,ta_sma200_pb"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = prices.copy()
        df = df.sort_values(["Ticker", "Date"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        df["EMA12"] = df.groupby("Ticker")["Close"].transform(lambda s: s.ewm(span=12, adjust=False).mean())
        df["EMA26"] = df.groupby("Ticker")["Close"].transform(lambda s: s.ewm(span=26, adjust=False).mean())
        df["MACD"] = df["EMA12"] - df["EMA26"]
        df["Signal"] = df.groupby("Ticker")["MACD"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
        df["MACD_prev"] = df.groupby("Ticker")["MACD"].shift(1)
        df["Signal_prev"] = df.groupby("Ticker")["Signal"].shift(1)
        df["SMA50"] = df.groupby("Ticker")["Close"].transform(lambda s: s.rolling(50, min_periods=50).mean())

        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)

        mask = (
            (df["Date"] >= cutoff)
            & (df["SMA50"].notna())
            & (df["Close"] < df["SMA50"])
            & (df["MACD"] < df["Signal"])
            & (df["MACD_prev"] >= df["Signal_prev"])
            & (df["Close"] > 30)
        )

        candidates = df.loc[mask].copy()
        if candidates.empty:
            return pd.DataFrame()

        candidates["Strategy"] = self.name
        candidates["Setup"] = "MACD Short"
        candidates["Side"] = "short"
        candidates["EntryTrigger"] = "< signal candle low"
        candidates["SignalOpen"] = candidates["Open"]
        candidates["SignalHigh"] = candidates["High"]
        candidates["SignalLow"] = candidates["Low"]
        candidates["SignalClose"] = candidates["Close"]
        candidates["Stop"] = ""
        candidates["Target"] = ""
        candidates["R"] = ""
        candidates["Grade"] = ""
        candidates["GradeBasis"] = "gpt_to_size"
        candidates["Reason"] = "MACD cross below signal in downtrend; GPT to set stop/target"

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


strategy = MACDShort()
