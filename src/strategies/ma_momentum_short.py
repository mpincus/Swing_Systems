"""
MA Momentum Short: downside momentum/pullback-and-go candidates in downtrends.
Filters:
- Price > 30
- Trend: SMA50 trending down (Close < SMA50)
- Trigger: either (a) pullback near EMA21/SMA50, or
           (b) fresh EMA9/EMA21 bearish crossover, or
           (c) within 3% of a 20-day low
Stops/targets left for GPT; signal bar OHLC included.
"""
import datetime as dt
import pandas as pd

from core.strategy_base import Strategy


class MAMomentumShort(Strategy):
    name = "ma_momentum_short"
    urls = [
        "https://finviz.com/screener.ashx?v=111&f=cap_midover,geo_usa,sh_opt_option,sh_price_o30,sh_avgvol_o1000,ta_sma50_pb,ta_sma200_pb"
    ]

    def generate(self, prices: pd.DataFrame, history_days: int) -> pd.DataFrame:
        if prices.empty:
            return pd.DataFrame()

        df = prices.copy()
        df = df.sort_values(["Ticker", "Date"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        df["SMA50"] = df.groupby("Ticker")["Close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
        df["EMA9"] = df.groupby("Ticker")["Close"].transform(lambda s: s.ewm(span=9, adjust=False).mean())
        df["EMA21"] = df.groupby("Ticker")["Close"].transform(lambda s: s.ewm(span=21, adjust=False).mean())
        df["Low20"] = df.groupby("Ticker")["Low"].transform(lambda s: s.rolling(20, min_periods=5).min())

        today = df["Date"].max()
        cutoff = today - dt.timedelta(days=history_days - 1)

        pullback = (abs(df["Close"] - df["EMA21"]) / df["EMA21"] <= 0.05) | (
            abs(df["Close"] - df["SMA50"]) / df["SMA50"] <= 0.03
        )

        df["EMA9_prev"] = df.groupby("Ticker")["EMA9"].shift(1)
        df["EMA21_prev"] = df.groupby("Ticker")["EMA21"].shift(1)
        crossover = (df["EMA9"] < df["EMA21"]) & (df["EMA9_prev"] >= df["EMA21_prev"]) & (df["Close"] < df["SMA50"])
        near_low = (df["Low20"].notna()) & (df["Close"] <= 1.03 * df["Low20"])

        base_filters = (
            (df["Date"] >= cutoff)
            & (df["SMA50"].notna())
            & (df["Close"] < df["SMA50"])
            & (df["Close"] > 30)
        )

        mask = base_filters & (pullback | crossover | near_low)

        candidates = df.loc[mask].copy()
        if candidates.empty:
            return pd.DataFrame()

        candidates["Strategy"] = self.name
        candidates["Setup"] = "MA Momentum Short"
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
        candidates["Reason"] = "Downtrend (50<200), pullback/crossover/near-low trigger; GPT to set stop/target"

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


strategy = MAMomentumShort()
