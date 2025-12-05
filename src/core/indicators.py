"""
Indicator utilities shared by strategies: RSI, engulfing detection, and 3-day
high/low calculations.
"""
import pandas as pd


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = -delta.clip(upper=0).rolling(window=period, min_periods=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def add_common_indicators(df: pd.DataFrame, rsi_period: int = 14) -> pd.DataFrame:
    out = df.copy()
    out = out.sort_values(["Ticker", "Date"])
    out["RSI"] = out.groupby("Ticker")["Close"].transform(lambda s: compute_rsi(s, rsi_period))

    out["BodyHigh"] = out[["Open", "Close"]].max(axis=1)
    out["BodyLow"] = out[["Open", "Close"]].min(axis=1)
    out["PrevBodyHigh"] = out.groupby("Ticker")["BodyHigh"].shift(1)
    out["PrevBodyLow"] = out.groupby("Ticker")["BodyLow"].shift(1)
    out["PrevClose"] = out.groupby("Ticker")["Close"].shift(1)
    out["PrevOpen"] = out.groupby("Ticker")["Open"].shift(1)

    out["BullishEngulfing"] = (
        (out["Close"] > out["Open"])
        & (out["PrevClose"] < out["PrevOpen"])
        & (out["BodyHigh"] >= out["PrevBodyHigh"])
        & (out["BodyLow"] <= out["PrevBodyLow"])
    )
    out["BearishEngulfing"] = (
        (out["Close"] < out["Open"])
        & (out["PrevClose"] > out["PrevOpen"])
        & (out["BodyHigh"] >= out["PrevBodyHigh"])
        & (out["BodyLow"] <= out["PrevBodyLow"])
    )

    out["L3"] = out.groupby("Ticker")["Low"].shift(1).rolling(window=3).min()
    out["H3"] = out.groupby("Ticker")["High"].shift(1).rolling(window=3).max()
    return out
