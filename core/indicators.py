import numpy as np
import pandas as pd

def atr_wilder(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df['high'].to_numpy(), df['low'].to_numpy(), df['close'].to_numpy()
    prev_close = np.r_[np.nan, close[:-1]]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return pd.Series(tr, index=df.index).ewm(alpha=1/period, adjust=False).mean()

def rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    delta = df['close'].diff()
    gain = (delta.clip(lower=0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-12)
    return 100 - (100 / (1 + rs))

def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()

def bbands(df: pd.DataFrame, period: int = 20, stds: float = 2.0):
    mid = sma(df['close'], period)
    std = df['close'].rolling(period, min_periods=period).std()
    upper = mid + stds * std
    lower = mid - stds * std
    return upper, mid, lower

def volume_relative(df: pd.DataFrame, period: int = 20) -> pd.Series:
    ma = sma(df['volume'], period)
    return df['volume'] / (ma + 1e-12)