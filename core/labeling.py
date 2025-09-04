import numpy as np
import pandas as pd

def label_up_down_atr(df: pd.DataFrame, horizon: int = 10, atr_col: str = 'atr') -> pd.DataFrame:
    high, low, close = df['high'].to_numpy(), df['low'].to_numpy(), df['close'].to_numpy()
    atr = df[atr_col].to_numpy()

    max_fwd = np.full(len(df), np.nan)
    min_fwd = np.full(len(df), np.nan)
    for t in range(len(df)-horizon):
        max_fwd[t] = np.max(high[t+1:t+1+horizon])
        min_fwd[t] = np.min(low[t+1:t+1+horizon])

    df['up_atr']   = (max_fwd - close) / atr
    df['down_atr'] = (close - min_fwd) / atr
    return df
