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

def generate_labeling_report(df: pd.DataFrame, horizon: int) -> dict:
    """
    Generates a report with statistics about the labeling columns.

    Args:
        df: DataFrame with 'up_atr' and 'down_atr' columns.
        horizon: The horizon value used for labeling.

    Returns:
        A dictionary with labeling statistics.
    """
    report = {}
    report['total_rows'] = len(df)
    report['horizon'] = horizon
    report['missing_values'] = int(df['up_atr'].isnull().sum())
    
    if report['missing_values'] < len(df):
        report['up_atr_stats'] = df['up_atr'].describe().to_dict()
        report['down_atr_stats'] = df['down_atr'].describe().to_dict()
    else:
        report['up_atr_stats'] = {}
        report['down_atr_stats'] = {}
        
    return report
