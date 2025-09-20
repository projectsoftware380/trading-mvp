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

    up_atr_values = np.full(len(df), np.nan)
    down_atr_values = np.full(len(df), np.nan)

    for t in range(len(df)):
        if not np.isnan(atr[t]) and atr[t] > 0:
            up_atr_values[t] = (max_fwd[t] - close[t]) / atr[t]
            down_atr_values[t] = (close[t] - min_fwd[t]) / atr[t]
        else:
            up_atr_values[t] = np.nan
            down_atr_values[t] = np.nan

    df['up_atr']   = up_atr_values
    df['down_atr'] = down_atr_values
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

def validate_targets(df: pd.DataFrame, horizon: int, up_col: str = 'up_atr', down_col: str = 'down_atr') -> None:
    """
    Validates the labeling columns by checking the number of NaNs at the tail.

    Args:
        df: DataFrame with labeling columns.
        horizon: The horizon value used for labeling.
        up_col: Name of the up_atr column.
        down_col: Name of the down_atr column.

    Raises:
        ValueError: If the number of NaNs in the tail of labeling columns is not approximately equal to horizon.
    """
    tail_nans_up = df[up_col].tail(horizon).isna().sum()
    tail_nans_dn = df[down_col].tail(horizon).isna().sum()

    if not (np.isclose(tail_nans_up, horizon) and np.isclose(tail_nans_dn, horizon)):
        raise ValueError(
            f"Validation failed: Expected approximately {horizon} NaNs at the tail of '{up_col}' and '{down_col}' "
            f"columns, but found {tail_nans_up} and {tail_nans_dn} respectively."
        )
    print("Target validation successful: NaNs in tail match horizon.")
