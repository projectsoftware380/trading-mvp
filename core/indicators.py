import pandas as pd

def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Calculates and adds the Average True Range (ATR) to the DataFrame.

    Args:
        df: DataFrame with 'high', 'low', 'close' columns.
        period: The period for the ATR calculation.

    Returns:
        DataFrame with 'atr' column added.
    """
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        raise ValueError("DataFrame must contain 'high', 'low', and 'close' columns.")

    high_low = df['high'] - df['low']
    high_prev_close = (df['high'] - df['close'].shift(1)).abs()
    low_prev_close = (df['low'] - df['close'].shift(1)).abs()

    tr = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
    
    # Calculate ATR using an exponential moving average
    df['atr'] = tr.ewm(alpha=1/period, adjust=False).mean()
    
    return df
