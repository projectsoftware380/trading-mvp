import numpy as np
import pandas as pd
from core.labeling import label_up_down_atr

def label_up_down_atr_wrapper(df: pd.DataFrame, horizon: int = 10, atr_col: str = 'atr'):
    df_labeled = label_up_down_atr(df.copy(), horizon, atr_col)

    # Calculate invalid_atr_count
    # Assuming invalid ATRs are those that are NaN or non-positive in the original atr_col
    invalid_atr_count = df[df[atr_col].isna() | (df[atr_col] <= 0)].shape[0]

    return df_labeled, invalid_atr_count
