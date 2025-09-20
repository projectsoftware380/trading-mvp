import pandas as pd
from typing import List

REQUIRED = {'time','open','high','low','close','volume'}

def load_parquet(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # columnas a minúscula para evitar errores
    df.columns = [c.lower() for c in df.columns]
    if not REQUIRED.issubset(set(df.columns)):
        raise ValueError(f"El parquet debe contener columnas: {REQUIRED}. Encontrado: {df.columns.tolist()}")
    # ordenar por tiempo si existe
    if 'time' in df.columns:
        df = df.sort_values('time').reset_index(drop=True)
    return df

def check_nans(df: pd.DataFrame, columns: List[str], threshold: float = 0.5) -> None:
    """
    Checks if the percentage of NaNs in specified columns exceeds a threshold.

    Args:
        df: DataFrame to check.
        columns: List of column names to check for NaNs.
        threshold: Maximum allowed percentage of NaNs (e.g., 0.5 for 50%).

    Raises:
        ValueError: If the percentage of NaNs in any column exceeds the threshold.
    """
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in DataFrame.")
        nan_percentage = df[col].isnull().sum() / len(df)
        if nan_percentage > threshold:
            raise ValueError(
                f"Column '{col}' has {nan_percentage:.2%} NaNs, which exceeds the {threshold:.0%} threshold."
            )
