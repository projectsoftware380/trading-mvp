import pandas as pd

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
