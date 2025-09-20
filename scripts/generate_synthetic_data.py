import pandas as pd
import numpy as np
from pathlib import Path

def generate_synthetic_data(file_path: Path, num_rows: int = 100, bad_atr_count: int = 0):
    time = pd.to_datetime(pd.date_range('2023-01-01', periods=num_rows, freq='min'))
    open_ = np.random.rand(num_rows) * 100
    high = open_ + np.random.rand(num_rows) * 5
    low = open_ - np.random.rand(num_rows) * 5
    close = open_ + (np.random.rand(num_rows) - 0.5) * 2 # price

    df = pd.DataFrame({
        'time': time,
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': np.random.randint(100, 1000, num_rows)
    })

    df.set_index('time', inplace=True)

    # Add a dummy ATR column for now, it will be calculated later by add_atr
    df['atr'] = np.random.rand(num_rows) * 10

    # Introduce bad ATR values if requested
    if bad_atr_count > 0:
        # Ensure bad ATRs are within the evaluated range [0, n-horizon)
        # For simplicity, let's place them at the beginning
        indices_to_corrupt = np.random.choice(range(num_rows - 10), size=min(bad_atr_count, num_rows - 10), replace=False)
        df.iloc[indices_to_corrupt, df.columns.get_loc('atr')] = -1 # Example of a bad ATR value

    file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(file_path)
    print(f"Synthetic data saved to {file_path}")

if __name__ == '__main__':
    output_dir = Path('C:/TradingInfraestructura/mvp_step1_cdrive/data/raw')

    # Generate good synthetic data
    good_output_file = output_dir / 'synthetic_good_data.parquet'
    generate_synthetic_data(good_output_file)

    # Generate bad synthetic data
    bad_output_file = output_dir / 'synthetic_bad_data.parquet'
    generate_synthetic_data(bad_output_file, bad_atr_count=10)
