import argparse
import json
import pandas as pd
from core.data_loader import load_parquet
from core.indicators import add_atr
from core.labeling import label_up_down_atr, generate_labeling_report

def main():
    parser = argparse.ArgumentParser(description='Generate labeling report for financial data.')
    parser.add_argument('--input-path', type=str, required=True, help='Path to the input parquet file.')
    parser.add_argument('--output-path', type=str, required=True, help='Path to save the JSON report.')
    parser.add_argument('--horizon', type=int, default=10, help='Horizon for labeling.')
    parser.add_argument('--atr-period', type=int, default=14, help='Period for ATR calculation.')
    args = parser.parse_args()

    # Load data
    df = load_parquet(args.input_path)

    # Add indicators
    df = add_atr(df, period=args.atr_period)

    # Apply labeling
    df = label_up_down_atr(df, horizon=args.horizon)

    # Generate report
    report = generate_labeling_report(df, horizon=args.horizon)

    # Save report
    with open(args.output_path, 'w') as f:
        json.dump(report, f, indent=4)

    print(f"Labeling report generated successfully at {args.output_path}")

if __name__ == '__main__':
    main()
