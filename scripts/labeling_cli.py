import argparse
import json
import pandas as pd
import sys
import datetime
import warnings
from pathlib import Path
from core.data_loader import load_parquet, check_nans
from core.indicators import add_atr
from core.labeling import generate_labeling_report, validate_targets
from core.atr_wr import label_up_down_atr_wrapper

def main():
    parser = argparse.ArgumentParser(description='Generate labeling report for financial data.')
    parser.add_argument('--input-path', type=str, required=True, help='Path to the input parquet file.')
    parser.add_argument('--output-path', type=str, required=False, help='Path to save the labeled parquet file. If not provided, derives from input_path.')
    parser.add_argument('--report-output-path', type=str, default="report.json", help='Path to save the JSON report.')
    parser.add_argument('--horizon', type=int, default=10, help='Horizon for labeling.')
    parser.add_argument('--atr-period', type=int, default=14, help='Period for ATR calculation.')
    parser.add_argument('--nan-threshold', type=float, default=0.5, help='Threshold for NaN percentage check.')
    parser.add_argument('--nan-check-columns', nargs='+', default=['open', 'high', 'low', 'close', 'volume'], help='Columns to check for NaNs.')
    parser.add_argument('--strict', action='store_true', help='If set, raise error for non-positive/NaN ATR values.')
    args = parser.parse_args()

    input_path = Path(args.input_path)
    report_output_path = Path(args.report_output_path)

    if args.output_path is None:
        labeled_output_path = input_path.parent / f"{input_path.stem}_lab.parquet"
    else:
        labeled_output_path = Path(args.output_path)

    # Load data
    df = load_parquet(str(input_path))

    # Check for NaNs
    check_nans(df, args.nan_check_columns, args.nan_threshold)

    # Add indicators
    df = add_atr(df, period=args.atr_period)

    # Apply labeling
    df, invalid_atr_count = label_up_down_atr_wrapper(df, horizon=args.horizon)

    # Validate targets
    validate_targets(df, args.horizon)

    # Generate report
    report_core = generate_labeling_report(df, horizon=args.horizon)

    # Add enhanced statistics
    up_atr_values = df['up_atr'].dropna()
    down_atr_values = df['down_atr'].dropna()

    report_enhanced = {
        "up_atr_share_gt_1": float((up_atr_values > 1).mean()),
        "up_atr_share_gt_2": float((up_atr_values > 2).mean()),
        "down_atr_share_gt_1": float((down_atr_values > 1).mean()),
        "down_atr_share_gt_2": float((down_atr_values > 2).mean()),
        "ATR_nonpositive_or_nan": invalid_atr_count,
    }

    report = dict(report_core)  # rows, horizon, etc.
    report.update(report_enhanced)

    # Handle strict mode for ATR
    if invalid_atr_count > 0:
        message = f"Found {invalid_atr_count} non-positive or NaN ATR values. Labels for these periods are NaN."
        if args.strict:
            import logging
            logging.error(message)
            sys.exit(1) # Exit with an error code
        else:
            warnings.warn(message)

    # Add metadata
    report.update({
        "input_path": str(input_path),
        "labeled_output_path": str(labeled_output_path),
        "report_output_path": str(report_output_path),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "python_version": sys.version,
    })

    # Save labeled data
    labeled_output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(labeled_output_path)
    print(f"Labeled data saved successfully at {labeled_output_path}")

    # Save report
    with open(report_output_path, 'w') as f:
        json.dump(report, f, indent=4)

    print(f"Labeling report generated successfully at {report_output_path}")

if __name__ == '__main__':
    main()
