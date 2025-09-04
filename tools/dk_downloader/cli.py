"""Command line interface for the Dukascopy downloader."""
from __future__ import annotations

import argparse
from typing import Optional

from .download import download


def parse_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Download data from Dukascopy")
    parser.add_argument("--symbol", required=True, help="Symbol, e.g. EURUSD")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--granularity",
        choices=["tick", "m1"],
        default="m1",
        help="Data granularity",
    )
    parser.add_argument("--aggregate-to", help="Resample frequency for tick data")
    parser.add_argument("--tz", default="UTC", help="Target timezone")
    parser.add_argument("--out", dest="out_parquet", help="Output parquet path")
    parser.add_argument(
        "--out-dir",
        default="data/raw",
        help="Directory for intermediate CSV downloads",
    )
    return parser.parse_args(args)


def main() -> None:
    """Entry point for the CLI."""
    ns = parse_args()
    path = download(
        symbol=ns.symbol,
        start=ns.start,
        end=ns.end,
        granularity=ns.granularity,
        out_dir=ns.out_dir,
        out_parquet=ns.out_parquet,
        aggregate_to=ns.aggregate_to,
        tz=ns.tz,
    )
    print("Saved:", path)


if __name__ == "__main__":  # pragma: no cover
    main()
