"""Synchronize data from Dukascopy using the downloader module."""
from __future__ import annotations

import argparse

from tools.dk_downloader.download import download


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync data from Dukascopy")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--granularity", choices=["tick", "m1"], default="m1")
    parser.add_argument("--aggregate-to")
    parser.add_argument("--tz", default="UTC")
    parser.add_argument("--out")
    parser.add_argument("--out-dir", default="data/raw")
    args = parser.parse_args()

    print("Starting download...")
    path = download(
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        granularity=args.granularity,
        out_dir=args.out_dir,
        out_parquet=args.out,
        aggregate_to=args.aggregate_to,
        tz=args.tz,
    )
    print("Saved:", path)


if __name__ == "__main__":
    main()
