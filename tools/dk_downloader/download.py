# tools/dk_downloader/download.py
"""Utilities to download and process Dukascopy data using dukascopy-node."""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from .config import VALID_GRANULARITY, resolve_symbol


def download(
    symbol: str,
    start: str,
    end: str,
    granularity: Literal["tick", "m1"] = "m1",
    out_dir: str = "data/raw",
    out_parquet: Optional[str] = None,
    aggregate_to: Optional[str] = None,
    tz: str = "UTC",
) -> str:
    """
    Download data from Dukascopy (via dukascopy-node) and store as Parquet.

    Parameters
    ----------
    symbol : str
        e.g., "EURUSD".
    start, end : str
        YYYY-MM-DD inclusive.
    granularity : {"tick", "m1"}
        Data granularity to download.
    out_dir : str
        Root dir for intermediate CSV files.
    out_parquet : Optional[str]
        Final Parquet path. If None, a default path is generated.
    aggregate_to : Optional[str]
        If ticks, resample frequency (e.g. "1min", "5min", "1H").
    tz : str
        Output timezone (default "UTC"). If different, will tz-convert.

    Returns
    -------
    str : path to saved Parquet file.
    """
    granularity = granularity.lower()
    if granularity not in VALID_GRANULARITY:
        raise ValueError(f"Invalid granularity: {granularity}")

    resolved_symbol = resolve_symbol(symbol).lower()

    # Correct timeframe argument for dukascopy-node
    timeframe = "m1" if granularity == "m1" else "tick"

    command = [
        "npx",
        "dukascopy-node",
        "-i",
        resolved_symbol,
        "-from",
        start,
        "-to",
        end,
        "-t",
        timeframe,
        "-f",
        "csv",
        "-v",
    ]

    # Run the command from the correct directory
    try:
        subprocess.run(command, check=True, cwd="tools/dk_downloader", shell=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error downloading data with dukascopy-node: {e.stderr}") from e

    # Find the downloaded file
    # The file is downloaded to tools/dk_downloader/download
    download_dir = Path("tools/dk_downloader/download")
    csv_files = list(download_dir.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files downloaded by dukascopy-node.")

    # Find the most recent csv file
    latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)

    try:
        df = pd.read_csv(latest_file)
    except FileNotFoundError as e:
        raise RuntimeError(f"Downloaded CSV file not found: {latest_file}") from e
    except pd.errors.EmptyDataError as e:
        raise RuntimeError(f"Downloaded CSV file is empty: {latest_file}") from e

    # Normalize columns dynamically
    df = df.rename(columns={"timestamp": "time"})
    if "volume" not in df.columns:
        df["volume"] = 0
    
    # Ensure columns are in the correct order
    df = df[["time", "open", "high", "low", "close", "volume"]]

    df["time"] = pd.to_datetime(df["time"])

    # Orden y TZ
    df = df.sort_values("time")
    if tz.upper() != "UTC":
        df["time"] = df["time"].dt.tz_convert(tz)

    # Salida Parquet
    if out_parquet is None:
        freq = aggregate_to if aggregate_to else granularity
        out_dir_parquet = Path("data/dukascopy")
        out_dir_parquet.mkdir(parents=True, exist_ok=True)
        fname = f"{resolved_symbol.upper()}_{freq}_{start}_{end}.parquet"
        out_parquet = str(out_dir_parquet / fname)

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet)
    return out_parquet
