"""Utilities to download and process Dukascopy data."""
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from .config import VALID_GRANULARITY, resolve_symbol


def _duka_download(args_list: list[str]) -> None:
    """Wrapper to invoke duka via API or CLI."""
    try:  # pragma: no cover
        from duka.app.app import run as duka_run  # type: ignore
        duka_run(args_list)
        return
    except Exception:
        pass

    import shutil
    import subprocess
    import sys

    if shutil.which("duka"):
        cmd = ["duka", "download"] + args_list
    else:
        cmd = [sys.executable, "-m", "duka.cli", "download"] + args_list

    subprocess.run(cmd, check=True)

def _normalize_tick_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns for tick data."""
    rename_map = {"timestamp": "time"}
    df = df.rename(columns=rename_map)
    if "time" not in df.columns:
        raise ValueError("Tick data missing 'time' column")
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df


def _normalize_m1_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns for m1 data."""
    if "time" not in df.columns:
        raise ValueError("m1 data missing 'time' column")
    df["time"] = pd.to_datetime(df["time"], utc=True)
    expected = {"open", "high", "low", "close", "volume"}
    missing = expected.difference(df.columns)
    if missing:
        raise ValueError(f"m1 data missing columns: {missing}")
    return df


def _aggregate_ticks(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Aggregate tick data to OHLCV."""
    price_col = "bid" if "bid" in df.columns else df.columns[1]
    df = df.set_index("time")
    ohlc = df[price_col].resample(freq).ohlc()
    if {"bid_volume", "ask_volume"}.issubset(df.columns):
        volume = (
            df["bid_volume"].resample(freq).sum()
            + df["ask_volume"].resample(freq).sum()
        )
    else:
        volume = pd.Series(0, index=ohlc.index)
    result = ohlc.assign(volume=volume).reset_index()
    return result


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
    """Download data from Dukascopy and store as Parquet.

    Parameters
    ----------
    symbol: str
        Trading pair symbol, e.g. ``"EURUSD"``.
    start, end: str
        Date range in ``YYYY-MM-DD``.
    granularity: Literal["tick", "m1"]
        Data granularity to download.
    out_dir: str
        Directory to store intermediate CSV files.
    out_parquet: Optional[str]
        Path to the output Parquet file. If ``None`` a default path is used.
    aggregate_to: Optional[str]
        Resample frequency when downloading ticks.
    tz: str
        Target timezone for timestamps.

    Returns
    -------
    str
        Path to the saved Parquet file.

    Raises
    ------
    ValueError
        If invalid parameters are provided or required columns are missing.
    FileNotFoundError
        If the download produced no CSV files.
    """

    granularity = granularity.lower()
    if granularity not in VALID_GRANULARITY:
        raise ValueError(f"Invalid granularity: {granularity}")

    resolved_symbol = resolve_symbol(symbol)

    download_dir = Path(out_dir) / resolved_symbol / granularity
    download_dir.mkdir(parents=True, exist_ok=True)

    args = [
        "-s",
        resolved_symbol,
        "-f",
        start,
        "-t",
        end,
        "-g",
        granularity,
        "-d",
        str(download_dir),
    ]

    _duka_download(args)

    csv_files = list(download_dir.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files downloaded")

    dfs = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)

    if granularity == "tick":
        df = _normalize_tick_df(df)
        if aggregate_to:
            df = _aggregate_ticks(df, aggregate_to)
            expected_cols = {"time", "open", "high", "low", "close", "volume"}
            missing = expected_cols.difference(df.columns)
            if missing:
                raise ValueError(f"Aggregated data missing columns: {missing}")
    else:
        df = _normalize_m1_df(df)

    df = df.sort_values("time")
    if tz.upper() != "UTC":
        df["time"] = df["time"].dt.tz_convert(tz)

    if out_parquet is None:
        freq = aggregate_to if aggregate_to else granularity
        out_dir_parquet = Path("data/dukascopy")
        out_dir_parquet.mkdir(parents=True, exist_ok=True)
        fname = f"{resolved_symbol}_{freq}_{start}_{end}.parquet"
        out_parquet = str(out_dir_parquet / fname)

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet)
    return out_parquet
