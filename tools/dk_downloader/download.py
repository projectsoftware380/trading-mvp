# tools/dk_downloader/download.py
"""Utilities to download and process Dukascopy data (duka 0.2.0 compatible)."""
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from .config import VALID_GRANULARITY, resolve_symbol


def _duka_download(args_list: list[str]) -> None:
    """
    Invoca Dukascopy downloader (duka) de forma robusta:

    1) Intenta usar la API interna si está disponible:
       from duka.app.app import run as duka_run
       duka_run(args_list)

    2) Si falla, usa el ejecutable del entorno virtual:
       - Windows:  <venv>\\Scripts\\duka.exe
       - Linux/Mac: <venv>/bin/duka
       Probando con y sin subcomando 'download' (algunas variantes lo requieren).
    """
    # 1) API interna (si existe en la versión instalada)
    try:  # pragma: no cover
        from duka.app.app import run as duka_run  # type: ignore
        duka_run(args_list)
        return
    except Exception:
        # Seguimos con el fallback al ejecutable
        pass

    # 2) Ejecutable
    import os
    import sys
    import subprocess
    from shutil import which

    scripts_dir = Path(sys.executable).parent
    duka_bin = scripts_dir / ("duka.exe" if os.name == "nt" else "duka")

    candidates: list[list[str]] = []
    if duka_bin.exists():
        candidates.append([str(duka_bin), "download"] + args_list)
        candidates.append([str(duka_bin)] + args_list)
    else:
        found = which("duka")
        if found:
            candidates.append([found, "download"] + args_list)
            candidates.append([found] + args_list)

    if not candidates:
        raise RuntimeError(
            "No se encontró el ejecutable 'duka'. "
            "Verifica que duka==0.2.0 esté instalado en este entorno."
        )

    last_err: Exception | None = None
    for cmd in candidates:
        try:
            subprocess.run(cmd, check=True)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            # probar siguiente variante
            continue

    raise RuntimeError(f"Fallo al ejecutar duka. Intentos: {candidates}\n{last_err}")


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
        # si no hay volúmenes en ticks, deja 0
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
    """
    Download data from Dukascopy (via duka) and store as Parquet.

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

    resolved_symbol = resolve_symbol(symbol)

    download_dir = Path(out_dir) / resolved_symbol / granularity
    download_dir.mkdir(parents=True, exist_ok=True)

    # Flags cortas compatibles con duka 0.2.0
    args = [
        "-s", resolved_symbol,
        "-f", start,
        "-t", end,
        "-g", granularity,
        "-d", str(download_dir),
    ]
    _duka_download(args)

    # Consolidación de CSVs a DataFrame
    csv_files = list(download_dir.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV files downloaded")

    dfs = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)

    # Normalización según tipo
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

    # Orden y TZ
    df = df.sort_values("time")
    if tz.upper() != "UTC":
        df["time"] = df["time"].dt.tz_convert(tz)

    # Salida Parquet
    if out_parquet is None:
        freq = aggregate_to if aggregate_to else granularity
        out_dir_parquet = Path("data/dukascopy")
        out_dir_parquet.mkdir(parents=True, exist_ok=True)
        fname = f"{resolved_symbol}_{freq}_{start}_{end}.parquet"
        out_parquet = str(out_dir_parquet / fname)

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_parquet)
    return out_parquet
