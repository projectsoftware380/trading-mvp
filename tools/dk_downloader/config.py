"""Configuration and utilities for the Dukascopy downloader."""
from __future__ import annotations

from typing import Dict

# Mapping of supported FX symbols. Keys are canonical uppercase codes used in
# the downloader; values are the codes expected by the Dukascopy API.
SYMBOLS: Dict[str, str] = {
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "USDJPY": "USDJPY",
    "USDCHF": "USDCHF",
    "USDCAD": "USDCAD",
    "AUDUSD": "AUDUSD",
    "NZDUSD": "NZDUSD",
    "EURGBP": "EURGBP",
    "EURJPY": "EURJPY",
}

VALID_GRANULARITY = {"tick", "m1"}


def resolve_symbol(symbol: str) -> str:
    """Resolve a user provided symbol to a supported Dukascopy symbol.

    Parameters
    ----------
    symbol:
        Symbol provided by the user. Comparison is case-insensitive.

    Returns
    -------
    str
        Dukascopy-compatible symbol code.

    Raises
    ------
    ValueError
        If the symbol is not supported.
    """

    key = symbol.upper()
    if key not in SYMBOLS:
        raise ValueError(f"Unsupported symbol: {symbol}")
    return SYMBOLS[key]