# Dukascopy Downloader

Descarga datos históricos de [Dukascopy](https://www.dukascopy.com/) y los
convierte a Parquet con columnas OHLCV en UTC.

## CLI

```bash
python -m tools.dk_downloader.cli --symbol EURUSD --start 2024-01-01 --end 2024-01-15 --granularity m1
python -m tools.dk_downloader.cli --symbol EURUSD --start 2024-01-01 --end 2024-01-01 --granularity tick --aggregate-to 1min
```

## Python

```python
from tools.dk_downloader.download import download
p = download("EURUSD", "2024-01-01", "2024-01-07", "m1")
print(p)
```

## Notas

- Requiere `duka`, `pandas>=1.5` y `pyarrow>=12`.
- Los tiempos se guardan en UTC por defecto.
