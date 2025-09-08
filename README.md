# MVP Step1 Starter Kit

## Carpetas
- **core/** → funciones base (indicadores, etiquetado, carga de datos)
- **data/** → aquí colocas tus archivos `.parquet`
- **notebooks/** → notebooks de validación
- **evaluation/** → scripts de backtest
- **service/** → API FastAPI (más adelante)
- **models/** → modelos guardados

## Archivos
- `config.yaml` → parámetros del MVP
- `MVP_SCOPE.md` → documento del alcance
- `requirements.txt` → dependencias

## Descarga de datos (Dukascopy)

CLI:

```bash
python -m tools.dk_downloader.cli --symbol EURUSD --start 2024-01-01 --end 2024-01-15 --granularity m1
python -m tools.dk_downloader.cli --symbol EURUSD --start 2024-01-01 --end 2024-01-01 --granularity tick --aggregate-to 1min
```

Python:

```python
from tools.dk_downloader.download import download
p = download("EURUSD", "2024-01-01", "2024-01-07", "m1")
print(p)
```