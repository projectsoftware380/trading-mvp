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

## Generación de Reporte de Etiquetado

Para generar un reporte con estadísticas sobre los labels de un dataset, puedes usar el script `scripts/labeling_cli.py`.

Uso:

```bash
python scripts/labeling_cli.py --input-path data/raw/EURUSD/2024_01_01-2024_01_07_m1.parquet --output-path report.json --horizon 10
```

Esto generará un archivo `report.json` con las estadísticas de las columnas `up_atr` y `down_atr`.

### Fórmulas de Etiquetado (up_atr y down_atr)

Las etiquetas `up_atr` y `down_atr` se calculan utilizando las siguientes fórmulas:

- `up_atr = (max_fwd - close) / atr`
- `down_atr = (close - min_fwd) / atr`

Donde:
- `max_fwd`: Es el precio máximo de la columna 'high' en los próximos `horizon` períodos.
- `min_fwd`: Es el precio mínimo de la columna 'low' en los próximos `horizon` períodos.
- `close`: Es el precio de cierre del período actual.
- `atr`: Es el Average True Range (ATR) del período actual.
