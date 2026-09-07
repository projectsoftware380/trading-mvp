# Trading MVP — Pipeline de datos para series temporales financieras

Proyecto experimental en Python para **adquirir, transformar, validar y etiquetar series temporales financieras** antes de su uso en modelos de Machine Learning.

El foco del repositorio está en la preparación correcta de datos: construcción de indicadores, generación de variables objetivo basadas en volatilidad, validación temporal y reproducibilidad del pipeline.

> Este repositorio es una demostración técnica de ingeniería de datos y analítica. No constituye una estrategia de inversión lista para producción ni una recomendación financiera.

## Qué demuestra

- Procesamiento de datos OHLCV con **Pandas y NumPy**.
- Descarga y normalización de datos históricos desde Dukascopy.
- Ingeniería de características con indicadores técnicos.
- Construcción de targets normalizados por **ATR**.
- Validaciones orientadas a evitar **look-ahead bias**.
- Exportación a **Parquet** y generación de reportes JSON.
- CLI reproducible para ejecutar el pipeline.
- Pruebas automatizadas con **pytest**.

## Arquitectura

```mermaid
flowchart LR
    A[Dukascopy / OHLCV] --> B[Descarga y normalización]
    B --> C[Parquet]
    C --> D[Pandas / NumPy]
    D --> E[Indicadores técnicos]
    E --> F[Feature engineering]
    F --> G[Targets up_atr / down_atr]
    G --> H[Validación temporal]
    H --> I[Dataset etiquetado]
    I --> J[Reporte estadístico]
```

## Componentes principales

| Componente | Función |
|---|---|
| `core/data_loader.py` | Carga y validación inicial de datos. |
| `core/indicators.py` | Construcción de indicadores técnicos. |
| `core/atr_wr.py` | Cálculos asociados a volatilidad/ATR. |
| `core/labeling.py` | Generación y validación de targets. |
| `core/build_dataset.py` | Ensamblaje del dataset procesado. |
| `scripts/labeling_cli.py` | Ejecución reproducible del etiquetado y reporte. |
| `tools/dk_downloader/` | Utilidades para adquisición de datos desde Dukascopy. |
| `tests/` | Pruebas del pipeline y de la CLI. |

## Targets

Las variables objetivo se expresan en múltiplos del ATR actual:

```text
up_atr   = (max_fwd - close) / atr
down_atr = (close - min_fwd) / atr
```

Donde `max_fwd` y `min_fwd` representan el máximo y mínimo observados dentro de un horizonte futuro definido.

Los últimos registros del dataset no disponen de horizonte futuro completo. Por diseño, esos targets permanecen como `NaN` y la función `validate_targets` comprueba que la cola de valores faltantes sea consistente con el horizonte especificado. Esta validación ayuda a detectar errores de etiquetado que podrían introducir fuga de información temporal.

## Ejemplo de ejecución

Instalación:

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

Ejemplo de descarga:

```bash
python -m tools.dk_downloader.cli \
  --symbol EURUSD \
  --start 2024-01-01 \
  --end 2024-01-15 \
  --granularity m1
```

Ejemplo de etiquetado:

```bash
python scripts/labeling_cli.py \
  --input-path data/raw/EURUSD/sample.parquet \
  --output-path data/processed/labeled.parquet \
  --report-output-path report.json \
  --horizon 10 \
  --atr-period 14
```

## Pruebas

```bash
pytest
```

Las pruebas usan datos temporales generados durante la ejecución y no dependen de rutas absolutas de un entorno virtual local.

## Resultado de ejemplo

El repositorio conserva `report_explicit.json` como evidencia de una ejecución del pipeline. Ese archivo contiene estadísticas descriptivas del dataset etiquetado. Las proporciones de `up_atr` y `down_atr` son **distribuciones de targets**, no métricas de accuracy de un modelo predictivo.

## Alcance actual

Implementado:

- adquisición de datos;
- transformación y validación;
- indicadores y feature engineering;
- etiquetado temporal;
- reportes;
- pruebas automatizadas.

Fuera del alcance actual del repositorio público:

- entrenamiento de modelos predictivos completos;
- API de serving;
- ejecución automática de operaciones.

Esas capacidades pertenecen a otras etapas/proyectos y no se presentan aquí como funcionalidades terminadas.

## Autor

**Manuel Alfonso Rincón Méndez**  
Tecnólogo en Análisis y Desarrollo de Sistemas de Información · Estudiante de Ingeniería de Sistemas  
Intereses: Python, ingeniería de datos, Machine Learning, automatización e IA aplicada.

## Licencia

MIT. Ver `LICENSE`.
