# Alcance del MVP

## Objetivo actual

Construir un pipeline reproducible para preparar series temporales financieras y generar variables objetivo `up_atr` y `down_atr` expresadas en múltiplos de ATR.

El alcance público actual se concentra en **datos y etiquetado**, no en presentar un modelo predictivo terminado.

## Implementado

- adquisición de datos históricos;
- procesamiento OHLCV;
- cálculo de ATR y otros indicadores;
- feature engineering;
- generación de targets futuros;
- validación de la cola temporal para reducir riesgo de look-ahead bias;
- exportación a Parquet;
- generación de reportes estadísticos;
- pruebas automatizadas.

## Fórmulas de etiquetado

```text
up_atr   = (max High[t+1:t+H] - Close[t]) / ATR[t]
down_atr = (Close[t] - min Low[t+1:t+H]) / ATR[t]
```

## Roadmap experimental

Las siguientes capacidades forman parte de la evolución prevista del proyecto, pero no se presentan como funcionalidades terminadas en este repositorio:

- entrenamiento de modelos tabulares o secuenciales;
- combinación de modelos (por ejemplo, modelos secuenciales + filtros tabulares);
- backtesting integral;
- serving mediante API;
- ejecución en vivo.

Esta separación entre **implementado** y **roadmap** mantiene el repositorio técnicamente verificable y evita atribuir al MVP capacidades que aún no están expuestas en su versión pública.
