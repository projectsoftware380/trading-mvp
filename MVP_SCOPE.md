# Ítem 0 — Alcance del MVP

Objetivo: predecir `up_atr` y `down_atr` en múltiplos de ATR para un único par/timeframe,
y usarlos para SL/TP adaptativos y filtrado de señales en un backtest mínimo.

Incluye: EURUSD 1h (editable), H=10, OHLCV+ATR/RSI/MAs/BB, LSTM→XGBoost, backtest mínimo.  
No incluye: multipar, integraciones en vivo, RL, trailing/BE, sentimiento/tendencia avanzados.

Fórmulas:
- `up_atr = (max High[t+1:t+H] - Close[t]) / ATR[t]`
- `down_atr = (Close[t] - min Low[t+1:t+H]) / ATR[t]`