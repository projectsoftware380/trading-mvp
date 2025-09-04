from pathlib import Path
import yaml, pandas as pd
from core.data_loader import load_parquet
from core.indicators import atr_wilder, rsi, sma, bbands, volume_relative
from core.labeling import label_up_down_atr

def load_cfg(cfg_path="config.yaml"):
    return yaml.safe_load(Path(cfg_path).read_text(encoding="utf-8"))

def build(input_parquet: str, output_parquet: str, cfg_path="config.yaml"):
    cfg = load_cfg(cfg_path)
    df = load_parquet(input_parquet)

    # Indicadores
    df['atr'] = atr_wilder(df, cfg['features']['indicators']['atr'])
    df['rsi'] = rsi(df, cfg['features']['indicators']['rsi'])
    df['ma_fast']  = sma(df['close'], cfg['features']['indicators']['ma_fast'])
    df['ma_slow']  = sma(df['close'], cfg['features']['indicators']['ma_slow'])
    bb_u, bb_m, bb_l = bbands(df, cfg['features']['indicators']['bb_period'], 2.0)
    df['bb_upper'], df['bb_middle'], df['bb_lower'] = bb_u, bb_m, bb_l
    df['vol_rel'] = volume_relative(df, 20)

    # Etiquetas
    df = label_up_down_atr(df, cfg['targets']['horizon'], atr_col='atr')

    # Limpieza: quita NaN iniciales por rolling/EMA/label
    df = df.dropna().reset_index(drop=True)

    # Guardar
    Path(output_parquet).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_parquet, index=False)

    # Muestra rápida
    preview_csv = str(Path(output_parquet).with_suffix(".preview.csv"))
    df.head(200).to_csv(preview_csv, index=False)
    print("Guardado:", output_parquet, "| Preview:", preview_csv, "| Filas:", len(df))

if __name__ == "__main__":
    # ejemplo rápido: ajusta el nombre de tu archivo de entrada
    in_file  = "data/eurusd_1h.parquet"       # <-- pon aquí tu parquet real
    out_file = "data/processed/eurusd_1h_processed.parquet"
    build(in_file, out_file)
