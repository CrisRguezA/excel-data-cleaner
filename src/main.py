"""
main.py — Complete Excel data cleaning pipeline.

Flow:
1. Load data
2. Pre-validation -> data/reports/
3. Cleaning -> data/reports/
4. Export clean Excel with formatting -> outputs/

Usage:
python main.py
python main.py --input data/raw/my_file.xlsx



main.py — Pipeline completo de limpieza de datos Excel.

Flujo:
    1. Cargar datos
    2. Pre-validación  -> data/reports/
    3. Limpieza        -> data/reports/
    4. Exportar Excel limpio con formato -> outputs/

Uso:
    python main.py
    python main.py --input data/raw/mi_archivo.xlsx
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from validation import run_validation
from cleaning import run_cleaning


# ---------------------------------------------------------------------------
# Default routes / Rutas por defecto
# ---------------------------------------------------------------------------

ROOT        = Path(__file__).parent.parent
INPUT_FILE  = ROOT / "data" / "raw" / "dirty_sales_data.xlsx"
REPORTS_DIR = ROOT / "data" / "reports"
OUTPUTS_DIR = ROOT / "outputs"


# ---------------------------------------------------------------------------
# JSON Serialization / Serialización JSON
# ---------------------------------------------------------------------------

def _json_serializer(obj):
    if isinstance(obj, (np.integer,)):            return int(obj)
    if isinstance(obj, (np.floating,)):           return float(obj)
    if isinstance(obj, (np.bool_,)):              return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime)): return obj.isoformat()
    if pd.isna(obj):                              return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# Save report as JSON / Guardar report como JSON
# ---------------------------------------------------------------------------

def _save_json(report: dict, name: str, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = folder / f"{name}_{timestamp}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=_json_serializer)
    return path


# ---------------------------------------------------------------------------
# Export clean Excel with formatting / Exportar Excel limpio con formato
# ---------------------------------------------------------------------------

def _export_clean_excel(df: pd.DataFrame, folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "cleaned_sales_report.xlsx"

    if "id_venta" in df.columns:
        df["id_venta"] = pd.to_numeric(df["id_venta"], errors="coerce")
        
    df = df.where(pd.notna(df), other=None)

    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="DD/MM/YYYY") as writer:
        df.to_excel(writer, sheet_name="clean_data", index=False)

        workbook  = writer.book
        worksheet = writer.sheets["clean_data"]

        # Cell formats
        header_fmt  = workbook.add_format({
            "bold": True, "font_name": "Arial", "font_size": 11,
            "bg_color": "#1F4E79", "font_color": "#FFFFFF",
            "align": "center", "valign": "vcenter",
            "border": 1
        })
        body_fmt    = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1
        })
        alt_fmt     = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "bg_color": "#EBF3FB"
        })
        date_fmt    = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "DD/MM/YYYY"
        })
        date_alt_fmt = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "DD/MM/YYYY", "bg_color": "#EBF3FB"
        })
        int_fmt     = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "0"
        })
        int_alt_fmt = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "0", "bg_color": "#EBF3FB"
        })
        decimal_fmt = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "#,##0.00"
        })
        decimal_alt_fmt = workbook.add_format({
            "font_name": "Arial", "font_size": 10, "border": 1,
            "num_format": "#,##0.00", "bg_color": "#EBF3FB"
        })

        # Headings
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_fmt)

        # Rows of data
        col_idx = {col: i for i, col in enumerate(df.columns)}
        date_cols    = {"fecha_venta"}
        int_cols     = {"id_venta"}
        decimal_cols = {"cantidad_m3", "precio_m3", "importe"}

        for row_num in range(len(df)):
            is_alt = row_num % 2 == 0
            for col_name in df.columns:
                col_num = col_idx[col_name]
                value   = df.iloc[row_num][col_name]
                
                # Convert NaN to None for xlsxwriter
                try:
                    if pd.isna(value):
                        value = None
                except (TypeError, ValueError):
                    pass

                if value is None:
                    worksheet.write_blank(row_num + 1, col_num, None, body_fmt)
                    continue

                if col_name in date_cols:
                    fmt = date_alt_fmt if is_alt else date_fmt
                elif col_name in int_cols:
                    fmt = int_alt_fmt if is_alt else int_fmt
                elif col_name in decimal_cols:
                    fmt = decimal_alt_fmt if is_alt else decimal_fmt
                else:
                    fmt = alt_fmt if is_alt else body_fmt

                worksheet.write(row_num + 1, col_num, value, fmt)

        # Column widths
        for i, col_name in enumerate(df.columns):
            if col_name == "fecha_venta":
                adjusted_width = 14
            else:
                try:
                    max_length = max(
                        df[col_name].dropna().astype(str).map(len).max(),
                        len(col_name)
                    )
                    adjusted_width = min(max_length + 2, 40)
                except (ValueError, TypeError):
                    adjusted_width = 12
            worksheet.set_column(i, i, adjusted_width)

        # Freeze panes and filter
        worksheet.freeze_panes(1, 0)
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

    return path

# ---------------------------------------------------------------------------
# Main pipeline / Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline(input_file: Path,  output_dir: Path):

    print("\n" + "=" * 55)
    print("  Excel Data Cleaner — Pipeline iniciado")
    print("=" * 55)

    # 1. LOAD
    if not input_file.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {input_file}")

    print(f"\n[1] Cargando: {input_file.name}")
    df = pd.read_excel(input_file, dtype=str)
    print(f"    {df.shape[0]} filas x {df.shape[1]} columnas")

    # 2. PRE-VALIDATION
    print("\n[2] Pre-validacion...")
    validation_report = run_validation(df, verbose=True)
    validation_path = _save_json(validation_report, "validation_report", REPORTS_DIR)
    print(f"    Report guardado: {validation_path.name}")

    # 3. CLEANING
    print("\n[3] Limpieza...")
    df_clean, cleaning_report = run_cleaning(df, verbose=True)
    cleaning_path = _save_json(cleaning_report, "cleaning_report", REPORTS_DIR)
    print(f"    Report guardado: {cleaning_path.name}")

    # 4. EXPORT CLEAN EXCEL
    print("\n[4] Exportando Excel limpio...")
    output_path = _export_clean_excel(df_clean, output_dir)
    print(f"    Guardado: {output_path}")

    # FINAL SUMMARY
    s = cleaning_report["shape"]
    print("\n" + "=" * 55)
    print("  PIPELINE COMPLETADO")
    print("=" * 55)
    print(f"  Filas originales   : {s['original_rows']}")
    print(f"  Filas finales      : {s['cleaned_rows']}")
    print(f"  Filas eliminadas   : {s['rows_removed_total']}")
    print(f"\n  Outputs:")
    print(f"    {output_path}")
    print(f"    {validation_path}")
    print(f"    {cleaning_path}")
    print("=" * 55 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Limpia un archivo Excel y genera reports de validacion y limpieza."
    )
    parser.add_argument(
        "--input", type=Path,
        default=INPUT_FILE,
        help="Ruta del archivo Excel de entrada"
    )
    parser.add_argument(
        "--output", type=Path,
        default=OUTPUTS_DIR,
        help="Carpeta de salida del Excel limpio"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.input, args.output)
