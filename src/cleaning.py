"""
cleaning.py — Data cleaning and transformation prior to post-validation.

Pure functions: they receive a DataFrame and return a transformed DataFrame.
Functions that remove rows return a tuple: (df, n_removed).
They do not raise exceptions when a column does not exist:
they silently skip it so that main.py can decide what to do.

Numeric parsing assumes English-style numeric input:
- decimal separator: dot (.)
- thousands separator: comma (,)

cleaning.py — Limpieza y transformación de datos previa a la post-validación.

Funciones puras: reciben un DataFrame y devuelven un DataFrame transformado.
Las funciones que eliminan filas devuelven una tupla: (df, n_removed).
No lanzan excepciones cuando una columna no existe:
la omiten silenciosamente para que main.py decida cómo actuar.

El parseo numérico asume entradas en formato inglés:
- separador decimal: punto (.)
- separador de miles: coma (,)
"""

from __future__ import annotations

import re
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Business rules / Reglas de negocio
# ---------------------------------------------------------------------------

CANONICAL_ESTADO_MAP = {
    "cerrado": "Cerrado",
    "cerrada": "Cerrado",
    "ok": "Cerrado",
    "pendiente": "Pendiente",
    "cancelado": "Cancelado",
}

TEXT_COLS_TITLE = ["cliente", "comercial", "tipo_madera"]
NUMERIC_COLS = ["importe", "cantidad_m3", "precio_m3"]
_DATE_FORMATS = ["%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"]


# ---------------------------------------------------------------------------
# 1. Merge duplicate columns / Fusión de columnas duplicadas
# ---------------------------------------------------------------------------

def merge_duplicate_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Combine pairs of columns that contain the same distributed data:
    - ('cliente', 'Cliente') -> 'cliente'
    - ('fecha venta', 'Fecha_Venta') -> 'fecha venta'

    Strategy: coalesce — first non-null value between the two columns.
    The secondary column is removed after the merge.

    Combina pares de columnas que contienen el mismo dato repartido:
    - ('cliente', 'Cliente') -> 'cliente'
    - ('fecha venta', 'Fecha_Venta') -> 'fecha venta'

    Estrategia: coalesce — primer valor no nulo entre ambas columnas.
    La columna secundaria se elimina tras la fusión.
    """
    df = df.copy()

    pairs = [
        ("cliente", "Cliente"),
        ("fecha venta", "Fecha_Venta"),
    ]

    merged_pairs = []
    dropped_columns = []

    for primary, secondary in pairs:
        if primary in df.columns and secondary in df.columns:
            df[primary] = df[primary].combine_first(df[secondary])
            df = df.drop(columns=[secondary])

            merged_pairs.append({
                "primary_column": primary,
                "secondary_column": secondary,
                "strategy": "combine_first"
            })
            dropped_columns.append(secondary)

    info = {
        "pairs_defined": len(pairs),
        "pairs_merged": len(merged_pairs),
        "merged_pairs": merged_pairs,
        "dropped_secondary_columns": dropped_columns
    }

    return df, info


# ---------------------------------------------------------------------------
# 2. Normalize column names / Normalizar nombres de columnas
# ---------------------------------------------------------------------------

def normalize_column_name(col: str) -> str:
    """
    Normalize a column name using a standard rule:
    - strip
    - lowercase
    - spaces -> _
    - non-alphanumeric chars -> _
    - repeated underscores collapsed

    Normaliza un nombre de columna usando una regla estándar:
    - strip
    - minúsculas
    - espacios -> _
    - caracteres no alfanuméricos -> _
    - colapsa guiones bajos repetidos
    """
    normalized = str(col).strip().lower()
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^\w]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def normalize_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Normalize all column names and return the mapping.

    Normaliza todos los nombres de columna y devuelve el mapeo.
    """
    df = df.copy()

    original_columns = list(df.columns)
    normalized_columns = [normalize_column_name(c) for c in df.columns]

    mapping = [
        {"original": original, "normalized": normalized}
        for original, normalized in zip(original_columns, normalized_columns)
    ]

    changed = [
        item for item in mapping
        if item["original"] != item["normalized"]
    ]

    df.columns = normalized_columns

    info = {
        "total_columns": len(original_columns),
        "renamed_columns": len(changed),
        "mapping": mapping,
        "changed_columns": changed
    }

    return df, info


# ---------------------------------------------------------------------------
# 3. Drop fully empty rows / Eliminar filas completamente vacías
# ---------------------------------------------------------------------------

def drop_fully_empty_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Drop rows where all values are null.

    Elimina filas donde todos los valores son nulos.
    """
    df = df.copy()

    before = len(df)
    df = df.dropna(how="all").reset_index(drop=True)
    removed = before - len(df)

    return df, removed


# ---------------------------------------------------------------------------
# 4. Remove duplicates by id_venta / Eliminar duplicados por id_venta
# ---------------------------------------------------------------------------

def remove_duplicates_by_id_venta(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove duplicated rows based on id_venta, keeping the first occurrence.

    Elimina filas duplicadas según id_venta, conservando la primera aparición.
    """
    df = df.copy()

    if "id_venta" not in df.columns:
        return df, 0

    before = len(df)
    df = df.drop_duplicates(subset=["id_venta"], keep="first").reset_index(drop=True)
    removed = before - len(df)

    return df, removed


# ---------------------------------------------------------------------------
# 5. Standardize text fields / Estandarizar campos de texto
# ---------------------------------------------------------------------------

def standardize_text_fields(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Standardize text fields:
    - cliente, comercial, tipo_madera -> strip + Title Case
    - estado -> canonical values
    - pais -> normalize common variants without forcing unknown values to null

    Estandariza campos de texto:
    - cliente, comercial, tipo_madera -> strip + Title Case
    - estado -> valores canónicos
    - pais -> normaliza variantes comunes sin forzar a null los valores desconocidos
    """
    df = df.copy()

    info = {
        "title_case_columns_processed": [],
        "estado_standardized": False,
        "pais_standardized": False
    }

    for col in TEXT_COLS_TITLE:
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = (
                df.loc[mask, col]
                .astype(str)
                .str.strip()
                .str.title()
            )
            info["title_case_columns_processed"].append(col)

    if "estado" in df.columns:
        mask = df["estado"].notna()
        cleaned = (
            df.loc[mask, "estado"]
            .astype(str)
            .str.strip()
            .str.lower()
        )
        df.loc[mask, "estado"] = cleaned.replace(CANONICAL_ESTADO_MAP)
        info["estado_standardized"] = True

    if "pais" in df.columns:
        mask = df["pais"].notna()
        cleaned = (
            df.loc[mask, "pais"]
            .astype(str)
            .str.strip()
            .str.lower()
        )

        pais_map = {
            "es": "España",
            "españa": "España",
            "espana": "España",
        }

        df.loc[mask, "pais"] = cleaned.replace(pais_map).str.title()
        info["pais_standardized"] = True

    return df, info


# ---------------------------------------------------------------------------
# 6. Fill certificacion / Rellenar certificacion
# ---------------------------------------------------------------------------

def fill_certificacion(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Fill null values in certificacion with 'Sin certificación'.

    Rellena valores nulos en certificacion con 'Sin certificación'.
    """
    df = df.copy()

    if "certificacion" not in df.columns:
        return df, {
            "column_found": False,
            "nulls_filled": 0,
            "fill_value": "Sin certificación"
        }

    nulls_before = int(df["certificacion"].isna().sum())
    df["certificacion"] = df["certificacion"].fillna("Sin certificación")
    nulls_after = int(df["certificacion"].isna().sum())

    return df, {
        "column_found": True,
        "nulls_filled": nulls_before - nulls_after,
        "fill_value": "Sin certificación"
    }


# ---------------------------------------------------------------------------
# 7. Parse fecha_venta to datetime / Convertir fecha_venta a datetime
# ---------------------------------------------------------------------------

def _parse_dates_multiformat(series: pd.Series, formats: list[str]) -> pd.Series:
    """
    Try to parse a date series by testing each format in order.
    Useful when the file mixes several date formats in the same column.

    Intenta parsear una serie de fechas probando cada formato en orden.
    Útil cuando el archivo mezcla varios formatos en la misma columna.
    """
    result = pd.Series(pd.NaT, index=series.index)

    for fmt in formats:
        mask = result.isna() & series.notna()
        if not mask.any():
            break

        parsed = pd.to_datetime(series[mask], format=fmt, errors="coerce")
        result.loc[mask] = parsed

    return result


def parse_fecha_venta(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Parse fecha_venta into datetime using multiple accepted formats.

    Convierte fecha_venta a datetime usando varios formatos aceptados.
    """
    df = df.copy()

    if "fecha_venta" not in df.columns:
        return df, {
            "column_found": False,
            "column": "fecha_venta",
            "parsed_to_datetime": False,
            "null_after_parse": 0
        }

    df["fecha_venta"] = _parse_dates_multiformat(df["fecha_venta"], _DATE_FORMATS)

    return df, {
        "column_found": True,
        "column": "fecha_venta",
        "parsed_to_datetime": True,
        "null_after_parse": int(df["fecha_venta"].isna().sum())
    }


# ---------------------------------------------------------------------------
# 8. Parse numeric columns / Convertir columnas numéricas
# ---------------------------------------------------------------------------

def parse_numeric_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Parse numeric columns assuming English-style numeric input:
    - decimal separator: dot (.)
    - thousands separator: comma (,)

    Expected valid examples:
    - 1234.56
    - 1,234.56
    - $2,500.00

    Incompatible formats such as 1.234,56 may become NaN.

    Convierte columnas numéricas asumiendo entradas en formato inglés:
    - separador decimal: punto (.)
    - separador de miles: coma (,)

    Ejemplos válidos esperados:
    - 1234.56
    - 1,234.56
    - $2,500.00

    Formatos incompatibles como 1.234,56 pueden convertirse en NaN.
    """
    df = df.copy()

    results = {}
    checked_cols = []
    missing_cols = []

    for col in NUMERIC_COLS:
        if col not in df.columns:
            missing_cols.append(col)
            continue

        checked_cols.append(col)
        original_nulls = int(df[col].isna().sum())

        cleaned = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace("$", "", regex=False)
            .str.replace("€", "", regex=False)
            .str.replace("£", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", "", regex=False)
        )

        cleaned = cleaned.replace({
            "nan": np.nan,
            "none": np.nan,
            "": np.nan
        })

        parsed = pd.to_numeric(cleaned, errors="coerce")
        nulls_after_parse = int(parsed.isna().sum())
        unparseable_values = nulls_after_parse - original_nulls

        df[col] = parsed

        results[col] = {
            "null_original": original_nulls,
            "null_after_parse": nulls_after_parse,
            "unparseable_values": unparseable_values,
            "assumed_input_format": "english"
        }

    return df, {
        "columns_checked": checked_cols,
        "missing_columns": missing_cols,
        "total_columns_checked": len(checked_cols),
        "numeric_parsing_by_column": results,
        "numeric_input_assumption": "english"
    }


# ---------------------------------------------------------------------------
# 9. Recalculate importe / Recálculo de importe
# ---------------------------------------------------------------------------

def recalculate_importe(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Where importe is null and cantidad_m3 + precio_m3 are available:
    importe = cantidad_m3 * precio_m3

    Returns (df, n_recovered_rows).

    Donde importe es nulo y cantidad_m3 + precio_m3 están disponibles:
    importe = cantidad_m3 * precio_m3

    Devuelve (df, n_filas_recuperadas).
    """
    df = df.copy()

    for col in ["importe", "cantidad_m3", "precio_m3"]:
        if col not in df.columns:
            return df, 0

    mask_recalc = (
        df["importe"].isna()
        & df["cantidad_m3"].notna()
        & df["precio_m3"].notna()
    )

    n_recovered = int(mask_recalc.sum())

    df.loc[mask_recalc, "importe"] = (
        df.loc[mask_recalc, "cantidad_m3"] * df.loc[mask_recalc, "precio_m3"]
    )

    return df, n_recovered


# ---------------------------------------------------------------------------
# 10. Filter invalid rows / Filtrar filas inválidas
# ---------------------------------------------------------------------------

def filter_invalid_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Remove rows with:
    - null id_venta
    - null fecha_venta
    - null importe
    - importe <= 0

    Elimina filas con:
    - id_venta nulo
    - fecha_venta nula
    - importe nulo
    - importe <= 0
    """
    df = df.copy()
    before = len(df)

    breakdown = {
        "null_id_venta"   : int(df["id_venta"].isna().sum()) if "id_venta" in df.columns else 0,
        "null_fecha_venta": int(df["fecha_venta"].isna().sum()) if "fecha_venta" in df.columns else 0,
        "null_importe"    : int(df["importe"].isna().sum()) if "importe" in df.columns else 0,
        "zero_or_neg_importe": int((df["importe"] <= 0).sum()) if "importe" in df.columns else 0,
    }
    
    mask = pd.Series(True, index=df.index)

    if "id_venta" in df.columns:
        mask &= df["id_venta"].notna()

    if "fecha_venta" in df.columns:
        mask &= df["fecha_venta"].notna()

    if "importe" in df.columns:
        mask &= df["importe"].notna() & (df["importe"] > 0)
   
    df = df[mask].reset_index(drop=True)

    return df, {"total_removed": before - len(df), "breakdown": breakdown}


# ---------------------------------------------------------------------------
# Cleaning report / Reporte de limpieza
# ---------------------------------------------------------------------------

def build_cleaning_report(
    original_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    merge_info: dict,
    column_norm_info: dict,
    empty_rows_removed: int,
    duplicates_removed: int,
    standardize_text_fields: dict,
    certificacion_info: dict,
    fecha_info: dict,
    numeric_info: dict,
    recovered_importe_rows: int,
    invalid_rows_removed: int
) -> dict:
    """
    Build a structured cleaning report similar in spirit to validation.py.

    Construye un reporte estructurado de limpieza con una lógica paralela
    a validation.py.
    """
    return {
        "shape": {
            "original_rows": len(original_df),
            "cleaned_rows": len(cleaned_df),
            "original_columns": original_df.shape[1],
            "cleaned_columns": cleaned_df.shape[1],
            "rows_removed_total": len(original_df) - len(cleaned_df)
        },
        "merged_columns": merge_info,
        "normalized_column_names": column_norm_info,
        "empty_rows": {
            "empty_rows_removed": empty_rows_removed
        },
        "duplicates_by_id_venta": {
            "duplicates_removed": duplicates_removed
        },
        "text_standardization": standardize_text_fields,
        "certificacion_fill": certificacion_info,
        "date_parsing": fecha_info,
        "numeric_parsing": numeric_info,
        "importe_recalculation": {
            "recovered_rows": recovered_importe_rows
        },
        "filtered_invalid_rows": invalid_rows_removed,
        "nulls_after_cleaning": {
            "missing_per_column": cleaned_df.isna().sum().to_dict(),
            "total_missing_values": int(cleaned_df.isna().sum().sum())
        }
    }


def _print_cleaning_report(report: dict) -> None:
    """
    Print a readable cleaning summary to the console.

    Imprime un resumen legible de limpieza en consola.
    """
    print("=" * 52)
    print("  CLEANING REPORT")
    print("=" * 52)

    s = report["shape"]
    print(
        f"Shape                       : "
        f"{s['original_rows']} -> {s['cleaned_rows']} rows | "
        f"{s['original_columns']} -> {s['cleaned_columns']} columns"
    )
    print(f"Total rows removed          : {s['rows_removed_total']}")

    mc = report["merged_columns"]
    print(f"Merged duplicate pairs      : {mc['pairs_merged']}")
    for item in mc["merged_pairs"]:
        print(
            f"  ✓ {item['secondary_column']} -> {item['primary_column']} "
            f"({item['strategy']})"
        )

    nc = report["normalized_column_names"]
    print(f"Renamed columns             : {nc['renamed_columns']}")
    for item in nc["changed_columns"]:
        print(f"  ✓ {item['original']} -> {item['normalized']}")

    er = report["empty_rows"]
    print(f"Fully empty rows removed    : {er['empty_rows_removed']}")

    dd = report["duplicates_by_id_venta"]
    print(f"Duplicates by id_venta      : {dd['duplicates_removed']} removed")

    ts = report["text_standardization"]
    print(
        f"Title-case columns          : "
        f"{', '.join(ts['title_case_columns_processed']) if ts['title_case_columns_processed'] else 'None'}"
    )
    print(f"Estado standardized         : {ts['estado_standardized']}")
    print(f"Pais standardized           : {ts['pais_standardized']}")

    cf = report["certificacion_fill"]
    print(f"Certificacion nulls filled  : {cf['nulls_filled']}")

    dp = report["date_parsing"]
    print(f"Fecha_venta parsed          : {dp['parsed_to_datetime']}")
    if dp["column_found"]:
        print(f"  Null after parse          : {dp['null_after_parse']}")

    np_info = report["numeric_parsing"]
    print(f"Numeric columns checked     : {np_info['total_columns_checked']}")
    print(f"Numeric input assumption    : {np_info['numeric_input_assumption']}")
    if np_info["missing_columns"]:
        print(f"  Missing numeric cols      : {', '.join(np_info['missing_columns'])}")
    for col, info in np_info["numeric_parsing_by_column"].items():
        print(
            f"  {col}: unparseable={info['unparseable_values']}, "
            f"null_original={info['null_original']}, "
            f"null_after_parse={info['null_after_parse']}"
        )

    ir = report["importe_recalculation"]
    print(f"Importe rows recovered      : {ir['recovered_rows']}")

    fr = report["filtered_invalid_rows"]
    print(f"Invalid rows removed        : {fr['total_removed']}")
    print(f"  null id_venta             : {fr['breakdown']['null_id_venta']}")
    print(f"  null fecha_venta          : {fr['breakdown']['null_fecha_venta']}")
    print(f"  null importe              : {fr['breakdown']['null_importe']}")
    print(f"  zero_or_neg_importe       : {fr['breakdown']['zero_or_neg_importe']}")
    

    na = report["nulls_after_cleaning"]
    print(f"Missing values after clean  : {na['total_missing_values']}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point: complete cleaning / Punto de entrada: limpieza completa
# ---------------------------------------------------------------------------

def run_cleaning(df: pd.DataFrame, verbose: bool = True) -> tuple[pd.DataFrame, dict]:
    """
    Execute the full cleaning pipeline and return:
    - cleaned DataFrame
    - cleaning report

    Ejecuta el pipeline completo de limpieza y devuelve:
    - DataFrame limpio
    - reporte de limpieza
    """
    original_df = df.copy()

    
    df, merge_info = merge_duplicate_columns(df)
   
    df, column_norm_info = normalize_column_names(df)
    
    df, empty_rows_removed = drop_fully_empty_rows(df)

    df, duplicates_removed = remove_duplicates_by_id_venta(df)
    
    df, text_info = standardize_text_fields(df)
    
    df, cert_info = fill_certificacion(df)
   
    df, fecha_info = parse_fecha_venta(df)

    df, numeric_info = parse_numeric_columns(df)

    df, recovered_importe_rows = recalculate_importe(df)

    df, invalid_rows_removed = filter_invalid_rows(df)

    report = build_cleaning_report(
        original_df=original_df,
        cleaned_df=df,
        merge_info=merge_info,
        column_norm_info=column_norm_info,
        empty_rows_removed=empty_rows_removed,
        duplicates_removed=duplicates_removed,
        standardize_text_fields=text_info,
        certificacion_info=cert_info,
        fecha_info=fecha_info,
        numeric_info=numeric_info,
        recovered_importe_rows=recovered_importe_rows,
        invalid_rows_removed=invalid_rows_removed
    )

    if verbose:
        _print_cleaning_report(report)

    return df, report