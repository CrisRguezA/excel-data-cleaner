"""
validation.py — Data quality diagnostics prior to cleaning.

validation.py — Diagnóstico de calidad de datos previo a la limpieza.

Pure functions: they receive a DataFrame and return a dictionary with results.
They do not modify the DataFrame. They do not raise exceptions when issues are found:
they document them and return them so that main.py can decide what to do.

Funciones puras: reciben un DataFrame y devuelven un diccionario con resultados.
No modifican el DataFrame. No lanzan excepciones cuando hay problemas:
los documentan y los devuelven para que main.py decida cómo actuar.
"""
import re
import pandas as pd


# ---------------------------------------------------------------------------
# Business rules / Reglas de negocio
# ---------------------------------------------------------------------------

VALID_ESTADOS = {"cerrado", "cerrada", "ok", "pendiente", "cancelado"}

MANDATORY_COLS = ["id_venta", "fecha_venta", "importe", "cliente"]

CATEGORICAL_COLS = ["cliente", "Cliente", "producto ", "tipo_madera", "certificacion", "estado", "comercial", "pais"]

KEY_COL = "ID Venta"
DATE_COLS = ["fecha venta", "Fecha_Venta"]
NUM_COLS = ["importe ", "cantidad_m3 ", "precio_m3"]


def normalize_column_name(col: str) -> str:
    """
    Normalize a column name using a simple standard rule.
    Normaliza un nombre de columna usando una regla estándar simple.
    """
    normalized = col.strip().lower()
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^\w]", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")

# ---------------------------------------------------------------------------
# 1. Structure / Estructura
# ---------------------------------------------------------------------------

def check_shape(df: pd.DataFrame) -> dict:
    """
    Basic dataset overview.
    Resumen básico del dataset.
    """
    missing_series = df.isna().sum()
    
    return{
        "total_columns": len(df.columns),
        "total_rows": len(df),
        "shape": df.shape,
        "columns": list(df.columns),
        "missing_per_column": missing_series.to_dict(),
        "total_missing_values": int(missing_series.sum()),
        "missing_values_found": missing_series.sum() > 0
    }


def check_column_names(df: pd.DataFrame) -> dict:
    """
    Detect spaces and problematic characters in column names.
    Detecta espacios y caracteres problemáticos en los nombres de columna.
    """
    issues = []
    problematic_chars = ["-", "/", "?", "(", ")", "%", "."]
    
    for col in df.columns:
        flags = []
        if col != col.lstrip():  flags.append("leading_space")
        if col != col.rstrip():  flags.append("trailing_space")
        if " " in col.strip():   flags.append("internal_space")
        if any(char in col for char in problematic_chars): flags.append("problematic_characters")
        
        if flags:
            issues.append({
                "column": col,
                "flags": flags
            })

    return {
        "total_columns": len(df.columns),
        "issues_found": len(issues) > 0,
        "n_issues": len(issues),
        "columns_with_issues": issues
    }

def check_duplicates_after_norm(df: pd.DataFrame) -> dict:
    """
    Detect duplicate columns after normalizing the names.
    Detecta columnas duplicadas después de normalizar los nombres.
    """
    normalized_headers = [normalize_column_name(col) for col in df.columns] # No modificated DataFrame
    
    seen = {}
    duplicates = []
    mapping = []

    for original_col, norm_col in zip(df.columns, normalized_headers):
        if norm_col in seen:
            duplicates.append({
                "column": original_col,
                "normalized": norm_col,
                "conflicts_with": seen[norm_col]
            })                
        else:
            seen[norm_col] = original_col
        
        mapping.append({
            "original": original_col,
            "normalized": norm_col
        })

    return {
        "total_columns": len(df.columns),
        "duplicates_found": len(duplicates) > 0,
        "n_duplicates": len(duplicates),
        "duplicates": duplicates,
        "mapping": mapping
    }


# ---------------------------------------------------------------------------
# 2. Data Quality/ Calidad
# ---------------------------------------------------------------------------

def check_missing_values(df) -> dict:
    """
    Detect missing values.
    Detecta valores vacíos.
    """
    
    results = []
    total = len(df)
    
    for column in df.columns:
        non_null_count = df[column].notna().sum()
        null_count = total - non_null_count
        null_percentage = (null_count / total) * 100
        
        results.append({
                'column': column,
                'non_null_count': non_null_count,
                'null_count': null_count,
                'null_percentage': round(null_percentage, 2)
                })
    
    
    return {
            "total_columns": len(df.columns),
            "total_rows": total,
            "missing_values_found": any(r["null_count"] > 0 for r in results),
            "columns_with_missings": results,
        }

def check_empty_rows (df:pd.DataFrame) -> dict:
    """
    Detect empty rows.
    Detecta filas (registros) vacías.
    """
    empty_rows = df.isna().all(axis=1).sum()
    empty_mask = df.isna().all(axis=1) # boolean mask
    empty_rows_idx = df[empty_mask].index.tolist()

    rows = [{"row_index": int(idx)} for idx in empty_rows_idx]

    return{
        "total_columns": len(df.columns),
        "total_rows": len(df),
        "empty_rows_found": len(empty_rows_idx) > 0,
        "n_empty_rows": len(empty_rows_idx),
        "empty_rows": rows
    }

# ---------------------------------------------------------------------------
# 3. Consistency/ Consistencia
# ---------------------------------------------------------------------------

def check_uniqueness(df: pd.DataFrame, column_name: str) -> dict:
    """
    Detect duplicate values in a column (e.g., id_venta).
    Detecta valores duplicados en una columna (por ejemplo, id_venta).
    """
    if column_name not in df.columns:
        return {
            "column": column_name,
            "error": f"Column '{column_name}' not found"
        }    
    
    duplicate_values = df[df.duplicated(column_name, keep=False)][column_name].dropna()
    duplicated_v = []

    for value in duplicate_values.unique():
        count = (df[column_name] == value).sum()
        duplicated_v.append({
            "value": value,
            "repetitions": int(count)
        }) 
    
    return{
        "column": column_name,
        "total_columns": len(df.columns),
        "total_rows": len(df),
        "duplicates_found": len(duplicated_v) > 0,
        "n_duplicates_values": len(duplicate_values),
        "n_rows_with_duplicates": int(len(duplicate_values)),
        "duplicated_values": duplicated_v
    }

def check_categorical_values(df: pd.DataFrame, cols: list = None) -> dict:
    """
    List unique values in categorical columns.
    Crea una lista de valores únicos en columnas categóricas
    """
  
    if cols is None:
        cols = CATEGORICAL_COLS
   
    valid_cols = []
    missing_cols = []

    for col in cols:
        if col in df.columns:
            valid_cols.append(col)
        else:
            missing_cols.append(col)

    unique_values_frequency = []

    for col in cols:
        unique_vals = sorted(df[col].dropna().unique().tolist())
        value_counts = df[col].dropna().value_counts()
        
        unique_values_frequency.append({
            "column": col,
            "unique_values": unique_vals,
            "n_uniques_values": len(unique_vals),
            "value_frequencies": {
                str(value): int(count)
                for value, count in value_counts.items()}                                    
        })
        

    return {
        "columns_requested": cols,
        "columns_checked": valid_cols,
        "missing_columns": missing_cols,
        "total_columns_checked": len(valid_cols),
        "total_rows": len(df),
        "categorical_values_found": len(unique_values_frequency) > 0,
        "categorical_value_summary": unique_values_frequency
        }

def check_date_columns(df: pd.DataFrame, cols: list = None) -> dict:
    """
    Detect unparseable values in a date column.
    Detecta valores no convertibles a fecha.
    """
    if cols is None:
        cols = DATE_COLS
    
    results = {}
    missing_cols = []

    for col in cols:
        if col not in df.columns:
            missing_cols.append(col)
            continue
            
        parsed = pd.to_datetime(df[col], errors="coerce")
    
        total_non_null = int(df[col].notna().sum())
        total_null_original = int(df[col].isna().sum())
        total_null_after_parse = int(parsed.isna().sum())
    
        unparseable = total_null_after_parse - total_null_original

        results[col] = {
            "total_non_null": total_non_null,
            "null_original": total_null_original,
            "null_after_parse": total_null_after_parse,
            "unparseable_values": unparseable            
        }
    total_unparseable = sum(
        info["unparseable_values"] for info in results.values()
    )
    return {
        "columns_checked": list(results.keys()),
        "missing_columns": missing_cols,
        "total_rows": len(df),
        "total_columns_checked": len(results),
        "total_unparseable_values": total_unparseable,
        "date_validation_by_column": results
    }

def check_numeric_columns(df: pd.DataFrame, cols: list = None) -> dict:
    """
    Detect non-numeric values, zero and negatives in numeric columns.
    Detecta valores no numéricos, ceros y negativos.
    """
    if cols is None:
        cols = NUM_COLS
    
    results = {}
    missing_cols = []
    
    for col in cols:
        if col not in df.columns:
            missing_cols.append(col)
            continue
            
        parsed = pd.to_numeric(df[col], errors="coerce")
    
        total_non_null = int(df[col].notna().sum())
        total_null_original = int(df[col].isna().sum())
        total_null_after_parse = int(parsed.isna().sum())
    
        unparseable = total_null_after_parse - total_null_original

        n_zero = int((parsed == 0).sum())
        n_negative = int((parsed < 0).sum())
        n_valid = int((parsed > 0).sum())

        results[col] = {
            "total_non_null": total_non_null,
            "null_original": total_null_original,
            "null_after_parse": total_null_after_parse,
            "unparseable_values": unparseable,
            "zero_count": n_zero,
            "negative_count": n_negative,
            "valid_positive_values": n_valid
        }
    
    total_unparseable = sum(
        info["unparseable_values"] for info in results.values()
    )

    total_zero_values = sum(
        info["zero_count"] for info in results.values()
    )

    total_negative_values = sum(
        info["negative_count"] for info in results.values()
    )
    
    return {
        "columns_checked": list(results.keys()),
        "missing_columns": missing_cols,
        "total_rows": len(df),
        "total_columns_checked": len(results),
        "total_unparseable_values": total_unparseable,
        "total_zero_values": total_zero_values,
        "total_negative_values" : total_negative_values,
        "numeric_validation_by_column": results
    }

# ---------------------------------------------------------------------------
# 4. Business rules / Reglas de negocio
# ---------------------------------------------------------------------------

def check_business_rules(df: pd.DataFrame) -> dict:
    """
    Validate business rules on key columns.
    Valida reglas de negocio en columnas clave.
    """
    df_norm = df.copy()
    
    if "cliente" in df.columns and "Cliente" in df.columns:
        df_norm["cliente"] = df["cliente"].combine_first(df["Cliente"])
        
    if "fecha venta" in df.columns and "Fecha_Venta" in df.columns:
        df_norm["fecha venta"] = df["fecha venta"].combine_first(df["Fecha_Venta"])
        
    cols_to_drop = [col for col in ["Cliente", "Fecha_Venta"] if col in df_norm.columns]
    df_norm = df_norm.drop(columns = cols_to_drop)

    df_norm.columns = [normalize_column_name(c) for c in df_norm.columns]

    numeric_importe = pd.to_numeric(df_norm["importe"], errors="coerce") if "importe" in df_norm.columns else None
    parsed_fecha    = pd.to_datetime(df_norm["fecha_venta"], errors="coerce") if "fecha_venta" in df_norm.columns else None
    
    
    invalid_estado = ~df_norm["estado"].str.strip().str.lower().isin(VALID_ESTADOS) if "estado" in df_norm.columns else pd.Series(False, index=df_norm.index)

    rules = {
        "null_sales_id"               : df_norm["id_venta"].isna() if "id_venta" in df_norm.columns else 0,
        "null_or_invalid_sales_date"  : parsed_fecha.isna() if parsed_fecha is not None else 0,
        "null_or_non_positive_importe": (numeric_importe.isna() | (numeric_importe <= 0)) if numeric_importe is not None else 0,
        "estado not in valid set"     : invalid_estado,
    }

    invalid_rules_count = {}
    
    for rule, mask in rules.items():
        count = int(mask.sum()) if hasattr(mask, "sum") else int(mask)
        invalid_rules_count[rule] = {
            "rows": count,
            "status": "fail" if count > 0 else "pass"
        }
    
    total_rule_violation = sum(
        info["rows"] for info in invalid_rules_count.values()
    )           

    return {
        "rules_checked": list(rules.keys()),
        "total_rows": len(df),
        "total_rules_checked": len(rules),
        "total_rule_violation": total_rule_violation,
        "invalid_rules_count": invalid_rules_count
    }
# ---------------------------------------------------------------------------
# Entry point: complete diagnosis / Punto de entrada: diagnóstico completo
# ---------------------------------------------------------------------------

def run_validation(df: pd.DataFrame, verbose: bool = True) -> dict:
    """
    Executes all validations and returns a dict with the results.
    If verbose=True it prints a summary to the console.
    
    Ejecuta todas las validaciones y devuelve un dict con los resultados.
    Si verbose=True imprime un resumen en consola.
    """
    
    report = {
        "shape": check_shape(df),
        "column_names": check_column_names(df),
        "duplicates_after_normalization": check_duplicates_after_norm(df),
        "missing_values": check_missing_values(df),
        "empty_rows": check_empty_rows(df),
        "sales_id_uniqueness": check_uniqueness(df, " ID Venta "),
        "categorical_validation": check_categorical_values(df, CATEGORICAL_COLS),
        "date_validation": check_date_columns(df, DATE_COLS),
        "numeric_validation": check_numeric_columns(df, NUM_COLS),
        "invalid_business_rules": check_business_rules(df)
    }

    if verbose:
        _print_report(report)

    return report

def _print_report(r: dict):

    print("=" * 52)
    print("  VALIDATION REPORT")
    print("=" * 52)
    
    # Shape
    s = r["shape"]
    print(f"Shape                       : {s['total_rows']} rows x {s['total_columns']} columns")
    
    # Column names
    cn = r["column_names"]
    print(f"Column issues               : {cn['n_issues']} column(s) with name problems")
    for issue in cn["columns_with_issues"]:
        print(f"    ⚠ '{issue['column']}'  →  {', '.join(issue['flags'])}")

    # Duplicates after normalization
    dn = r["duplicates_after_normalization"]
    print(f"Duplicates after norm       : {dn['n_duplicates']}")
    for dup in dn["duplicates"]:
        print(
            f"  ⚠ '{dup['column']}' -> '{dup['normalized']}' "
            f"(conflicts with '{dup['conflicts_with']}')"
        )

    # Missing values
    mv = r["missing_values"]
    print(f"Columns with missing values  : {mv['missing_values_found']}")
    for item in mv["columns_with_missings"]:
        if item["null_count"] > 0:
            print(
                f"  ⚠ {item['column']}: "
                f"{item['null_count']} null(s) ({item['null_percentage']}%)"
            )

    # Empty rows
    er = r["empty_rows"]
    print(f"Empty rows                   : {er['n_empty_rows']}")
    for row in er["empty_rows"]:
        print(f"  ⚠ Empty row index: {row['row_index']}")

    # Uniqueness
    uq = r["sales_id_uniqueness"]
    print(f"Duplicated sales IDs         : {uq['n_duplicates_values']}")
    for item in uq["duplicated_values"]:
        print(f"  ⚠ Value '{item['value']}' repeated {item['repetitions']} times")

    # Categorical validation
    cv = r["categorical_validation"]
    print(f"Categorical columns checked : {cv['total_columns_checked']}")
    if cv["missing_columns"]:
        print(f"  Missing categorical cols  : {', '.join(cv['missing_columns'])}")

    # Date validation
    dv = r["date_validation"]
    print(f"Date columns checked        : {dv['total_columns_checked']}")
    print(f"Total unparseable dates     : {dv['total_unparseable_values']}")
    if dv["missing_columns"]:
        print(f"  Missing date cols         : {', '.join(dv['missing_columns'])}")
    for col, info in dv["date_validation_by_column"].items():
        print(
            f"  {col}: unparseable={info['unparseable_values']}, "
            f"null_original={info['null_original']}, "
            f"null_after_parse={info['null_after_parse']}"
        )

     # Numeric validation
    nv = r["numeric_validation"]
    print(f"Numeric columns checked     : {nv['total_columns_checked']}")
    print(f"Total non-numeric values    : {nv['total_unparseable_values']}")
    print(f"Total zero values           : {nv['total_zero_values']}")
    print(f"Total negative values       : {nv['total_negative_values']}")
    if nv["missing_columns"]:
        print(f"  Missing numeric cols      : {', '.join(nv['missing_columns'])}")
    for col, info in nv["numeric_validation_by_column"].items():
        print(
            f"  {col}: non_numeric={info['unparseable_values']}, "
            f"zeros={info['zero_count']}, negatives={info['negative_count']}, "
            f"valid_positive={info['valid_positive_values']}"
        )

    # Business rules
    br = r["invalid_business_rules"]
    print(f"Business rules checked      : {br['total_rules_checked']}")
    print(f"Total rule violations       : {br['total_rule_violation']}")
    for rule, info in br["invalid_rules_count"].items():
        flag = "⚠" if info["status"] == "fail" else "✓"
        print(f"  {flag} {rule}: {info['rows']} row(s)")

    print("=" * 60)
    
    

    