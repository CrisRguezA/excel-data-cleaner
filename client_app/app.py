# =============================================================================
# Excel Sales Data Cleaner
# -----------------------------------------------------------------------------
# Herramienta de validación y limpieza automática de archivos Excel de ventas.
# Diseñada para equipos que trabajan con una plantilla fija de datos y
# necesitan procesar su archivo periódicamente antes de cualquier análisis.
#
# Automatic validation and cleaning tool for Excel sales data files.
# Designed for teams working with a fixed data template who need to process
# their file periodically before any financial analysis.
# -----------------------------------------------------------------------------
# Autora / Author  : Cristina Rodríguez Arroyo
# Stack            : Python · Streamlit · pandas · openpyxl · xlsxwriter
# Versión / Version: 1.0.0
# =============================================================================

import json
import sys
import time
from pathlib import Path
from io import BytesIO
 
import streamlit as st
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from validation import run_validation
from cleaning import run_cleaning
from main import _json_serializer


# ── Cache functions ────────────────────────────────────────────────────────────

@st.cache_data
def cached_validate(df: pd.DataFrame) -> dict:
    return run_validation(df, verbose=False)

@st.cache_data
def cached_clean(df: pd.DataFrame) -> tuple:
    return run_cleaning(df, verbose=False)



# ── Report helpers ───────────────────────────────────────────────────────────────

def report_to_tables(report: dict) -> dict:
    """Converts the validation report dict into a dict of DataFrames for Excel export."""
   
    tables = {}

    tables["shape"] = pd.DataFrame([report["shape"]])

    tables["column_names_summary"] = pd.DataFrame([{
        "n_issues": report["column_names"]["n_issues"],
        "issues_found": report["column_names"]["issues_found"],
        "total_columns": report["column_names"]["total_columns"]
    }])
    tables["column_name_issues"] = pd.DataFrame(report["column_names"]["columns_with_issues"])

    tables["duplicates_after_norm_summary"] = pd.DataFrame([{
        "total_columns": report["duplicates_after_normalization"]["total_columns"],
        "duplicates_found": report["duplicates_after_normalization"]["duplicates_found"],
        "n_duplicates": report["duplicates_after_normalization"]["n_duplicates"]
    }])
    tables["duplicates_after_norm_details"] = pd.DataFrame(report["duplicates_after_normalization"]["duplicates"])
    tables["duplicates_after_norm_mapping"] = pd.DataFrame(report["duplicates_after_normalization"]["mapping"])

    tables["missing_summary"] = pd.DataFrame([{
        "total_columns": report["missing_values"]["total_columns"],
        "total_rows": report["missing_values"]["total_rows"],
        "missing_values_found": report["missing_values"]["missing_values_found"]
    }])
    tables["missing_details"] = pd.DataFrame(report["missing_values"]["columns_with_missings"])

    tables["empty_rows_summary"] = pd.DataFrame([{
        "total_columns": report["empty_rows"]["total_columns"],
        "total_rows": report["empty_rows"]["total_rows"],
        "empty_rows_found": report["empty_rows"]["empty_rows_found"],
        "n_empty_rows": report["empty_rows"]["n_empty_rows"]
    }])
    tables["empty_rows_details"] = pd.DataFrame(report["empty_rows"]["empty_rows"])

    tables["uniqueness_summary"] = pd.DataFrame([{
        "column": report["sales_id_uniqueness"]["column"],
        "total_rows": report["sales_id_uniqueness"]["total_rows"],
        "duplicates_found": report["sales_id_uniqueness"]["duplicates_found"],
        "n_duplicated_values": report["sales_id_uniqueness"]["n_duplicates_values"],
        "n_rows_with_duplicates": report["sales_id_uniqueness"]["n_rows_with_duplicates"]
    }])
    tables["uniqueness_details"] = pd.DataFrame(report["sales_id_uniqueness"]["duplicated_values"])

    tables["categorical_summary"] = pd.DataFrame([{
        "total_rows": report["categorical_validation"]["total_rows"],
        "total_columns_checked": report["categorical_validation"]["total_columns_checked"],
        "categorical_values_found": report["categorical_validation"]["categorical_values_found"]
    }])
    tables["categorical_columns"] = pd.DataFrame(report["categorical_validation"]["categorical_value_summary"])

    cat_rows = []
    for item in report["categorical_validation"]["categorical_value_summary"]:
        col = item["column"]
        for value, count in item["value_frequencies"].items():
            cat_rows.append({
                "column": col,
                "value": value,
                "count": count
            })
    tables["categorical_frequencies"] = pd.DataFrame(cat_rows)

    tables["date_summary"] = pd.DataFrame([{
        "total_rows": report["date_validation"]["total_rows"],
        "total_columns_checked": report["date_validation"]["total_columns_checked"],
        "total_unparseable_values": report["date_validation"]["total_unparseable_values"]
    }])
    tables["date_by_column"] = pd.DataFrame.from_dict(
        report["date_validation"]["date_validation_by_column"],
        orient="index"
    ).reset_index().rename(columns={"index": "column"})

    tables["numeric_summary"] = pd.DataFrame([{
        "total_rows": report["numeric_validation"]["total_rows"],
        "total_columns_checked": report["numeric_validation"]["total_columns_checked"],
        "total_unparseable_values": report["numeric_validation"]["total_unparseable_values"],
        "total_zero_values": report["numeric_validation"]["total_zero_values"],
        "total_negative_values": report["numeric_validation"]["total_negative_values"]
    }])
    tables["numeric_by_column"] = pd.DataFrame.from_dict(
        report["numeric_validation"]["numeric_validation_by_column"],
        orient="index"
    ).reset_index().rename(columns={"index": "column"})

    tables["rules_summary"] = pd.DataFrame([{
        "total_rows": report["invalid_business_rules"]["total_rows"],
        "total_rules_checked": report["invalid_business_rules"]["total_rules_checked"],
        "total_rule_violations": report["invalid_business_rules"]["total_rule_violation"]
    }])
    tables["rules_by_rule"] = pd.DataFrame.from_dict(
        report["invalid_business_rules"]["invalid_rules_count"],
        orient="index"
    ).reset_index().rename(columns={"index": "rule"})

    return tables

def cleaning_report_to_excel_bytes(report: dict) -> bytes:
    """Exports the cleaning report summary as an Excel file in memory."""
    
    s = report["shape"]
    fr = report["filtered_invalid_rows"]
    
    summary_data = {
        "Metric": [
            "Original rows",
            "Cleaned rows",
            "Rows removed total",
            "Original columns",
            "Cleaned columns",
            "Empty rows removed",
            "Duplicates removed",
            "Certificacion nulls filled",
            "Importe rows recovered",
            "Null importe removed",
            "Zero importe removed",
            "Missing values after clean",
        ],
        "Value": [
            s["original_rows"],
            s["cleaned_rows"],
            s["rows_removed_total"],
            s["original_columns"],
            s["cleaned_columns"],
            report["empty_rows"]["empty_rows_removed"],
            report["duplicates_by_id_venta"]["duplicates_removed"],
            report["certificacion_fill"]["nulls_filled"],
            report["importe_recalculation"]["recovered_rows"],
            fr["breakdown"]["null_importe"],
            fr["breakdown"]["zero_or_neg_importe"],
            report["nulls_after_cleaning"]["total_missing_values"],
        ]
    }
    
    df_summary = pd.DataFrame(summary_data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name="cleaning_summary", index=False)
    
    return output.getvalue()


def export_clean_excel_bytes(df: pd.DataFrame) -> bytes:
    """Exports the clean DataFrame as a formatted Excel file in memory."""
    
    if "id_venta" in df.columns:
        df["id_venta"] = pd.to_numeric(df["id_venta"], errors="coerce")
    
    df = df.where(pd.notna(df), other=None)
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter", datetime_format="DD/MM/YYYY") as writer:
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

        # Headers
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_fmt)

        # Data rows
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

    return output.getvalue()

def validation_summary_table(report: dict) -> pd.DataFrame:
    """Builds a human-readable validation summary with status indicators."""

    def status(has_issues: bool, count: int = 0) -> str:
        if not has_issues:
            return "✅ OK"
        elif count <= 5:
            return "⚠️ Warning"
        else:
            return "🔴 Error"

    rows = [
        {
            "Check": "Shape",
            "Status": "✅ OK",
            "Issues found": "—"
        },
        {
            "Check": "Column names",
            "Status": status(report["column_names"]["issues_found"], report["column_names"]["n_issues"]),
            "Issues found": report["column_names"]["n_issues"] or "—"
        },
        {
            "Check": "Duplicates",
            "Status": status(report["duplicates_after_normalization"]["duplicates_found"], report["duplicates_after_normalization"]["n_duplicates"]),
            "Issues found": report["duplicates_after_normalization"]["n_duplicates"] or "—"
        },
        {
            "Check": "Missing values",
            "Status": status(report["missing_values"]["missing_values_found"], report["missing_values"]["missing_values_found"]),
            "Issues found": report["missing_values"]["missing_values_found"] or "—"
        },
        {
            "Check": "Empty rows",
            "Status": status(report["empty_rows"]["empty_rows_found"], report["empty_rows"]["n_empty_rows"]),
            "Issues found": report["empty_rows"]["n_empty_rows"] or "—"
        },
        {
            "Check": "ID uniqueness",
            "Status": status(report["sales_id_uniqueness"]["duplicates_found"], report["sales_id_uniqueness"]["n_rows_with_duplicates"]),
            "Issues found": report["sales_id_uniqueness"]["n_rows_with_duplicates"] or "—"
        },
        {
            "Check": "Categorical values",
            "Status": status(report["categorical_validation"]["categorical_values_found"]),
            "Issues found": "—" if not report["categorical_validation"]["categorical_values_found"] else "See report"
        },
        {
            "Check": "Date formats",
            "Status": status(report["date_validation"]["total_unparseable_values"] > 0, report["date_validation"]["total_unparseable_values"]),
            "Issues found": report["date_validation"]["total_unparseable_values"] or "—"
        },
        {
            "Check": "Numeric values",
            "Status": status(report["numeric_validation"]["total_unparseable_values"] > 0, report["numeric_validation"]["total_unparseable_values"]),
            "Issues found": report["numeric_validation"]["total_unparseable_values"] or "—"
        },
        {
            "Check": "Business rules",
            "Status": status(report["invalid_business_rules"]["total_rule_violation"] > 0, report["invalid_business_rules"]["total_rule_violation"]),
            "Issues found": report["invalid_business_rules"]["total_rule_violation"] or "—"
        },
    ]

    return pd.DataFrame(rows)


# ── Page config ──────────────────────────────────────────────────

st.set_page_config(
    page_title='SALES DATA',
    page_icon="📊",
    layout='wide',
    initial_sidebar_state = "expanded"
)

if "step" not in st.session_state:
    st.session_state.step = 0


# ── Main app ───────────────────────────────────────────────────────────────────

def main():
    
    st.title('EXCEL SALES DATA CLEANER')
    
    # ES: Esta herramienta está diseñada para limpiar y validar archivos 
    # de datos de ventas con una plantilla fija. (Mensaje informativo para el usuario)
    st.info(
        "📋 This tool is designed to clean and validate sales data files "
        "with a fixed template. Use it every time you need to process "
        "a new file before your periodic financial analysis."
    )
    
    # __ Sidebar - vertical progress bar __________________________________________
    with st.sidebar:
        with st.expander("📖 How to use this app"):
            st.markdown("""
            1. **Upload** your Excel sales file
            2. **Validate** the data to detect errors
            3. **Download** the validation report
            4. **Clean** the data automatically
            5. **Download** the clean file and cleaning report
            """)
    
        st.divider()
        
        st.subheader ("Pipeline progress")
        steps = [
            ("📁 Upload",          0),
            ("🔍 Validate",        1),
            ("📊 Clean & Results", 2)
        ]
        for label, i in steps:
            if st.session_state.step > i:
                st.success(label)
            elif st.session_state.step == i:
                st.info(label)
            else:
                st.empty()   # no muestra nada hasta que llegue
                st.write(f"⬜ {label}")
        
        # ES: Solo compatible con la plantilla de ventas oficial
        st.caption("⚠️ Compatible with the official sales data template only.")
        
    st.divider()
    
       
    # ── Step 1: Upload ────────────────────────────────────────────────────────
    
    st.subheader("1. Load Excel Files")
    
    excel_file = st.file_uploader("Upload your Excel sales file", type=["xlsx"])
    load_dirty_data_button = st.button("Upload File")
    
    if load_dirty_data_button and excel_file:
        st.session_state.df = pd.read_excel(excel_file, dtype=str)
        st.session_state.step = 1
        st.success(f"✅ File loaded: {excel_file.name}")
        st.dataframe(st.session_state.df)
        
    st.divider()
    
    
    # ── Step 2: Validate ──────────────────────────────────────────────────────
            
    if st.session_state.step >= 1:
        st.subheader("Validation")
        
        validate_button = st.button("Validate")
        if validate_button:
            st.session_state.validation_report = cached_validate(st.session_state.df)
            st.session_state.step = 2
        
    if st.session_state.step >= 2: 
        validation_report = cached_validate(st.session_state.df)
        
        # Validation summary table
        st.markdown("### Validation Summary")
        st.dataframe(
            validation_summary_table(validation_report),
            hide_index=True
        )       
        
        output = BytesIO()
        tables = report_to_tables(validation_report)
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for name, df_table in tables.items():
                df_table.to_excel(writer, sheet_name=name[:31], index=False)
        
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        validation_report_name_excel = f"{excel_file.name.split('.')[0]}_validation_{time_stamp}.xlsx"
        validation_report_name_json = f"{excel_file.name.split('.')[0]}_validation_{time_stamp}.json"
       
        col1, col2, col3 = st.columns(3)
        with col1:
            # Load validation report button as an excel file
            st.download_button(
                '📁 Download Validation Report as Excel',
                data=output.getvalue(),
                file_name=validation_report_name_excel,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ) 
                 
        with col2:
            # Load validation report button as an JSON file
            st.download_button(
                '📁 Download Validation Report as JSON',
                data=json.dumps(validation_report, indent=2, ensure_ascii=False, default=_json_serializer),
                file_name=validation_report_name_json,
                mime='application/json'
        ) 
                       
        st.divider()
    
                
    # ── Step 3: Clean & Results ───────────────────────────────────────────────
    if st.session_state.step >= 2:
        st.subheader("Cleaning and Results")
        
        clean_button = st.button("Clean")
        if clean_button:
            df_clean, cleaning_report = cached_clean(st.session_state.df)
            st.session_state.df_clean = df_clean
            st.session_state.cleaning_report = cleaning_report
            st.session_state.step = 3
    
    # Show DataFrame on screen
    if st.session_state.step >= 3:
        st.dataframe(st.session_state.df_clean)    
                
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        cleaning_report_name_excel = f"{excel_file.name.split('.')[0]}_cleaning_{time_stamp}.xlsx"
        cleaning_report_name_json = f"{excel_file.name.split('.')[0]}_cleaning_{time_stamp}.json"
        clean_data_name = f"{excel_file.name.split('.')[0]}_clean_{time_stamp}.xlsx"
        
        col1, col2, col3 = st.columns(3)
        with col1:         
            # Load cleaning report button as an excel file
            st.download_button(
                "📊 Download Clean Data Excel",
                data=export_clean_excel_bytes(st.session_state.df_clean),
                file_name=clean_data_name,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
                      
        with col2:
            # Load cleaning report button as an excel file
            st.download_button(
                '📁 Download Cleaning Report as Excel',
                data=cleaning_report_to_excel_bytes(st.session_state.cleaning_report),
                file_name=cleaning_report_name_excel,
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ) 
             
        with col3:
            # Load validation report button as an JSON file
            st.download_button(
                '📁 Download Cleaning Report as JSON',
                data=json.dumps(st.session_state.cleaning_report, indent=2, ensure_ascii=False, default=_json_serializer),
                file_name=cleaning_report_name_json,
                mime='application/json'
        ) 
        
    
if __name__ == '__main__':
    main()