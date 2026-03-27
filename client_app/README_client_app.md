# 📊 Excel Sales Data Cleaner

**Herramienta de validación y limpieza automática de datos de ventas en Excel**, diseñada para equipos que trabajan con una plantilla fija y necesitan preparar sus datos antes de cualquier análisis.

**Automatic validation and cleaning tool for Excel sales data**, designed for teams working with a fixed template who need reliable, analysis-ready data.

---

## Demo

Example workflow from raw input to clean output:
 
![Pipeline](../docs/screenshots/pipeline.png)  
![Load](../docs/screenshots/load.png)
![Validation](../docs/screenshots/validation.png)
![Clean Results](../docs/screenshots/results.png)  

---

## 🚀 ¿Qué hace esta aplicación? / What does this app do?

Permite a cualquier usuario — sin conocimientos técnicos —:

1. 📁 **Subir** un archivo Excel de ventas  
2. 🔍 **Validar** errores y problemas de calidad de datos  
3. 🧹 **Limpiar** los datos automáticamente  
4. 📥 **Descargar** archivos listos para análisis  

It enables any user — without programming knowledge — to:

1. 📁 Upload a sales Excel file  
2. 🔍 Validate data quality issues  
3. 🧹 Automatically clean the dataset  
4. 📥 Download analysis-ready outputs  

---

## ⚠️ Requisito importante / Important requirement

La aplicación es **compatible únicamente con la plantilla oficial de datos de ventas**.

This app is **only compatible with the official sales data template**.

👉 Si el archivo no sigue la estructura esperada:
- aparecerán errores en validación  
- algunos procesos de limpieza pueden fallar  

👉 If the file does not match the expected structure:
- validation errors will appear  
- some cleaning steps may fail  

---

## 🧩 Funcionalidades / Features

| Sección / Section | Descripción / Description |
|-------------------|--------------------------|
| 📁 **Upload** | Carga el archivo Excel de ventas / Load the Excel sales file |
| 🔍 **Validate** | Detecta errores, duplicados, valores nulos y reglas incumplidas / Detects errors, duplicates, null values and rule violations |
| 📥 **Download Validation Report** | Exporta el informe en Excel o JSON / Export validation report as Excel or JSON |
| 🧹 **Clean** | Limpia y filtra los datos automáticamente / Automatically cleans and filters the data |
| 📥 **Download Results** | Exporta datos limpios y reporte de limpieza / Export clean data and cleaning report |

---

## 🔍 ¿Qué se valida? / What gets validated?

- Estructura del dataset (filas y columnas)  
- Nombres de columnas inconsistentes  
- Duplicados tras normalización  
- Valores nulos por columna  
- Filas completamente vacías  
- Unicidad del identificador `id_venta`  
- Categorías inesperadas  
- Formatos de fecha incorrectos  
- Valores numéricos inválidos  
- Reglas de negocio específicas  

---

## 🧹 ¿Qué se limpia? / What gets cleaned?

- Eliminación de filas vacías  
- Eliminación de duplicados por `id_venta`  
- Relleno de valores nulos en `certificacion`  
- Recalculo de `importe` cuando es posible  
- Filtrado de registros inválidos (nulos, cero o negativos en `importe`)  

---

## 📦 Archivos de salida / Output Files

| Archivo / File | Contenido / Content |
|----------------|---------------------|
| `*_validation_*.xlsx` | Informe de validación |
| `*_validation_*.json` | Informe completo en bruto |
| `*_clean_*.xlsx` | Dataset limpio formateado |
| `*_cleaning_*.xlsx` | Resumen de limpieza |
| `*_cleaning_*.json` | Informe de limpieza completo |

---

## 🖥️ Uso básico / Quick Usage

```bash
# Activar entorno virtual
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac / Linux

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar la aplicación
streamlit run client_app/app.py
```

---

## 📁 Estructura del proyecto / Project Structure

```
excel-data-cleaner/
│
├── client_app/
│   ├── app.py
│   └── README_client_app.md
│
├── src/
│   ├── validation.py
│   ├── cleaning.py
│   └── main.py
│
├── data/
└── outputs/
```

---

## 📝 Notas

- Los archivos de salida incluyen timestamp (`YYYYMMDD-HHMMSS`)  
- La app procesa **un archivo por sesión**  
- Recargar la página reinicia el flujo  
- Los datos no se almacenan de forma permanente  

---

## 👤 Autora / Author

**Cristina Rodríguez Arroyo**  
Data Engineer · AI & Data Science  
GitHub: https://github.com/CrisRguezA

---

## 💡 Contexto del proyecto / Project Context

Aplicación desarrollada como **simulación de entregable profesional** en un flujo de datos real:

- validación de calidad de datos  
- limpieza automatizada  
- generación de outputs listos para negocio  

This application was developed as a **realistic client-facing deliverable**, focused on:

- data quality validation  
- automated cleaning pipelines  
- business-ready outputs  
