# Azure Data Engineering Pipeline — Bronze to Gold

## Project Overview
End-to-end batch data pipeline built on Microsoft Azure using the 
Medallion Architecture (Bronze → Silver → Gold).

## Architecture

HTTP Source (airtravel.csv)
↓
Azure Data Factory (Copy Activity)
↓
ADLS Gen2 — Bronze Layer (raw CSV)
↓
Databricks + PySpark (transformation)
↓
Silver Layer (clean Parquet)
↓
Gold Layer (business-ready Parquet)

## Pipeline Screenshots

### ADF Pipeline — Succeeded
![ADF Pipeline](screenshots/adf_pipeline_success.png)

### Bronze Layer — Data Landed in ADLS Gen2
![Bronze Layer](screenshots/bronze_layer.png)

### Gold Layer — Final Output in Databricks
![Gold Layer](screenshots/gold_layer.png)


## Tech Stack
| Tool | Purpose |
|---|---|
| Azure Data Factory | Ingestion — HTTP to ADLS Gen2 |
| ADLS Gen2 | Data lake storage — Bronze/Silver/Gold |
| Databricks (PySpark) | Transformation and aggregation |
| Parquet | Columnar storage format for Silver and Gold |
| Unity Catalog | Volume management in Databricks |

## Pipeline Steps

### Bronze Layer
- ADF Copy Activity pulls raw CSV from HTTP source
- Lands data as-is into `nyctaxi/bronze/` in ADLS Gen2
- No transformations — raw data preserved exactly as received

### Silver Layer
- PySpark reads raw CSV from Bronze
- Schema inferred automatically
- Null rows removed with `dropna()`
- Column names cleaned using `withColumnRenamed()`
- Written to Silver as Parquet format

### Gold Layer
- Silver Parquet read back into Databricks
- Columns cast to correct data types (double → integer)
- Business-ready table with clean column names
- Written to Gold as Parquet — ready for reporting

## Dataset
- **Source:** International Airline Passengers dataset
- **Columns:** Month, passengers_1958, passengers_1959, passengers_1960
- **Rows:** 12 (one per month)

## Key Learnings
- Medallion architecture (Bronze/Silver/Gold)
- ADF Linked Services, Datasets, Copy Activity
- PySpark transformations: read, dropna, withColumnRenamed, cast, write
- Parquet format vs CSV
- Unity Catalog Volumes in Databricks Free Edition

## Author
SrilekhaEmma — Azure Data Engineering learner
