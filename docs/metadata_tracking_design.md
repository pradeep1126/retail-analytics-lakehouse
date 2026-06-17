# Metadata Tracking Design

## Purpose

The metadata tracking layer captures information about every dataset ingestion into the Bronze layer.

Metadata enables:

* Data lineage
* Operational monitoring
* Data quality validation
* Auditability
* Troubleshooting
* Historical ingestion analysis

The metadata repository acts as the system of record for all ingestion events.

---

## Metadata Storage Location

```text
bronze/metadata/ingestion_metadata.parquet
```

The metadata dataset will be stored in Parquet format to support efficient querying and reporting.

---

## Metadata Schema

| Column Name         | Data Type | Description                                                |
| ------------------- | --------- | ---------------------------------------------------------- |
| dataset_name        | STRING    | Logical dataset name (orders, products, aisles, etc.)      |
| source_file_name    | STRING    | Original source file name                                  |
| source_file_path    | STRING    | Source file location                                       |
| file_size_bytes     | BIGINT    | Source file size in bytes                                  |
| row_count           | BIGINT    | Total number of records in the source file                 |
| column_count        | INTEGER   | Number of columns in the dataset                           |
| column_names        | STRING    | Comma-separated list of source columns                     |
| load_date           | DATE      | Bronze partition date                                      |
| ingestion_timestamp | TIMESTAMP | Timestamp when ingestion occurred                          |
| checksum            | STRING    | Hash value of the source file used for duplicate detection |
| run_id              | STRING    | Unique identifier for the ingestion execution              |

---

## Example Metadata Record

| Column              | Example Value                                                                             |
| ------------------- | ----------------------------------------------------------------------------------------- |
| dataset_name        | orders                                                                                    |
| source_file_name    | orders.csv                                                                                |
| source_file_path    | data/source/archive/orders.csv                                                            |
| file_size_bytes     | 108968645                                                                                 |
| row_count           | 3421083                                                                                   |
| column_count        | 7                                                                                         |
| column_names        | order_id,user_id,eval_set,order_number,order_dow,order_hour_of_day,days_since_prior_order |
| load_date           | 2026-06-17                                                                                |
| ingestion_timestamp | 2026-06-17 19:15:23                                                                       |
| checksum            | 8d7f4f12b8e12d7a4f8d123456789abc                                                          |
| run_id              | manual_20260617_001                                                                       |

---

## Metadata Validation Rules

The ingestion framework should validate the following before writing metadata:

### Required Fields

* dataset_name
* source_file_name
* row_count
* load_date
* ingestion_timestamp
* run_id

### Validation Checks

* row_count must be greater than or equal to zero
* column_count must be greater than zero
* file_size_bytes must be greater than zero
* checksum must not be null
* dataset_name must be a known dataset

---

## Future Enhancements

The schema can be extended in future phases to include:

* ingestion_status
* schema_version
* source_system
* processing_duration_seconds
* data_quality_score
* record_rejection_count

These fields are intentionally excluded from the initial version to keep the metadata framework simple while remaining production-oriented.

---

## Design Principles

1. Capture sufficient information for troubleshooting.
2. Maintain full ingestion history.
3. Support duplicate load detection.
4. Enable lineage between source files and Bronze datasets.
5. Support future Airflow orchestration and monitoring.
6. Remain storage-efficient and query-friendly.
