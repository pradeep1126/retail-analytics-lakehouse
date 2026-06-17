# Bronze Layer Design

## Purpose

The Bronze layer is the raw ingestion layer of the lakehouse.

Its primary objectives are:

* Preserve source data in its original format
* Maintain ingestion history
* Support replay and backfill operations
* Enable auditability and traceability
* Provide a reliable source for Silver layer processing

The Bronze layer should remain as close as possible to the source system and should not contain business transformations.

---

## Bronze Storage Layout

```text
bronze/
│
├── aisles/
│   └── load_date=YYYY-MM-DD/
│       └── aisles.csv
│
├── departments/
│   └── load_date=YYYY-MM-DD/
│       └── departments.csv
│
├── products/
│   └── load_date=YYYY-MM-DD/
│       └── products.csv
│
├── orders/
│   └── load_date=YYYY-MM-DD/
│       └── orders.csv
│
├── order_products_prior/
│   └── load_date=YYYY-MM-DD/
│       └── order_products__prior.csv
│
├── order_products_train/
│   └── load_date=YYYY-MM-DD/
│       └── order_products__train.csv
│
├── metadata/
│   └── ingestion_metadata.parquet
│
└── audit/
    └── ingestion_audit.parquet
```

---

## Why Bronze Stores Raw CSV

The source system delivers data as CSV files.

The Bronze layer preserves the source data exactly as received to ensure:

* Data lineage
* Auditability
* Reproducibility
* Recovery from downstream processing issues

Storing raw files allows the team to reprocess data without requiring access to the original source system.

Data format conversion will occur in the Silver layer.

---

## Why Load Date Partitioning Is Used

Each ingestion run writes data into a separate load_date partition.

Example:

```text
bronze/orders/load_date=2026-06-17/orders.csv
```

Benefits:

* Maintains ingestion history
* Supports replay and backfill operations
* Prevents accidental overwrites
* Simplifies troubleshooting and auditing
* Enables future incremental loading patterns

---

## Metadata Layer

Metadata captures ingestion statistics and file-level information.

Example fields:

* dataset_name
* source_file_name
* file_size_bytes
* row_count
* column_count
* load_date
* ingestion_timestamp

Metadata enables:

* Operational monitoring
* Data validation
* Data lineage
* Ingestion reporting

Metadata will be stored in:

```text
bronze/metadata/ingestion_metadata.parquet
```

---

## Audit Layer

Audit logs capture execution details for every ingestion run.

Example fields:

* run_id
* dataset_name
* status
* start_time
* end_time
* duration_seconds
* error_message

Audit data enables:

* Pipeline monitoring
* Failure investigation
* SLA tracking
* Operational reporting

Audit records will be stored in:

```text
bronze/audit/ingestion_audit.parquet
```

---

## Design Principles

The Bronze layer follows the following principles:

1. Raw and immutable storage.
2. No business transformations.
3. Full ingestion history retention.
4. Source-system traceability.
5. Support for replay and backfill.
6. Separation of data, metadata, and audit information.
7. Cloud-native object storage design compatible with MinIO and Amazon S3.
