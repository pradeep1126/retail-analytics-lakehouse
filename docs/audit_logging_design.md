# Audit Logging Design

## Purpose

The audit logging framework captures execution-level information for every ingestion run.

Audit logs help answer operational questions such as:

* Did the ingestion run successfully?
* When did the ingestion start and finish?
* How long did the ingestion take?
* How many records were processed?
* Why did the ingestion fail?
* Which dataset was affected?

Audit logs are primarily used by Data Engineers, Operations teams, and Airflow monitoring processes.

---

## Metadata vs Audit Logging

### Metadata

Metadata describes the data that was ingested.

Examples:

* File name
* Row count
* Column count
* File size
* Load date

Question answered:

```text
What data was ingested?
```

---

### Audit Logging

Audit logs describe the execution of the ingestion process.

Examples:

* Start time
* End time
* Status
* Error message
* Duration

Question answered:

```text
What happened during the ingestion process?
```

---

## Audit Storage Location

```text
bronze/audit/ingestion_audit.parquet
```

The audit dataset will be stored in Parquet format for efficient querying and reporting.

---

## Audit Schema

| Column Name       | Data Type | Description                                    |
| ----------------- | --------- | ---------------------------------------------- |
| run_id            | STRING    | Unique ingestion execution identifier          |
| dataset_name      | STRING    | Dataset being processed                        |
| start_time        | TIMESTAMP | Ingestion start timestamp                      |
| end_time          | TIMESTAMP | Ingestion completion timestamp                 |
| duration_seconds  | INTEGER   | Total execution duration                       |
| status            | STRING    | SUCCESS or FAILED                              |
| records_processed | BIGINT    | Number of records processed                    |
| file_name         | STRING    | Processed source file                          |
| error_message     | STRING    | Failure reason, NULL for successful executions |

---

## Example Success Record

| Column            | Example Value       |
| ----------------- | ------------------- |
| run_id            | manual_20260617_001 |
| dataset_name      | orders              |
| start_time        | 2026-06-17 19:00:00 |
| end_time          | 2026-06-17 19:00:45 |
| duration_seconds  | 45                  |
| status            | SUCCESS             |
| records_processed | 3421083             |
| file_name         | orders.csv          |
| error_message     | NULL                |

---

## Example Failure Record

| Column            | Example Value       |
| ----------------- | ------------------- |
| run_id            | manual_20260617_002 |
| dataset_name      | orders              |
| start_time        | 2026-06-17 19:05:00 |
| end_time          | 2026-06-17 19:05:03 |
| duration_seconds  | 3                   |
| status            | FAILED              |
| records_processed | 0                   |
| file_name         | orders.csv          |
| error_message     | File not found      |

---

## Audit Validation Rules

The ingestion framework should enforce the following:

### Required Fields

* run_id
* dataset_name
* start_time
* status
* file_name

### Status Values

Allowed values:

```text
SUCCESS
FAILED
```

### Duration Validation

```text
duration_seconds >= 0
```

### Failure Validation

If status = FAILED:

```text
error_message must not be NULL
```

---

## Future Enhancements

The audit schema can be extended in later phases to include:

* airflow_dag_id
* airflow_task_id
* retry_count
* host_name
* environment
* processing_stage
* notification_status

These fields are intentionally excluded from the initial implementation to keep the framework simple while remaining production-oriented.

---

## Design Principles

1. Capture every ingestion attempt.
2. Record both successful and failed executions.
3. Maintain historical audit records.
4. Enable operational monitoring.
5. Support future Airflow integration.
6. Provide sufficient information for troubleshooting.
