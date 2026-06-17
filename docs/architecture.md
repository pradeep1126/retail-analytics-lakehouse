# Retail Analytics Lakehouse Platform

## Project Goal

Build a production-grade end-to-end Data Engineering platform using the Instacart Market Basket Analysis dataset.

## Architecture

Source Data
    ↓
Bronze Layer (MinIO)
    ↓
Spark Processing
    ↓
Silver Layer (MinIO)
    ↓
Spark Aggregations
    ↓
Gold Layer (MinIO)
    ↓
Amazon Redshift
    ↓
Amazon QuickSight

## Technologies

- Python
- PySpark
- Apache Airflow
- Docker Compose
- PostgreSQL
- MinIO
- AWS S3
- Amazon Redshift
- Amazon QuickSight
- AWS IAM
- AWS Secrets Manager
- AWS CloudWatch
- AWS SNS

## Data Lake Layers

### Bronze

Raw ingested source data.

### Silver

Cleaned and standardized data.

### Gold

Business-ready analytical datasets.