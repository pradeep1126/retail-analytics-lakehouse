import os

from pyspark.sql import SparkSession

from spark.ingestion.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create or return a configured SparkSession for the Retail Analytics
    Lakehouse Platform.

    The Spark session is configured for:
    - MinIO (S3A)
    - Adaptive Query Execution
    - Dynamic partition overwrite
    - Local Spark execution inside Docker
    """

    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ROOT_USER"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_ROOT_PASSWORD"),
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        )
        .config(
            "spark.driver.memory",
            os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        )
        .config(
            "spark.executor.memory",
            os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        )
        .getOrCreate()
    )

    logger.info("=" * 60)
    logger.info("Spark Session Started")
    logger.info(f"Application : {app_name}")
    logger.info(f"Spark Version : {spark.version}")
    logger.info("=" * 60)

    return spark
