import logging

import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from spark.silver.exceptions import SilverQualityError
from spark.silver.quality import QualityManager


# --- Fixtures ---
@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("QualityTests").getOrCreate()


@pytest.fixture
def manager():
    return QualityManager(logger=logging.getLogger("test_logger"))


# --- Tests ---
def test_all_validations_pass(spark, manager):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    config = {"required_columns": ["id", "value"], "primary_key": ["id"]}

    report = manager.validate(df, "test_dataset", config)

    assert report.passed is True
    assert report.total_records == 2
    assert report.duplicate_records == 0
    assert report.null_counts == {"id": 0, "value": 0}
    assert len(report.errors) == 0


def test_missing_required_column(spark, manager):
    df = spark.createDataFrame([(1,)], ["id"])
    config = {
        "required_columns": ["id", "missing_col"],
    }

    with pytest.raises(SilverQualityError) as exc_info:
        manager.validate(df, "test_dataset", config)

    assert "Missing required columns: missing_col" in exc_info.value.report.errors


def test_nulls_in_required_columns_fail_validation(spark, manager):
    # Required columns should now strictly fail if they contain nulls
    schema = T.StructType([T.StructField("val", T.StringType(), True)])
    df = spark.createDataFrame([(None,), ("A",)], schema)

    config = {"required_columns": ["val"]}

    with pytest.raises(SilverQualityError) as exc_info:
        manager.validate(df, "test_dataset", config)

    assert exc_info.value.report.passed is False
    assert exc_info.value.report.null_counts["val"] == 1
    assert any(
        "Required column 'val' contains 1 null value(s)" in e for e in exc_info.value.report.errors
    )


def test_nulls_in_primary_key_columns(spark, manager):
    schema = T.StructType(
        [T.StructField("id", T.IntegerType(), True), T.StructField("val", T.StringType(), True)]
    )
    df = spark.createDataFrame([(None, "A"), (2, "B")], schema)

    config = {"primary_key": ["id"]}

    with pytest.raises(SilverQualityError) as exc_info:
        manager.validate(df, "test_dataset", config)

    errors = exc_info.value.report.errors
    assert any("Primary key column 'id' contains 1 null value(s)" in e for e in errors)


def test_duplicate_primary_keys(spark, manager):
    df = spark.createDataFrame([(1, "A"), (1, "B"), (2, "C")], ["id", "val"])
    config = {"primary_key": ["id"]}

    with pytest.raises(SilverQualityError) as exc_info:
        manager.validate(df, "test_dataset", config)

    errors = exc_info.value.report.errors
    assert exc_info.value.report.duplicate_records == 1
    assert any("Found 1 duplicate primary key(s)" in e for e in errors)


def test_multiple_validation_failures_reported_together(spark, manager):
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
        ]
    )
    # 1 null PK, 1 duplicate PK (the 2s), and missing 'val' column
    df = spark.createDataFrame([(None,), (2,), (2,)], schema)

    config = {"required_columns": ["id", "val"], "primary_key": ["id"]}

    with pytest.raises(SilverQualityError) as exc_info:
        manager.validate(df, "test_dataset", config)

    errors = exc_info.value.report.errors
    assert len(errors) == 3
    assert any("Missing required columns: val" in e for e in errors)
    assert any(
        "Required column 'id' contains 1 null value(s)" in e for e in errors
    )  # Checked as required first
    assert any("Found 1 duplicate primary key(s)" in e for e in errors)


def test_quality_report_fields_populated_correctly(spark, manager):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
    config = {"required_columns": ["id"], "primary_key": ["id"]}

    report = manager.validate(df, "orders", config)

    assert report.dataset_name == "orders"
    assert report.total_records == 2
    assert report.duplicate_records == 0
    assert report.null_counts == {"id": 0}
    assert report.passed is True
    assert report.errors == []
