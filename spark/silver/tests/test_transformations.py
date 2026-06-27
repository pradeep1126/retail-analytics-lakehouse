import logging

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from spark.silver.transformations import (
    TRANSFORMATION_REGISTRY,
    SilverTransformationError,
    TransformationManager,
)


# --- Fixtures ---
@pytest.fixture(scope="session")
def spark():
    """Creates a local Spark session for testing."""
    return SparkSession.builder.master("local[1]").appName("TransformationTests").getOrCreate()


@pytest.fixture
def manager(caplog):
    """Provides a TransformationManager instance and captures logs."""
    caplog.set_level(logging.INFO)
    # Passing a standard logger for tests since get_logger might need env config
    return TransformationManager(logger=logging.getLogger("test_logger"))


# --- Tests ---
def test_string_columns_are_trimmed_and_numeric_untouched(spark, manager):
    data = [(" Banana ", 10), ("Apple", 20)]
    schema = T.StructType(
        [
            T.StructField("product_name", T.StringType(), True),
            T.StructField("price", T.IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    result_df = manager._trim_string_columns(df, "dummy_dataset", {})
    result_data = result_df.collect()

    # Assert string is trimmed
    assert result_data[0]["product_name"] == "Banana"
    # Assert numeric is untouched
    assert result_data[0]["price"] == 10


def test_column_names_are_standardized(spark, manager):
    data = [("test",)]
    schema = T.StructType([T.StructField("Product Name", T.StringType(), True)])
    df = spark.createDataFrame(data, schema)

    result_df = manager._standardize_column_names(df, "dummy_dataset", {})

    assert result_df.columns == ["product_name"]


def test_duplicates_removed_when_enabled(spark, manager, caplog):
    data = [(1, "A"), (1, "B"), (2, "C")]
    df = spark.createDataFrame(data, ["order_id", "val"])

    config = {"drop_duplicates": True, "primary_key": ["order_id"]}

    result_df = manager._drop_duplicates(df, "dummy_dataset", config)

    assert result_df.count() == 2
    assert "duplicates removed (only if enabled)" in caplog.text


def test_duplicates_not_removed_when_disabled(spark, manager, caplog):
    data = [(1, "A"), (1, "B"), (2, "C")]
    df = spark.createDataFrame(data, ["order_id", "val"])

    config = {"drop_duplicates": False, "primary_key": ["order_id"]}

    result_df = manager._drop_duplicates(df, "dummy_dataset", config)

    assert result_df.count() == 3
    assert "duplicates removed (only if enabled)" not in caplog.text


def test_dataset_specific_transformation_invoked(spark, manager, caplog):
    # Temporarily register a mock function to verify invocation
    def mock_transform(df, config):
        return df.withColumn("custom_col", F.lit(True))

    TRANSFORMATION_REGISTRY["test_ds"] = mock_transform

    df = spark.createDataFrame([(1,)], ["id"])
    result_df = manager._apply_dataset_specific_logic(df, "test_ds", {})

    assert "custom_col" in result_df.columns
    assert "dataset-specific transformation executed" in caplog.text

    # Cleanup
    del TRANSFORMATION_REGISTRY["test_ds"]


def test_unknown_dataset_executes_noop(spark, manager, caplog):
    df = spark.createDataFrame([(1,)], ["id"])
    result_df = manager._apply_dataset_specific_logic(df, "unknown_ds", {})

    # DataFrame should be unchanged
    assert result_df.columns == ["id"]
    # Log shouldn't appear
    assert "dataset-specific transformation executed" not in caplog.text


def test_original_dataframe_is_not_mutated(spark, manager):
    """
    Spark DataFrames are immutable. We test this by ensuring the original
    DataFrame reference yields the same un-transformed data after passing
    it through the manager.
    """
    data = [(" Banana ", 10, 10)]
    df = spark.createDataFrame(data, ["Product Name", "id", "id"])

    config = {"drop_duplicates": True, "primary_key": ["id"]}

    # Run through full pipeline
    result_df = manager.apply_transformations(df, "order_products__prior", config)

    # The new dataframe is changed
    assert result_df.columns[0] == "product_name"
    assert result_df.collect()[0]["product_name"] == "Banana"

    # The original dataframe remains completely unchanged
    assert df.columns[0] == "Product Name"
    assert df.collect()[0]["Product Name"] == " Banana "


def test_exception_wrapping_and_reraising(spark, manager):
    df = spark.createDataFrame([(1,)], ["id"])

    # 1. Force an unexpected failure (passing None will break Spark's schema resolution)
    with pytest.raises(SilverTransformationError) as exc_info:
        manager.apply_transformations(None, "orders", {})
    assert "Transformation step failed" in str(exc_info.value)

    # 2. Test that existing SilverErrors are re-raised, not double-wrapped
    def fail_transform(df, config):
        raise SilverTransformationError("Already wrapped exception")

    TRANSFORMATION_REGISTRY["failing_ds"] = fail_transform

    with pytest.raises(SilverTransformationError) as exc_info_2:
        manager.apply_transformations(df, "failing_ds", {})

    assert str(exc_info_2.value) == "Already wrapped exception"
    assert "Transformation step failed:" not in str(exc_info_2.value)

    # Cleanup
    del TRANSFORMATION_REGISTRY["failing_ds"]
