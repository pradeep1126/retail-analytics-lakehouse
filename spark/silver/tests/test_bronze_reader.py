"""
Unit tests for spark.silver.readers.BronzeReader.

PySpark is not installed in this environment, so the entire Spark surface is
mocked using unittest.mock.  Three distinct mock helpers cover the three
things Spark does that the reader cares about:

    _make_spark(df)       → SparkSession whose .read.format().load() returns df
    _make_df()            → DataFrame with schema stubbed for debug log assertions
    _make_failing_spark() → SparkSession whose .read.format().load() raises

This is intentionally minimal: each helper mocks only the call chain the
reader actually exercises, so a test failure points directly at the broken
behaviour rather than at a misconfigured mock.
"""

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Stub out pyspark so imports in readers.py that may transitively reference
# it don't blow up.  (readers.py itself doesn't import pyspark, but keeping
# this stub here makes the test file safe even if that changes later.)
# ---------------------------------------------------------------------------
def _stub_pyspark():
    pyspark = types.ModuleType("pyspark")
    pyspark.sql = types.ModuleType("pyspark.sql")
    pyspark.sql.SparkSession = object
    pyspark.sql.DataFrame = object
    sys.modules.setdefault("pyspark", pyspark)
    sys.modules.setdefault("pyspark.sql", pyspark.sql)


_stub_pyspark()

from spark.silver.exceptions import SilverReadError  # noqa: E402
from spark.silver.readers import _SUPPORTED_FORMATS, BronzeReader  # noqa: E402

# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------


def _make_df() -> MagicMock:
    """Return a mock DataFrame with schema stubbed for the debug log test."""
    df = MagicMock(name="DataFrame")
    df.schema.simpleString.return_value = "struct<id:int,name:string>"
    return df


def _make_spark(df: MagicMock) -> MagicMock:
    """
    Return a mock SparkSession that serves *df* from
    spark.read.format(fmt).load(path).
    """
    spark = MagicMock(name="SparkSession")
    spark.read.format.return_value.load.return_value = df
    return spark


def _make_failing_spark(exc: Exception) -> MagicMock:
    """Return a SparkSession whose .read.format().load() raises *exc*."""
    spark = MagicMock(name="SparkSession")
    spark.read.format.return_value.load.side_effect = exc
    return spark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _full_config(**overrides) -> dict:
    """Return a complete dataset config, optionally overriding keys."""
    base = {
        "bronze_path": "s3://lake/bronze/orders/",
        "silver_path": "s3://lake/silver/orders/",
        "file_format": "parquet",
        "write_format": "delta",
        "write_mode": "overwrite",
        "compression": "snappy",
        "partition_columns": ["order_date"],
        "primary_key": ["order_id"],
        "required_columns": ["order_id", "customer_id"],
        "drop_duplicates": True,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1. Successful reads
# ---------------------------------------------------------------------------


class TestSuccessfulRead(unittest.TestCase):

    def test_parquet_read_returns_dataframe(self):
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        result = reader.read(_full_config(file_format="parquet"))
        self.assertIs(result, df)

    def test_csv_read_returns_dataframe(self):
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        result = reader.read(_full_config(file_format="csv"))
        self.assertIs(result, df)

    def test_parquet_calls_correct_spark_format(self):
        df = _make_df()
        spark = _make_spark(df)
        BronzeReader(spark).read(_full_config(file_format="parquet"))
        spark.read.format.assert_called_once_with("parquet")

    def test_csv_calls_correct_spark_format(self):
        df = _make_df()
        spark = _make_spark(df)
        BronzeReader(spark).read(_full_config(file_format="csv"))
        spark.read.format.assert_called_once_with("csv")

    def test_load_called_with_bronze_path(self):
        df = _make_df()
        spark = _make_spark(df)
        path = "s3://lake/bronze/orders/"
        BronzeReader(spark).read(_full_config(bronze_path=path))
        spark.read.format.return_value.load.assert_called_once_with(path)

    def test_count_is_never_called(self):
        """Reader must not trigger a Spark action — count() belongs in the processor."""
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        reader.read(_full_config())
        df.count.assert_not_called()

    def test_file_format_is_case_insensitive(self):
        """'PARQUET', 'Parquet', 'parquet' should all work."""
        for fmt in ("PARQUET", "Parquet", "  parquet  "):
            with self.subTest(fmt=fmt):
                df = _make_df()
                reader = BronzeReader(_make_spark(df))
                result = reader.read(_full_config(file_format=fmt))
                self.assertIs(result, df)


# ---------------------------------------------------------------------------
# 2. Unsupported file format
# ---------------------------------------------------------------------------


class TestUnsupportedFormat(unittest.TestCase):

    def test_raises_for_unsupported_format(self):
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError):
            reader.read(_full_config(file_format="avro"))

    def test_error_message_names_the_format(self):
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(_full_config(file_format="avro"))
        self.assertIn("avro", str(ctx.exception))

    def test_error_message_lists_supported_formats(self):
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(_full_config(file_format="avro"))
        msg = str(ctx.exception)
        for fmt in sorted(_SUPPORTED_FORMATS.keys()):
            self.assertIn(fmt, msg)

    def test_spark_read_is_never_called_for_unsupported_format(self):
        spark = MagicMock(name="SparkSession")
        reader = BronzeReader(spark)
        with self.assertRaises(SilverReadError):
            reader.read(_full_config(file_format="avro"))
        spark.read.format.assert_not_called()

    @unittest.skipIf(not _SUPPORTED_FORMATS, "No formats registered — nothing to test against.")
    def test_every_registered_format_is_accepted(self):
        """Regression guard: every key in _SUPPORTED_FORMATS must not raise."""
        for fmt in _SUPPORTED_FORMATS:
            with self.subTest(fmt=fmt):
                df = _make_df()
                reader = BronzeReader(_make_spark(df))
                result = reader.read(_full_config(file_format=fmt))
                self.assertIs(result, df)


# ---------------------------------------------------------------------------
# 3. Missing required config keys
# ---------------------------------------------------------------------------


class TestMissingConfigKeys(unittest.TestCase):

    def test_raises_when_bronze_path_missing(self):
        cfg = _full_config()
        del cfg["bronze_path"]
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError):
            reader.read(cfg)

    def test_error_names_bronze_path(self):
        cfg = _full_config()
        del cfg["bronze_path"]
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(cfg)
        self.assertIn("bronze_path", str(ctx.exception))

    def test_raises_when_file_format_missing(self):
        cfg = _full_config()
        del cfg["file_format"]
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError):
            reader.read(cfg)

    def test_error_names_file_format(self):
        cfg = _full_config()
        del cfg["file_format"]
        reader = BronzeReader(MagicMock())
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(cfg)
        self.assertIn("file_format", str(ctx.exception))

    def test_spark_is_never_called_when_key_missing(self):
        cfg = _full_config()
        del cfg["bronze_path"]
        spark = MagicMock(name="SparkSession")
        BronzeReader(spark)
        try:
            BronzeReader(spark).read(cfg)
        except SilverReadError:
            pass
        spark.read.format.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Spark read exception is wrapped in SilverReadError
# ---------------------------------------------------------------------------


class TestSparkReadFailure(unittest.TestCase):

    def _spark_exc(self, msg="Simulated Spark error"):
        return RuntimeError(msg)

    def test_raises_silver_read_error_on_spark_failure(self):
        spark = _make_failing_spark(self._spark_exc())
        reader = BronzeReader(spark)
        with self.assertRaises(SilverReadError):
            reader.read(_full_config())

    def test_original_exception_is_chained(self):
        original = self._spark_exc("S3 access denied")
        spark = _make_failing_spark(original)
        reader = BronzeReader(spark)
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(_full_config())
        self.assertIs(ctx.exception.__cause__, original)

    def test_error_message_includes_path(self):
        spark = _make_failing_spark(self._spark_exc())
        reader = BronzeReader(spark)
        path = "s3://lake/bronze/orders/"
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(_full_config(bronze_path=path))
        self.assertIn(path, str(ctx.exception))

    def test_error_message_includes_format(self):
        spark = _make_failing_spark(self._spark_exc())
        reader = BronzeReader(spark)
        with self.assertRaises(SilverReadError) as ctx:
            reader.read(_full_config(file_format="parquet"))
        self.assertIn("parquet", str(ctx.exception))

    def test_any_exception_type_is_wrapped(self):
        """Reader must catch all Exception subclasses, not just RuntimeError."""
        for exc_type in (ValueError, IOError, PermissionError, Exception):
            with self.subTest(exc_type=exc_type.__name__):
                spark = _make_failing_spark(exc_type("boom"))
                reader = BronzeReader(spark)
                with self.assertRaises(SilverReadError):
                    reader.read(_full_config())


# ---------------------------------------------------------------------------
# 5. Logging
# ---------------------------------------------------------------------------


class TestLogging(unittest.TestCase):

    def test_info_logged_on_successful_read(self):
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        with patch("spark.silver.readers.logger") as mock_log:
            reader.read(_full_config(bronze_path="s3://b/orders/"))
        # One info call before the read, one after — no count logged
        self.assertEqual(mock_log.info.call_count, 2)

    def test_debug_logged_for_schema(self):
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        with patch("spark.silver.readers.logger") as mock_log:
            reader.read(_full_config())
        mock_log.debug.assert_called_once()
        args = mock_log.debug.call_args[0]
        self.assertIn("Schema", args[0])

    def test_path_appears_in_log(self):
        df = _make_df()
        reader = BronzeReader(_make_spark(df))
        path = "s3://lake/bronze/orders/"
        with patch("spark.silver.readers.logger") as mock_log:
            reader.read(_full_config(bronze_path=path))
        all_calls = " ".join(str(c) for c in mock_log.info.call_args_list)
        self.assertIn(path, all_calls)


# ---------------------------------------------------------------------------
# 6. Reader has no side effects
# ---------------------------------------------------------------------------


class TestNoSideEffects(unittest.TestCase):

    def test_read_does_not_mutate_dataset_config(self):
        cfg = _full_config()
        original_keys = set(cfg.keys())
        df = _make_df()
        BronzeReader(_make_spark(df)).read(cfg)
        self.assertEqual(set(cfg.keys()), original_keys)

    def test_multiple_reads_return_independent_dataframes(self):
        df1 = _make_df()
        df2 = _make_df()
        spark = MagicMock(name="SparkSession")
        spark.read.format.return_value.load.side_effect = [df1, df2]
        reader = BronzeReader(spark)
        result1 = reader.read(_full_config())
        result2 = reader.read(_full_config())
        self.assertIs(result1, df1)
        self.assertIs(result2, df2)


if __name__ == "__main__":
    unittest.main()
