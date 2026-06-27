"""
Bronze Reader.

Sole responsibility: produce a Spark DataFrame from Bronze storage.
No transformations, no schema enforcement, no quality checks, no writes.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from spark.silver.exceptions import SilverReadError

logger = logging.getLogger(__name__)

# Formats the reader knows how to handle.
# Each key is the normalised lowercase token that may appear in config;
# the value is the Spark format string passed to DataFrameReader.format().
_SUPPORTED_FORMATS: dict[str, str] = {
    "csv": "csv",
    "parquet": "parquet",
}


class BronzeReader:
    """
    Reads a Bronze dataset into a Spark DataFrame.

    Parameters
    ----------
    spark:
        An active ``SparkSession``.  Injected so the caller controls the
        session lifecycle and tests can substitute a mock without patching
        any internals.

    Raises
    ------
    SilverReadError
        Propagated from :meth:`read` on any read failure.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(self, dataset_config: dict[str, Any]) -> DataFrame:
        """
        Read Bronze data and return a Spark DataFrame.

        Parameters
        ----------
        dataset_config:
            A validated dataset configuration dict as returned by
            ``SilverConfig.get_dataset_config``.  Must contain at minimum
            ``bronze_path`` and ``file_format``.

        Returns
        -------
        pyspark.sql.DataFrame

        Raises
        ------
        SilverReadError
            If ``bronze_path`` or ``file_format`` are absent, if the format
            is not supported, or if Spark raises any exception during the read.
        """
        bronze_path, fmt = self._extract_and_validate(dataset_config)
        return self._read_dataframe(bronze_path, fmt)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_and_validate(self, dataset_config: dict[str, Any]) -> tuple[str, str]:
        """
        Pull required keys from config and resolve the Spark format token.

        Returns
        -------
        (bronze_path, spark_format)
        """
        # --- presence checks -------------------------------------------
        # SilverConfig already guarantees these keys exist, but BronzeReader
        # may be used without SilverConfig in tests or future tooling, so we
        # guard explicitly with a clear error rather than letting a KeyError
        # surface from deep inside Spark.
        if "bronze_path" not in dataset_config:
            raise SilverReadError("dataset_config is missing the required key 'bronze_path'.")
        if "file_format" not in dataset_config:
            raise SilverReadError("dataset_config is missing the required key 'file_format'.")

        bronze_path: str = dataset_config["bronze_path"]
        file_format: str = str(dataset_config["file_format"]).lower().strip()

        # --- format support check --------------------------------------
        if file_format not in _SUPPORTED_FORMATS:
            supported = sorted(_SUPPORTED_FORMATS.keys())
            raise SilverReadError(
                f"Unsupported file format '{file_format}'. " f"Supported formats: {supported}"
            )

        spark_format = _SUPPORTED_FORMATS[file_format]
        return bronze_path, spark_format

    def _read_dataframe(self, bronze_path: str, spark_format: str) -> DataFrame:
        """Issue the Spark read and wrap any Spark exception in SilverReadError."""
        logger.info(
            "Reading Bronze dataset | path='%s' format='%s'",
            bronze_path,
            spark_format,
        )

        try:
            df = self._spark.read.format(spark_format).load(bronze_path)
        except Exception as exc:
            raise SilverReadError(
                f"Failed to read Bronze dataset at '{bronze_path}' "
                f"(format='{spark_format}'): {exc}"
            ) from exc

        logger.info(
            "Bronze dataset loaded | path='%s' format='%s'",
            bronze_path,
            spark_format,
        )
        logger.debug("Schema for '%s':\n%s", bronze_path, df.schema.simpleString())

        return df
