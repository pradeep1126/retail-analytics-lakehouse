import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from spark.silver.exceptions import (
    SilverError,
    SilverTransformationError,
)
from spark.utils.logger import get_logger

# --- Dataset-Specific Transformation Registry ---
# Default no-ops for current state.
# Receives both DataFrame and config for future extensibility.


def transform_orders(df: DataFrame, config: dict) -> DataFrame:
    return df


def transform_products(df: DataFrame, config: dict) -> DataFrame:
    return df


def transform_aisles(df: DataFrame, config: dict) -> DataFrame:
    return df


def transform_departments(df: DataFrame, config: dict) -> DataFrame:
    return df


def transform_order_products__prior(df: DataFrame, config: dict) -> DataFrame:
    return df


def transform_order_products__train(df: DataFrame, config: dict) -> DataFrame:
    return df


TRANSFORMATION_REGISTRY = {
    "orders": transform_orders,
    "products": transform_products,
    "aisles": transform_aisles,
    "departments": transform_departments,
    "order_products__prior": transform_order_products__prior,
    "order_products__train": transform_order_products__train,
}


# --- Transformation Manager ---
class TransformationManager:
    def __init__(self, logger=None):
        self.logger = logger or get_logger(__name__)

    def _trim_string_columns(
        self, df: DataFrame, dataset_name: str, dataset_config: dict
    ) -> DataFrame:
        """Trims leading/trailing whitespace for all StringType columns in a single select."""
        select_expr = [
            (
                F.trim(F.col(field.name)).alias(field.name)
                if isinstance(field.dataType, T.StringType)
                else F.col(field.name)
            )
            for field in df.schema.fields
        ]

        df = df.select(*select_expr)
        self.logger.info("trim completed")
        return df

    def _standardize_column_names(
        self, df: DataFrame, dataset_name: str, dataset_config: dict
    ) -> DataFrame:
        """Converts column names to lowercase and replaces spaces with underscores."""
        for col_name in df.columns:
            new_name = col_name.lower().replace(" ", "_")
            if col_name != new_name:
                df = df.withColumnRenamed(col_name, new_name)
        self.logger.info("column standardization completed")
        return df

    def _drop_duplicates(self, df: DataFrame, dataset_name: str, dataset_config: dict) -> DataFrame:
        """Drops duplicates based on config (drop_duplicates: true and primary_key)."""
        if dataset_config.get("drop_duplicates"):
            primary_key = dataset_config.get("primary_key", [])
            if primary_key:
                df = df.dropDuplicates(primary_key)
                self.logger.info("duplicates removed (only if enabled)")
        return df

    def _apply_dataset_specific_logic(
        self, df: DataFrame, dataset_name: str, dataset_config: dict
    ) -> DataFrame:
        """Looks up and executes dataset-specific transformations from the registry."""
        transform_func = TRANSFORMATION_REGISTRY.get(dataset_name)

        if transform_func:
            df = transform_func(df, dataset_config)
            self.logger.info("dataset-specific transformation executed")

        return df

    def apply_transformations(
        self,
        df: DataFrame,
        dataset_name: str,
        dataset_config: dict,
    ) -> DataFrame:
        """Executes the transformation pipeline dynamically."""
        self.logger.info("transformation pipeline started")

        # Data-driven pipeline setup
        # Standardized signature: step(df, dataset_name, dataset_config)
        pipeline = [
            self._trim_string_columns,
            self._standardize_column_names,
            self._drop_duplicates,
            self._apply_dataset_specific_logic,
        ]

        try:
            for step in pipeline:
                df = step(df, dataset_name, dataset_config)
        except SilverError:
            # Re-raise known silver exceptions without double-wrapping
            raise
        except Exception as e:
            # Wrap unexpected Spark/Python exceptions
            raise SilverTransformationError(f"Transformation step failed: {str(e)}") from e

        self.logger.info("pipeline completed")
        return df
