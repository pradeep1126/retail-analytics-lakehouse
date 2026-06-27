"""
Silver Schema Manager.

Sole responsibility: enforce explicit Spark data types on a Bronze DataFrame.

No transformations, no null-filling, no deduplication, no business rules.
Input DataFrame → Expected Schema → Correctly-typed DataFrame.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark.silver.exceptions import SilverSchemaError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Explicit schemas — one per Instacart dataset.
#
# Type decisions:
#   IntegerType   — surrogate keys and small ordinal counters that are never
#                   NULL in the source (order_id, user_id, product_id, …).
#   DoubleType    — days_since_prior_order: nullable in source (NULL for a
#                   user's very first order); a floating-point type survives
#                   Spark's CSV inference without silent truncation.
#   StringType    — human-readable labels and the eval_set categorical.
#
# nullable=True is the default but is written explicitly on every field for
# clarity.  Primary key / NOT-NULL fields are marked nullable=False so that
# downstream quality checks can rely on the constraint being visible in the
# schema.
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("eval_set", StringType(), nullable=False),
        StructField("order_number", IntegerType(), nullable=False),
        StructField("order_dow", IntegerType(), nullable=False),
        StructField("order_hour_of_day", IntegerType(), nullable=False),
        StructField("days_since_prior_order", DoubleType(), nullable=True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("aisle_id", IntegerType(), nullable=False),
        StructField("department_id", IntegerType(), nullable=False),
    ]
)

AISLES_SCHEMA = StructType(
    [
        StructField("aisle_id", IntegerType(), nullable=False),
        StructField("aisle", StringType(), nullable=False),
    ]
)

DEPARTMENTS_SCHEMA = StructType(
    [
        StructField("department_id", IntegerType(), nullable=False),
        StructField("department", StringType(), nullable=False),
    ]
)

# order_products__prior and order_products__train share identical structure.
# A single schema constant is reused rather than duplicated.
_ORDER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("product_id", IntegerType(), nullable=False),
        StructField("add_to_cart_order", IntegerType(), nullable=False),
        StructField("reordered", IntegerType(), nullable=False),
    ]
)

ORDER_PRODUCTS_PRIOR_SCHEMA = _ORDER_PRODUCTS_SCHEMA
ORDER_PRODUCTS_TRAIN_SCHEMA = _ORDER_PRODUCTS_SCHEMA

# ---------------------------------------------------------------------------
# Registry — the single place to look up a schema by dataset name.
# Adding a new dataset is one line here; no if/elif chain to maintain.
# ---------------------------------------------------------------------------

SCHEMA_REGISTRY: dict[str, StructType] = {
    "orders": ORDERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "aisles": AISLES_SCHEMA,
    "departments": DEPARTMENTS_SCHEMA,
    "order_products__prior": ORDER_PRODUCTS_PRIOR_SCHEMA,
    "order_products__train": ORDER_PRODUCTS_TRAIN_SCHEMA,
}


class SchemaManager:
    """
    Enforces explicit Spark data types on a Bronze DataFrame.

    Parameters are intentionally absent from ``__init__``: the manager is
    stateless and holds no references to Spark or config.  A single instance
    can be reused across any number of datasets.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply_schema(self, df: DataFrame, dataset_name: str) -> DataFrame:
        """
        Cast every column in *df* to the type declared in the registered
        schema for *dataset_name* and return the new DataFrame.

        The original *df* is not mutated (Spark DataFrames are immutable).

        Parameters
        ----------
        df:
            Raw Bronze DataFrame as returned by ``BronzeReader.read``.
        dataset_name:
            Key into ``SCHEMA_REGISTRY`` (e.g. ``"orders"``).

        Returns
        -------
        pyspark.sql.DataFrame
            New DataFrame with every column cast to the expected type.

        Raises
        ------
        SilverSchemaError
            If no schema is registered for *dataset_name*, or if any
            expected column is absent from *df*.
        """
        schema = self._resolve_schema(dataset_name)
        self._validate_columns(df, dataset_name, schema)
        return self._cast_columns(df, dataset_name, schema)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_schema(dataset_name: str) -> StructType:
        """Look up the schema or raise SilverSchemaError."""
        schema = SCHEMA_REGISTRY.get(dataset_name)
        if schema is None:
            registered = sorted(SCHEMA_REGISTRY.keys())
            raise SilverSchemaError(
                f"No schema registered for dataset '{dataset_name}'. "
                f"Registered datasets: {registered}"
            )
        return schema

    @staticmethod
    def _validate_columns(
        df: DataFrame,
        dataset_name: str,
        schema: StructType,
    ) -> None:
        """
        Raise SilverSchemaError if any column declared in *schema* is absent
        from *df*.  Reports all missing columns at once rather than stopping
        at the first one.
        """
        actual_columns = set(df.columns)
        expected_columns = {field.name for field in schema.fields}

        missing = sorted(expected_columns - actual_columns)
        if missing:
            logger.error(
                "Schema validation failed for dataset '%s': "
                "missing columns %s | expected=%d actual=%d",
                dataset_name,
                missing,
                len(expected_columns),
                len(actual_columns),
            )
            raise SilverSchemaError(
                f"Dataset '{dataset_name}' is missing required columns: {missing}"
            )

        logger.info(
            "Schema validation passed for dataset '%s' | " "expected_columns=%d actual_columns=%d",
            dataset_name,
            len(expected_columns),
            len(actual_columns),
        )

    @staticmethod
    def _cast_columns(
        df: DataFrame,
        dataset_name: str,
        schema: StructType,
    ) -> DataFrame:
        """
        Return a new DataFrame containing only the schema-declared columns,
        each cast to its target type.  Columns present in *df* but absent
        from the schema are dropped — Silver is a strict contract and unknown
        columns must not propagate into downstream layers.
        """
        cast_exprs = [
            col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields
        ]

        typed_df = df.select(*cast_exprs)

        logger.info(
            "Schema applied successfully for dataset '%s'",
            dataset_name,
        )
        logger.debug(
            "Schema for '%s': %s",
            dataset_name,
            schema.simpleString(),
        )

        return typed_df
