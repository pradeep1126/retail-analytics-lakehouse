"""
Unit tests for spark.silver.schema_manager.SchemaManager.

PySpark is not installed, so the Spark surface is mocked at two levels:

  _make_df(columns)
      A mock DataFrame whose .columns property returns *columns* and whose
      .select() returns a new mock.  This is enough for all paths through
      SchemaManager because the manager only calls df.columns and df.select().

  _cast_spy(df)
      Wraps _make_df and additionally records every column expression passed
      to df.select() so tests can inspect which casts were requested.

The pyspark.sql.functions.col and pyspark.sql.types stubs are injected into
sys.modules before any import of schema_manager so the module-level type
constants (StructType, StructField, …) resolve without a real Spark install.
"""

import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Minimal PySpark stubs
# Populate sys.modules before importing schema_manager so that
# ``from pyspark.sql.types import ...`` and
# ``from pyspark.sql.functions import col`` resolve cleanly.
# ---------------------------------------------------------------------------


def _build_pyspark_stubs():
    # --- pyspark.sql.types --------------------------------------------------
    class _StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class _StructType:
        def __init__(self, fields=None):
            self.fields = fields or []

        def simpleString(self):
            return "struct<" + ",".join(f"{f.name}:{f.dataType}" for f in self.fields) + ">"

    class _AtomicType:
        """Lightweight stand-in for IntegerType, StringType, DoubleType."""

        def __init__(self, name):
            self._name = name

        def __repr__(self):
            return self._name

    types_mod = types.ModuleType("pyspark.sql.types")
    types_mod.StructType = _StructType
    types_mod.StructField = _StructField
    types_mod.IntegerType = lambda: _AtomicType("IntegerType")
    types_mod.StringType = lambda: _AtomicType("StringType")
    types_mod.DoubleType = lambda: _AtomicType("DoubleType")

    # --- pyspark.sql.functions ----------------------------------------------
    # col() returns a _ColExpr that records the column name and supports
    # .cast() and .alias() so the comprehension in _cast_columns works.
    class _ColExpr:
        def __init__(self, name):
            self.name = name
            self._cast_type = None
            self._alias = name

        def cast(self, dataType):
            self._cast_type = dataType
            return self

        def alias(self, name):
            self._alias = name
            return self

        def __repr__(self):
            return f"col({self.name!r})" f".cast({self._cast_type})" f".alias({self._alias!r})"

    functions_mod = types.ModuleType("pyspark.sql.functions")
    functions_mod.col = _ColExpr

    # --- pyspark and pyspark.sql --------------------------------------------
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    sql_mod.DataFrame = object
    sql_mod.SparkSession = object

    sys.modules.setdefault("pyspark", pyspark_mod)
    sys.modules.setdefault("pyspark.sql", sql_mod)
    sys.modules.setdefault("pyspark.sql.types", types_mod)
    sys.modules.setdefault("pyspark.sql.functions", functions_mod)


_build_pyspark_stubs()

# Now safe to import
from spark.silver.exceptions import SilverSchemaError  # noqa: E402
from spark.silver.schema_manager import (  # noqa: E402
    AISLES_SCHEMA,
    DEPARTMENTS_SCHEMA,
    ORDER_PRODUCTS_PRIOR_SCHEMA,
    ORDER_PRODUCTS_TRAIN_SCHEMA,
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    SCHEMA_REGISTRY,
    SchemaManager,
)

# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------


def _make_df(columns: list[str]) -> MagicMock:
    """
    Return a mock DataFrame.

    .columns  → *columns*
    .select() → a fresh MagicMock representing the post-cast DataFrame
    """
    df = MagicMock(name="DataFrame")
    df.columns = columns
    result_df = MagicMock(name="ResultDataFrame")
    df.select.return_value = result_df
    return df


def _all_columns_for(dataset_name: str) -> list[str]:
    """Return every column name declared in the schema for *dataset_name*."""
    schema = SCHEMA_REGISTRY[dataset_name]
    return [field.name for field in schema.fields]


# ---------------------------------------------------------------------------
# 1. Schema registry integrity
# ---------------------------------------------------------------------------


class TestSchemaRegistry(unittest.TestCase):

    def test_all_six_datasets_are_registered(self):
        expected = {
            "orders",
            "products",
            "aisles",
            "departments",
            "order_products__prior",
            "order_products__train",
        }
        self.assertEqual(set(SCHEMA_REGISTRY.keys()), expected)

    def test_orders_has_seven_fields(self):
        self.assertEqual(len(ORDERS_SCHEMA.fields), 7)

    def test_products_has_four_fields(self):
        self.assertEqual(len(PRODUCTS_SCHEMA.fields), 4)

    def test_aisles_has_two_fields(self):
        self.assertEqual(len(AISLES_SCHEMA.fields), 2)

    def test_departments_has_two_fields(self):
        self.assertEqual(len(DEPARTMENTS_SCHEMA.fields), 2)

    def test_order_products_prior_has_four_fields(self):
        self.assertEqual(len(ORDER_PRODUCTS_PRIOR_SCHEMA.fields), 4)

    def test_order_products_train_has_four_fields(self):
        self.assertEqual(len(ORDER_PRODUCTS_TRAIN_SCHEMA.fields), 4)

    def test_orders_field_names(self):
        names = [f.name for f in ORDERS_SCHEMA.fields]
        self.assertIn("order_id", names)
        self.assertIn("user_id", names)
        self.assertIn("eval_set", names)
        self.assertIn("order_number", names)
        self.assertIn("order_dow", names)
        self.assertIn("order_hour_of_day", names)
        self.assertIn("days_since_prior_order", names)

    def test_days_since_prior_order_is_nullable(self):
        """First-order NULLs must be accommodated."""
        field = next(f for f in ORDERS_SCHEMA.fields if f.name == "days_since_prior_order")
        self.assertTrue(field.nullable)

    def test_order_products_prior_and_train_share_same_fields(self):
        prior_names = [f.name for f in ORDER_PRODUCTS_PRIOR_SCHEMA.fields]
        train_names = [f.name for f in ORDER_PRODUCTS_TRAIN_SCHEMA.fields]
        self.assertEqual(prior_names, train_names)

    def test_every_registry_value_is_a_struct_type(self):
        from pyspark.sql.types import StructType

        from spark.silver.schema_manager import SCHEMA_REGISTRY as R

        for name, schema in R.items():
            with self.subTest(dataset=name):
                self.assertIsInstance(schema, StructType)


# ---------------------------------------------------------------------------
# 2. Valid schema application
# ---------------------------------------------------------------------------


class TestApplySchemaSuccess(unittest.TestCase):

    def _apply(self, dataset_name: str) -> tuple[MagicMock, MagicMock]:
        """Helper: build a complete mock df and apply schema. Returns (df, result)."""
        columns = _all_columns_for(dataset_name)
        df = _make_df(columns)
        result = SchemaManager().apply_schema(df, dataset_name)
        return df, result

    def test_returns_dataframe_for_orders(self):
        df, result = self._apply("orders")
        df.select.assert_called_once()
        self.assertIsNotNone(result)

    @unittest.skipIf("orders" not in SCHEMA_REGISTRY, "orders not registered")
    def test_select_called_once_per_apply(self):
        df, _ = self._apply("orders")
        self.assertEqual(df.select.call_count, 1)

    def test_valid_schema_applied_for_all_six_datasets(self):
        """Regression guard: every registered dataset must apply without error."""
        for name in SCHEMA_REGISTRY:
            with self.subTest(dataset=name):
                df, result = self._apply(name)
                df.select.assert_called_once()

    def test_returned_df_is_not_original_df(self):
        """Spark is immutable — apply_schema must return the result of select()."""
        columns = _all_columns_for("orders")
        df = _make_df(columns)
        result = SchemaManager().apply_schema(df, "orders")
        self.assertIsNot(result, df)

    def test_extra_columns_in_df_are_dropped(self):
        """
        Silver is a strict contract — columns not declared in the schema
        must be dropped, not passed through.
        """
        columns = _all_columns_for("orders") + ["extra_col_not_in_schema"]
        df = _make_df(columns)
        SchemaManager().apply_schema(df, "orders")
        all_exprs = list(df.select.call_args[0])
        expr_names = {e.name for e in all_exprs}
        self.assertNotIn("extra_col_not_in_schema", expr_names)


# ---------------------------------------------------------------------------
# 3. Unknown dataset
# ---------------------------------------------------------------------------


class TestUnknownDataset(unittest.TestCase):

    def test_raises_for_unknown_dataset(self):
        df = _make_df(["col_a"])
        with self.assertRaises(SilverSchemaError):
            SchemaManager().apply_schema(df, "nonexistent")

    def test_error_names_the_dataset(self):
        df = _make_df(["col_a"])
        with self.assertRaises(SilverSchemaError) as ctx:
            SchemaManager().apply_schema(df, "nonexistent")
        self.assertIn("nonexistent", str(ctx.exception))

    def test_error_lists_registered_datasets(self):
        df = _make_df(["col_a"])
        with self.assertRaises(SilverSchemaError) as ctx:
            SchemaManager().apply_schema(df, "nonexistent")
        msg = str(ctx.exception)
        for name in SCHEMA_REGISTRY:
            self.assertIn(name, msg)

    def test_select_never_called_for_unknown_dataset(self):
        df = _make_df(["col_a"])
        try:
            SchemaManager().apply_schema(df, "nonexistent")
        except SilverSchemaError:
            pass
        df.select.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Missing required column
# ---------------------------------------------------------------------------


class TestMissingColumn(unittest.TestCase):

    def test_raises_when_column_missing(self):
        # orders requires order_id — omit it
        columns = [f.name for f in ORDERS_SCHEMA.fields if f.name != "order_id"]
        df = _make_df(columns)
        with self.assertRaises(SilverSchemaError):
            SchemaManager().apply_schema(df, "orders")

    def test_error_names_the_missing_column(self):
        columns = [f.name for f in ORDERS_SCHEMA.fields if f.name != "order_id"]
        df = _make_df(columns)
        with self.assertRaises(SilverSchemaError) as ctx:
            SchemaManager().apply_schema(df, "orders")
        self.assertIn("order_id", str(ctx.exception))

    def test_error_reports_all_missing_columns_at_once(self):
        """A single call must surface every missing column, not just the first."""
        missing = ["order_id", "user_id"]
        columns = [f.name for f in ORDERS_SCHEMA.fields if f.name not in missing]
        df = _make_df(columns)
        with self.assertRaises(SilverSchemaError) as ctx:
            SchemaManager().apply_schema(df, "orders")
        msg = str(ctx.exception)
        for col_name in missing:
            self.assertIn(col_name, msg)

    def test_select_not_called_when_column_missing(self):
        columns = [f.name for f in ORDERS_SCHEMA.fields if f.name != "order_id"]
        df = _make_df(columns)
        try:
            SchemaManager().apply_schema(df, "orders")
        except SilverSchemaError:
            pass
        df.select.assert_not_called()

    def test_empty_dataframe_columns_raises(self):
        df = _make_df([])
        with self.assertRaises(SilverSchemaError):
            SchemaManager().apply_schema(df, "orders")

    @unittest.skipIf("orders" not in SCHEMA_REGISTRY, "orders not registered")
    def test_missing_column_for_each_field(self):
        """Removing any single required column must raise for every dataset."""
        for dataset_name in SCHEMA_REGISTRY:
            schema = SCHEMA_REGISTRY[dataset_name]
            for field in schema.fields:
                with self.subTest(dataset=dataset_name, column=field.name):
                    columns = [f.name for f in schema.fields if f.name != field.name]
                    df = _make_df(columns)
                    with self.assertRaises(SilverSchemaError):
                        SchemaManager().apply_schema(df, dataset_name)


# ---------------------------------------------------------------------------
# 5. Cast expressions — every column is cast to the declared type
# ---------------------------------------------------------------------------


class TestCastExpressions(unittest.TestCase):
    """
    Inspect the arguments passed to df.select() to verify that each
    schema field produces a col(name).cast(type).alias(name) expression.
    """

    def _get_cast_exprs(self, dataset_name: str):
        """Return the positional args list passed to df.select()."""
        columns = _all_columns_for(dataset_name)
        df = _make_df(columns)
        SchemaManager().apply_schema(df, dataset_name)
        # select() receives *args, so call_args[0] is the tuple of expressions
        return list(df.select.call_args[0])

    def test_number_of_select_args_equals_schema_field_count(self):
        exprs = self._get_cast_exprs("orders")
        # 7 schema columns, 0 extra columns → 7 expressions
        self.assertEqual(len(exprs), len(ORDERS_SCHEMA.fields))

    def test_each_expression_has_correct_column_name(self):
        exprs = self._get_cast_exprs("orders")
        expr_names = {e.name for e in exprs}
        schema_names = {f.name for f in ORDERS_SCHEMA.fields}
        self.assertEqual(expr_names, schema_names)

    def test_alias_matches_original_column_name(self):
        """Alias must equal the column name — manager must not rename columns."""
        exprs = self._get_cast_exprs("orders")
        for expr in exprs:
            with self.subTest(column=expr.name):
                self.assertEqual(expr._alias, expr.name)

    def test_cast_applied_to_every_expression(self):
        """Every expression must carry a cast type (i.e. .cast() was called)."""
        exprs = self._get_cast_exprs("orders")
        for expr in exprs:
            with self.subTest(column=expr.name):
                self.assertIsNotNone(
                    expr._cast_type,
                    f"col('{expr.name}') has no cast type — .cast() was not called.",
                )

    def test_cast_expressions_for_all_six_datasets(self):
        """Regression guard: every registered dataset produces cast expressions."""
        for name in SCHEMA_REGISTRY:
            with self.subTest(dataset=name):
                exprs = self._get_cast_exprs(name)
                self.assertGreater(len(exprs), 0)
                for expr in exprs:
                    self.assertIsNotNone(expr._cast_type)

    def test_extra_columns_are_excluded_from_select(self):
        """
        Columns not in the schema must not appear in the select() call —
        strict contract means no schema drift passes through.
        """
        extra = "unknown_extra_column"
        columns = _all_columns_for("aisles") + [extra]
        df = _make_df(columns)
        SchemaManager().apply_schema(df, "aisles")
        all_exprs = list(df.select.call_args[0])
        expr_names = {e.name for e in all_exprs}
        self.assertNotIn(extra, expr_names)

    def test_select_arg_count_equals_schema_field_count_even_with_extra_columns(self):
        """
        With strict enforcement, select() receives exactly as many expressions
        as there are schema fields — extras are silently dropped.
        """
        extra = "unknown_extra_column"
        columns = _all_columns_for("aisles") + [extra]
        df = _make_df(columns)
        SchemaManager().apply_schema(df, "aisles")
        all_exprs = list(df.select.call_args[0])
        self.assertEqual(len(all_exprs), len(AISLES_SCHEMA.fields))


# ---------------------------------------------------------------------------
# 6. Logging
# ---------------------------------------------------------------------------


class TestLogging(unittest.TestCase):

    def test_info_logged_on_successful_apply(self):
        columns = _all_columns_for("orders")
        df = _make_df(columns)
        with patch("spark.silver.schema_manager.logger") as mock_log:
            SchemaManager().apply_schema(df, "orders")
        self.assertGreaterEqual(mock_log.info.call_count, 1)

    def test_debug_logged_for_schema_string(self):
        columns = _all_columns_for("orders")
        df = _make_df(columns)
        with patch("spark.silver.schema_manager.logger") as mock_log:
            SchemaManager().apply_schema(df, "orders")
        mock_log.debug.assert_called_once()

    def test_error_logged_on_missing_column(self):
        columns = [f.name for f in ORDERS_SCHEMA.fields if f.name != "order_id"]
        df = _make_df(columns)
        with patch("spark.silver.schema_manager.logger") as mock_log:
            try:
                SchemaManager().apply_schema(df, "orders")
            except SilverSchemaError:
                pass
        mock_log.error.assert_called_once()

    def test_dataset_name_appears_in_info_log(self):
        columns = _all_columns_for("products")
        df = _make_df(columns)
        with patch("spark.silver.schema_manager.logger") as mock_log:
            SchemaManager().apply_schema(df, "products")
        all_calls = " ".join(str(c) for c in mock_log.info.call_args_list)
        self.assertIn("products", all_calls)


if __name__ == "__main__":
    unittest.main()
