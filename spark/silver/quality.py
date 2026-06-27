import logging
from dataclasses import dataclass
from typing import Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark.silver.exceptions import SilverQualityError


@dataclass
class QualityReport:
    """Standardized report for dataset quality validation."""

    dataset_name: str
    total_records: int
    duplicate_records: int
    null_counts: Dict[str, int]
    passed: bool
    errors: List[str]


# --- Quality Manager ---
class QualityManager:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def validate(
        self,
        df: DataFrame,
        dataset_name: str,
        dataset_config: dict,
    ) -> QualityReport:
        """
        Runs comprehensive quality checks against the DataFrame based on config.
        Returns a QualityReport on success, or raises SilverQualityError on failure.
        """
        self.logger.info(f"Starting quality validation for '{dataset_name}'")

        errors = []
        null_counts = {}
        duplicate_records = 0

        # Action 1: Total count
        total_records = df.count()

        required_columns = dataset_config.get("required_columns", [])
        primary_key = dataset_config.get("primary_key", [])

        # 1. Validate columns exist
        missing_req = [col for col in required_columns if col not in df.columns]
        if missing_req:
            errors.append(f"Missing required columns: {', '.join(missing_req)}")

        missing_pk = [col for col in primary_key if col not in df.columns]
        if missing_pk:
            errors.append(f"Primary key columns missing from dataset: {', '.join(missing_pk)}")

        # 2. Consolidated Null Checks (One Spark Action)
        valid_req_cols = [col for col in required_columns if col in df.columns]
        valid_pk_cols = [col for col in primary_key if col in df.columns]

        # Deduplicate columns to check
        cols_to_check = list(set(valid_req_cols + valid_pk_cols))

        if cols_to_check:
            null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols_to_check]
            # Action 2: Collect null counts
            null_row = df.select(*null_exprs).collect()[0].asDict()
            null_counts = {k: (v if v is not None else 0) for k, v in null_row.items()}

            # Enforce strictly non-null for required columns
            for col in valid_req_cols:
                count = null_counts.get(col, 0)
                if count > 0:
                    errors.append(f"Required column '{col}' contains {count} null value(s).")

            # Enforce strictly non-null for primary keys (avoid double-logging if already required)
            for col in valid_pk_cols:
                count = null_counts.get(col, 0)
                if count > 0 and col not in valid_req_cols:
                    errors.append(f"Primary key column '{col}' contains {count} null value(s).")

        # 3. Validate Primary Key Duplicates
        if valid_pk_cols:
            # Action 3: Count duplicates
            duplicate_records = df.groupBy(valid_pk_cols).count().filter(F.col("count") > 1).count()
            if duplicate_records > 0:
                errors.append(f"Found {duplicate_records} duplicate primary key(s).")

        # 4. Build Quality Report
        passed = len(errors) == 0
        report = QualityReport(
            dataset_name=dataset_name,
            total_records=total_records,
            duplicate_records=duplicate_records,
            null_counts=null_counts,
            passed=passed,
            errors=errors,
        )

        if not passed:
            self.logger.error(
                f"Quality validation failed | Dataset: {dataset_name} | "
                f"Records: {total_records} | Duplicates: {duplicate_records} | "
                f"Errors: {len(errors)}"
            )
            raise SilverQualityError(report)

        self.logger.info(
            f"Quality validation passed | Dataset: {dataset_name} | " f"Records: {total_records}"
        )
        return report
