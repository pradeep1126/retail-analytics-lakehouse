"""
Custom exceptions for Silver layer processing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spark.silver.quality import QualityReport


class SilverError(Exception):
    """Base exception for Silver processing."""


class SilverConfigurationError(SilverError):
    """Raised when Silver configuration is invalid or missing."""


class SilverReadError(SilverError):
    """Raised when Bronze data cannot be read."""


class SilverSchemaError(SilverError):
    """Raised when schema enforcement fails."""


class SilverTransformationError(SilverError):
    """Raised when a transformation step fails."""


class SilverQualityError(SilverError):
    """Raised when one or more quality validations fail."""

    def __init__(self, report: QualityReport):
        self.report = report
        super().__init__(
            f"Quality validation failed for dataset '{report.dataset_name}' "
            f"with {len(report.errors)} error(s)."
        )
