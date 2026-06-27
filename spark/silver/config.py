"""
Silver Configuration Manager.

Loads and validates silver_config.yaml, providing a cached, validated
configuration for each dataset. All consumers share one source of truth.
"""

import copy
import logging
from typing import Any

import yaml

from spark.silver.exceptions import SilverConfigurationError

logger = logging.getLogger(__name__)

REQUIRED_KEYS: list[str] = [
    "bronze_path",
    "silver_path",
    "file_format",
    "write_format",
    "write_mode",
    "compression",
    "partition_columns",
    "primary_key",
    "required_columns",
    "drop_duplicates",
]


class SilverConfig:
    """
    Loads and validates the silver_config.yaml file.

    The raw YAML is read from disk exactly once and cached on the instance.
    Subsequent calls to ``get_dataset_config`` use the in-memory cache.

    Parameters
    ----------
    config_path:
        Path to the silver_config.yaml file (e.g. "configs/silver_config.yaml").

    Raises
    ------
    SilverConfigurationError
        If the file is missing, the YAML is invalid, or the top-level
        structure is not a mapping.
    """

    def __init__(self, config_path: str) -> None:
        self._config_path = config_path
        self._config: dict[str, Any] = self._load(config_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_dataset_config(self, dataset_name: str) -> dict[str, Any]:
        """
        Return the validated configuration block for *dataset_name*.

        Parameters
        ----------
        dataset_name:
            Key that must exist under the top-level ``datasets`` mapping
            in the YAML file (e.g. ``"orders"``).

        Returns
        -------
        dict
            A copy of the validated configuration block.

        Raises
        ------
        SilverConfigurationError
            If the dataset is not found or any required key is absent.
        """
        datasets: dict[str, Any] = self._config.get("datasets", {})

        if dataset_name not in datasets:
            available = sorted(datasets.keys())
            raise SilverConfigurationError(
                f"Dataset '{dataset_name}' not found in '{self._config_path}'. "
                f"Available datasets: {available}"
            )

        dataset_cfg: dict[str, Any] = datasets[dataset_name]

        if not isinstance(dataset_cfg, dict):
            raise SilverConfigurationError(
                f"Configuration for dataset '{dataset_name}' must be a YAML mapping, "
                f"got {type(dataset_cfg).__name__}."
            )

        self._validate(dataset_name, dataset_cfg)

        logger.info("Dataset configuration retrieved: '%s'", dataset_name)
        return copy.deepcopy(dataset_cfg)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load(self, config_path: str) -> dict[str, Any]:
        """Read and parse the YAML file; cache the result."""
        logger.info("Loading Silver configuration file: '%s'", config_path)

        try:
            with open(config_path, "r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh)
        except FileNotFoundError:
            raise SilverConfigurationError(
                f"Silver configuration file not found: '{config_path}'"
            ) from None
        except yaml.YAMLError as exc:
            raise SilverConfigurationError(
                f"Invalid YAML in Silver configuration file '{config_path}': {exc}"
            ) from exc

        if raw is None:
            raise SilverConfigurationError(f"Silver configuration file '{config_path}' is empty.")

        if not isinstance(raw, dict):
            raise SilverConfigurationError(
                f"Silver configuration file '{config_path}' must contain a YAML "
                f"mapping at the top level, got {type(raw).__name__}."
            )

        if "datasets" not in raw:
            raise SilverConfigurationError(
                f"Silver configuration file '{config_path}' is missing the "
                f"required top-level 'datasets' key."
            )

        if not isinstance(raw["datasets"], dict):
            raise SilverConfigurationError(
                f"'datasets' in '{config_path}' must be a YAML mapping, "
                f"got {type(raw['datasets']).__name__}."
            )

        logger.info("Silver configuration loaded successfully from '%s'", config_path)
        return raw

    @staticmethod
    def _validate(dataset_name: str, dataset_cfg: dict[str, Any]) -> None:
        """Raise SilverConfigurationError if any required key is absent."""
        missing = [key for key in REQUIRED_KEYS if key not in dataset_cfg]

        if missing:
            logger.error(
                "Configuration validation failed for dataset '%s': missing keys %s",
                dataset_name,
                missing,
            )
            raise SilverConfigurationError(
                f"Dataset '{dataset_name}' is missing required configuration " f"keys: {missing}"
            )
