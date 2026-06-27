"""
Unit tests for spark.silver.config.SilverConfig.

Each test is fully isolated: temporary YAML files are written via
pytest's ``tmp_path`` fixture so nothing touches the real configs/ folder.
"""

import textwrap

import pytest

from spark.silver.config import REQUIRED_KEYS, SilverConfig
from spark.silver.exceptions import SilverConfigurationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path, content: str):
    """Write *content* to *path* and return the path as a string."""
    p = path / "silver_config.yaml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def _full_dataset_block(overrides: dict = None, omit: list = None) -> str:
    """
    Return a YAML string for a complete 'orders' dataset block,
    optionally merging *overrides* or dropping keys in *omit*.
    """
    base = {
        "bronze_path": '"s3://bronze/orders/"',
        "silver_path": '"s3://silver/orders/"',
        "file_format": '"parquet"',
        "write_format": '"delta"',
        "write_mode": '"overwrite"',
        "compression": '"snappy"',
        "partition_columns": "[order_date]",
        "primary_key": "[order_id]",
        "required_columns": "[order_id, customer_id]",
        "drop_duplicates": "true",
    }
    if overrides:
        base.update(overrides)
    if omit:
        for key in omit:
            base.pop(key, None)

    lines = ["datasets:", "  orders:"]
    for k, v in base.items():
        lines.append(f"    {k}: {v}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 1. Valid configuration
# ---------------------------------------------------------------------------


class TestValidConfiguration:
    def test_returns_dict_for_known_dataset(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        result = cfg.get_dataset_config("orders")
        assert isinstance(result, dict)

    def test_all_required_keys_present(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        result = cfg.get_dataset_config("orders")
        for key in REQUIRED_KEYS:
            assert key in result, f"Expected key '{key}' in dataset config"

    def test_returns_copy_not_original(self, tmp_path):
        """Mutating the returned dict must not affect cached state."""
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        result = cfg.get_dataset_config("orders")
        result["bronze_path"] = "MUTATED"
        fresh = cfg.get_dataset_config("orders")
        assert fresh["bronze_path"] != "MUTATED"

    def test_yaml_read_once(self, tmp_path, mocker):
        """The YAML file should be read exactly once regardless of call count."""
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        mock_open = mocker.patch("builtins.open", wraps=open)
        cfg = SilverConfig(cfg_path)
        cfg.get_dataset_config("orders")
        cfg.get_dataset_config("orders")
        assert mock_open.call_count == 1

    def test_multiple_datasets(self, tmp_path):
        yaml_content = textwrap.dedent("""
            datasets:
              orders:
                bronze_path: s3://b/orders/
                silver_path: s3://s/orders/
                file_format: parquet
                write_format: delta
                write_mode: overwrite
                compression: snappy
                partition_columns: []
                primary_key: [order_id]
                required_columns: [order_id]
                drop_duplicates: true
              customers:
                bronze_path: s3://b/customers/
                silver_path: s3://s/customers/
                file_format: parquet
                write_format: delta
                write_mode: merge
                compression: snappy
                partition_columns: []
                primary_key: [customer_id]
                required_columns: [customer_id]
                drop_duplicates: false
        """)
        cfg_path = _write_yaml(tmp_path, yaml_content)
        cfg = SilverConfig(cfg_path)
        assert cfg.get_dataset_config("orders")["write_mode"] == "overwrite"
        assert cfg.get_dataset_config("customers")["write_mode"] == "merge"


# ---------------------------------------------------------------------------
# 2. Unknown dataset
# ---------------------------------------------------------------------------


class TestUnknownDataset:
    def test_raises_for_unknown_dataset(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError, match="not found"):
            cfg.get_dataset_config("nonexistent")

    def test_error_message_includes_dataset_name(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError, match="nonexistent"):
            cfg.get_dataset_config("nonexistent")

    def test_error_message_lists_available_datasets(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block())
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError, match="orders"):
            cfg.get_dataset_config("wrong_name")


# ---------------------------------------------------------------------------
# 3. Missing required key
# ---------------------------------------------------------------------------


class TestMissingRequiredKey:
    @pytest.mark.parametrize("missing_key", REQUIRED_KEYS)
    def test_raises_when_key_is_missing(self, tmp_path, missing_key):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block(omit=[missing_key]))
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError, match=missing_key):
            cfg.get_dataset_config("orders")

    def test_error_message_names_all_missing_keys(self, tmp_path):
        omit = ["primary_key", "bronze_path"]
        cfg_path = _write_yaml(tmp_path, _full_dataset_block(omit=omit))
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError) as exc_info:
            cfg.get_dataset_config("orders")
        msg = str(exc_info.value)
        for key in omit:
            assert key in msg

    def test_error_is_silver_configuration_error(self, tmp_path):
        cfg_path = _write_yaml(tmp_path, _full_dataset_block(omit=["primary_key"]))
        cfg = SilverConfig(cfg_path)
        with pytest.raises(SilverConfigurationError):
            cfg.get_dataset_config("orders")


# ---------------------------------------------------------------------------
# 4. Missing config file
# ---------------------------------------------------------------------------


class TestMissingConfigFile:
    def test_raises_when_file_not_found(self):
        with pytest.raises(SilverConfigurationError, match="not found"):
            SilverConfig("/definitely/does/not/exist/silver_config.yaml")

    def test_error_message_includes_path(self):
        bad_path = "/no/such/path/silver_config.yaml"
        with pytest.raises(SilverConfigurationError, match=bad_path):
            SilverConfig(bad_path)

    def test_is_silver_error_subclass(self):
        from spark.silver.exceptions import SilverError

        with pytest.raises(SilverError):
            SilverConfig("/no/such/file.yaml")


# ---------------------------------------------------------------------------
# 5. Invalid YAML
# ---------------------------------------------------------------------------


class TestInvalidYaml:
    def test_raises_for_invalid_yaml(self, tmp_path):
        bad_yaml = tmp_path / "silver_config.yaml"
        bad_yaml.write_text("datasets:\n  orders:\n  - bad: [unbalanced", encoding="utf-8")
        with pytest.raises(SilverConfigurationError, match="Invalid YAML"):
            SilverConfig(str(bad_yaml))

    def test_raises_when_top_level_is_list(self, tmp_path):
        """Top-level list is valid YAML but not a valid config mapping."""
        list_yaml = tmp_path / "silver_config.yaml"
        list_yaml.write_text("- item1\n- item2\n", encoding="utf-8")
        with pytest.raises(SilverConfigurationError, match="mapping"):
            SilverConfig(str(list_yaml))

    def test_raises_when_file_is_empty(self, tmp_path):
        empty_yaml = tmp_path / "silver_config.yaml"
        empty_yaml.write_text("", encoding="utf-8")
        with pytest.raises(SilverConfigurationError):
            SilverConfig(str(empty_yaml))
