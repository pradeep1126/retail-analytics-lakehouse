from pathlib import Path

import yaml

CONFIG_FILE = Path(__file__).resolve().parent / "datasets.yaml"


def load_datasets_config():
    """
    load dataset ingestion configuration
    """
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config["datasets"]
