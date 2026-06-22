from pathlib import Path

import pandas as pd


def get_csv_metadata(file_path: Path):
    """
    Get metadata for a CSV file.

    Args:
        file_path (Path): Path to the CSV file.

    Returns:
        dict: Metadata for the CSV file.
    """
    df = pd.read_csv(file_path)

    return {
        "file_size_bytes": file_path.stat().st_size,
        "row_count": len(df),
        "column_count": len(df.columns),
        "column_names": ",".join(df.columns.tolist()),
    }
