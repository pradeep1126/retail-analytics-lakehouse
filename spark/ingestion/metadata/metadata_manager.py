from datetime import datetime
from pathlib import Path

import pandas as pd


def create_metadata_record(
    dataset_name: str,
    source_file_name: str,
    source_file_path: str,
    file_size_bytes: int,
    row_count: int,
    column_count: int,
    column_names: str,
    load_date: str,
    checksum: str,
    run_id: str,
) -> dict:
    """
    Create ingestion metadata record.
    """

    return {
        "dataset_name": dataset_name,
        "source_file_name": source_file_name,
        "source_file_path": source_file_path,
        "file_size_bytes": file_size_bytes,
        "row_count": row_count,
        "column_count": column_count,
        "column_names": column_names,
        "load_date": load_date,
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "checksum": checksum,
        "run_id": run_id,
    }


def write_metadata_record(metadata_record: dict) -> None:
    """
    Write metadata record to parquet.
    """

    metadata_dir = Path("data/bronze/metadata")
    metadata_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_file = metadata_dir / "ingestion_metadata.parquet"

    new_df = pd.DataFrame([metadata_record])

    if metadata_file.exists():
        existing_df = pd.read_parquet(metadata_file)

        combined_df = pd.concat(
            [existing_df, new_df],
            ignore_index=True,
        )

        combined_df.to_parquet(
            metadata_file,
            index=False,
        )
    else:
        new_df.to_parquet(
            metadata_file,
            index=False,
        )
