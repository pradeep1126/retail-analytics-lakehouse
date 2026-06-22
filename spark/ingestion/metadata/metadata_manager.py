from datetime import datetime


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
