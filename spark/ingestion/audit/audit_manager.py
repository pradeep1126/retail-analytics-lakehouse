from datetime import datetime
from pathlib import Path

import pandas as pd


def create_audit_record(
    run_id: str,
    dataset_name: str,
    status: str,
    records_processed: int,
    file_name: str,
    start_time: datetime,
    end_time: datetime,
    error_message: str | None = None,
) -> dict:
    """
    Create audit record.
    """

    duration_seconds = int((end_time - start_time).total_seconds())

    return {
        "run_id": run_id,
        "dataset_name": dataset_name,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": duration_seconds,
        "status": status,
        "records_processed": records_processed,
        "file_name": file_name,
        "error_message": error_message,
    }


def write_audit_record(audit_record: dict) -> None:
    """
    Write audit record to parquet.
    """

    audit_dir = Path("data/bronze/audit")

    audit_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    audit_file = audit_dir / "ingestion_audit.parquet"

    new_df = pd.DataFrame([audit_record])

    if audit_file.exists():
        existing_df = pd.read_parquet(audit_file)

        combined_df = pd.concat(
            [existing_df, new_df],
            ignore_index=True,
        )

        combined_df.to_parquet(
            audit_file,
            index=False,
        )
    else:
        new_df.to_parquet(
            audit_file,
            index=False,
        )
