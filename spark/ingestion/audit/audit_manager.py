from datetime import datetime


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
