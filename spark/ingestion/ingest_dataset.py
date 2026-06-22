from datetime import datetime
from pathlib import Path

from audit.audit_manager import create_audit_record
from config.config_loader import load_datasets_config
from metadata.metadata_manager import create_metadata_record
from readers.csv_reader import get_csv_metadata
from utils.checksum import generate_md5_checksum
from validators.file_validator import validate_source_file


def ingest_dataset(dataset_name: str):

    start_time = datetime.utcnow()
    run_id = "manual_001"

    config = load_datasets_config()
    dataset_config = config[dataset_name]

    source_file_path = Path(dataset_config["source_path"])

    if not validate_source_file(source_file_path):
        print("Validation failed")
        return

    metadata = get_csv_metadata(source_file_path)

    checksum = generate_md5_checksum(source_file_path)

    metadata_record = create_metadata_record(
        dataset_name=dataset_name,
        source_file_name=source_file_path.name,
        source_file_path=str(source_file_path),
        file_size_bytes=metadata["file_size_bytes"],
        row_count=metadata["row_count"],
        column_count=metadata["column_count"],
        column_names=metadata["column_names"],
        load_date=datetime.utcnow().date().isoformat(),
        checksum=checksum,
        run_id=run_id,
    )

    end_time = datetime.utcnow()

    audit_record = create_audit_record(
        run_id=run_id,
        dataset_name=dataset_name,
        status="SUCCESS",
        records_processed=metadata["row_count"],
        file_name=source_file_path.name,
        start_time=start_time,
        end_time=end_time,
    )

    print("\nMetadata Record")
    print(metadata_record)

    print("\nAudit Record")
    print(audit_record)


if __name__ == "__main__":
    ingest_dataset("aisles")
