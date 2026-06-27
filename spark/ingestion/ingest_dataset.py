import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from audit.audit_manager import create_audit_record, write_audit_record
from config.config_loader import load_datasets_config
from metadata.metadata_manager import create_metadata_record, write_metadata_record
from readers.csv_reader import get_csv_metadata
from storage.minio_writer import MinIOWriter
from utils.checksum import generate_md5_checksum
from utils.logger import get_logger
from validators.file_validator import validate_source_file


def ingest_dataset(dataset_name: str):
    logger = get_logger(__name__)

    start_time = datetime.now(UTC)
    run_id = f"{dataset_name}_" f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    config = load_datasets_config()
    dataset_config = config[dataset_name]

    source_file_path = Path(dataset_config["source_path"])

    if not validate_source_file(source_file_path):
        logger.error(f"Validation failed for {source_file_path}")
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
        load_date=datetime.now(UTC).date().isoformat(),
        checksum=checksum,
        run_id=run_id,
    )

    end_time = datetime.now(UTC)

    audit_record = create_audit_record(
        run_id=run_id,
        dataset_name=dataset_name,
        status="SUCCESS",
        records_processed=metadata["row_count"],
        file_name=source_file_path.name,
        start_time=start_time,
        end_time=end_time,
    )

    logger.info("Metadata record created")
    logger.info(metadata_record)
    logger.info("Audit record created")
    logger.info(audit_record)

    minio_url = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ROOT_USER")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    writer = MinIOWriter(
        endpoint=minio_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    load_date = datetime.now(UTC).date().isoformat()
    bucket_name = "bronze"
    object_name = f"{dataset_name}/" f"load_date={load_date}/" f"{dataset_name}.csv"
    file_path = str(source_file_path)

    try:
        writer.upload_file(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )
        logger.info(f"{object_name} uploaded successfully")
        write_metadata_record(metadata_record)
        write_audit_record(audit_record)

    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        failed_audit_record = create_audit_record(
            run_id=run_id,
            dataset_name=dataset_name,
            status="FAILED",
            records_processed=0,
            file_name=source_file_path.name,
            start_time=start_time,
            end_time=datetime.now(UTC),
            error_message=str(e),
        )
        write_audit_record(failed_audit_record)
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Ingest one or all datasets into the bronze layer."
    )
    parser.add_argument(
        "dataset",
        help='Dataset name to ingest, or "all" to ingest every dataset in datasets.yaml',
    )
    args = parser.parse_args()

    logger = get_logger(__name__)

    # ── Resolve & validate dataset names against datasets.yaml ──────────────
    config = load_datasets_config()
    known_datasets = set(config.keys())

    if args.dataset == "all":
        dataset_names = list(config.keys())
    else:
        if args.dataset not in known_datasets:
            logger.error(
                f"Unknown dataset '{args.dataset}'. " f"Known datasets: {sorted(known_datasets)}"
            )
            sys.exit(1)
        dataset_names = [args.dataset]

    logger.info(f"Datasets to ingest: {dataset_names}")

    # ── Ingestion loop ───────────────────────────────────────────────────────
    succeeded: list[str] = []
    failed: list[str] = []

    for name in dataset_names:
        logger.info(f"--- Starting ingestion: {name} ---")
        try:
            ingest_dataset(name)
            succeeded.append(name)
        except Exception as e:
            logger.error(f"Ingestion failed for '{name}': {e}")
            failed.append(name)

    # ── Execution summary ────────────────────────────────────────────────────
    total = len(dataset_names)
    logger.info("=" * 40)
    logger.info(f"SUMMARY  |  total={total}  passed={len(succeeded)}  failed={len(failed)}")
    if succeeded:
        logger.info(f"  ✓ {', '.join(succeeded)}")
    if failed:
        logger.error(f"  ✗ {', '.join(failed)}")
    logger.info("=" * 40)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
