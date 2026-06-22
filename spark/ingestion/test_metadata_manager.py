from metadata.metadata_manager import create_metadata_record

record = create_metadata_record(
    dataset_name="aisles",
    source_file_name="aisles.csv",
    source_file_path="data/source/archive/aisles.csv",
    file_size_bytes=2603,
    row_count=134,
    column_count=2,
    column_names="aisle_id,aisle",
    load_date="2026-06-18",
    checksum="abc123",
    run_id="manual_001",
)

print(record)
