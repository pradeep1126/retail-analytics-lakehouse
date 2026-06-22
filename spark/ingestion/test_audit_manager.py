from datetime import datetime, timedelta

from audit.audit_manager import create_audit_record

start_time = datetime.now()
end_time = start_time + timedelta(seconds=5)

record = create_audit_record(
    run_id="manual_001",
    dataset_name="aisles",
    status="SUCCESS",
    records_processed=134,
    file_name="aisles.csv",
    start_time=start_time,
    end_time=end_time,
)

print(record)
