from datetime import date

from storage.minio_writer import MinIOWriter


def main():
    writer = MinIOWriter(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin123",
        secure=False,
    )

    load_date = date.today().isoformat()

    bucket_name = "bronze"

    object_name = f"aisles/" f"load_date={load_date}/" f"aisles.csv"

    file_path = "data/source/archive/aisles.csv"

    writer.upload_file(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
    )


if __name__ == "__main__":
    main()
