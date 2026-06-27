from minio import Minio


class MinIOWriter:
    """
    Wrapper around MinIO client for file uploads.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ) -> None:
        """
        Upload a local file to MinIO.
        """

        self.client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
        )

        print(f"Uploaded {file_path} " f"to {bucket_name}/{object_name}")
