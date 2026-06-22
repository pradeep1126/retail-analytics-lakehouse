from pathlib import Path


def generate_md5_checksum(file_path: Path) -> str:
    """
    Generate MD5 checksum for a given file.
    """
    print(f"Generating MD5 checksum for file: {file_path}")
    import hashlib

    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
