from pathlib import Path


def validate_file_exists(file_path: Path) -> bool:
    return file_path.is_file()


def validate_file_not_empty(file_path: Path) -> bool:
    return file_path.stat().st_size > 0


def validate_source_file(file_path: Path) -> bool:
    file_exists = validate_file_exists(file_path)
    file_not_empty = validate_file_not_empty(file_path)

    return file_exists and file_not_empty
