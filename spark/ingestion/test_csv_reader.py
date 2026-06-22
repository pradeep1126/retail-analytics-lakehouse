from pathlib import Path

from readers.csv_reader import get_csv_metadata


def test_get_csv_metadata():
    # Path to the sample CSV file
    sample_csv_path = Path("data/source/archive/aisles.csv")
    # Get metadata for the sample CSV file
    metadata = get_csv_metadata(sample_csv_path)

    # Print the metadata
    print(f"Metadata for {sample_csv_path}:")
    print(metadata)


if __name__ == "__main__":
    test_get_csv_metadata()
