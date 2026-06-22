from utils.checksum import generate_md5_checksum


def test_generate_md5_checksum():
    # Create a temporary file for testing
    test_file_path = "data/source/archive/aisles.csv"

    # Generate checksum for the test file
    checksum = generate_md5_checksum(test_file_path)

    # Print the generated checksum
    print(f"MD5 Checksum for {test_file_path}: {checksum}")


if __name__ == "__main__":
    test_generate_md5_checksum()
