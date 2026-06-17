from pathlib import Path

import pandas as pd

SOURCE_DIR = Path("data/source/archive")


def profile_csv(file_path: Path):
    df = pd.read_csv(file_path)

    return {
        "file_name": file_path.name,
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
    }


def main():
    csv_files = sorted(SOURCE_DIR.glob("*.csv"))

    for file in csv_files:
        profile = profile_csv(file)

        print("\n" + "=" * 80)
        print(f"FILE: {profile['file_name']}")
        print(f"ROWS: {profile['rows']:,}")
        print(f"COLUMNS: {profile['columns']}")
        print(f"COLUMN NAMES: {profile['column_names']}")


if __name__ == "__main__":
    main()
