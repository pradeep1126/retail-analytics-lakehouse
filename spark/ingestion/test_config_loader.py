from config.config_loader import load_datasets_config

print("Starting config loader test")


def main():
    config = load_datasets_config()

    for dataset_name, dataset_config in config.items():
        print(f"\nDataset: {dataset_name}")
        print(dataset_config)


if __name__ == "__main__":
    main()
