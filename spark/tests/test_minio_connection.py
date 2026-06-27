from spark.utils.spark_session import get_spark_session


def main():
    spark = get_spark_session("MinIO Smoke Test")

    df = spark.read.option("header", "true").csv(
        "s3a://bronze/aisles/load_date=2026-06-26/aisles.csv"
    )

    df.printSchema()
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
