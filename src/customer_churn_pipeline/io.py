from pyspark.sql import DataFrame, SparkSession


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.csv(path, header=True, inferSchema=True)


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)
