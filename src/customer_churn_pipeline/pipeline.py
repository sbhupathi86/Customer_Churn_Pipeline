from pyspark.sql import SparkSession

from customer_churn_pipeline.io import read_csv, write_parquet
from customer_churn_pipeline.transforms import (
    build_customer_features,
    prepare_customers,
    prepare_orders,
)


def run_pipeline(
    customers_path: str,
    orders_path: str,
    output_path: str,
    as_of_date: str,
    app_name: str = "customer-churn-pipeline",
) -> None:
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    try:
        customers_df = prepare_customers(read_csv(spark, customers_path))
        orders_df = prepare_orders(read_csv(spark, orders_path))

        features_df = build_customer_features(
            df_customers=customers_df,
            df_orders=orders_df,
            as_of_date=as_of_date,
        )

        write_parquet(features_df, output_path)
    finally:
        spark.stop()
