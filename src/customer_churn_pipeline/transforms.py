from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_customers(df_customers: DataFrame) -> DataFrame:
    return df_customers.withColumn("signup_date", F.to_date("signup_date"))


def prepare_orders(df_orders: DataFrame) -> DataFrame:
    return df_orders.withColumn("order_date", F.to_date("order_date")).withColumn(
        "amount", F.col("amount").cast("double")
    )


def compute_last_order_date(df_orders: DataFrame) -> DataFrame:
    return df_orders.groupBy("customer_id").agg(
        F.max("order_date").alias("last_order_date")
    )


def compute_orders_last_30d(df_orders: DataFrame, as_of_date: str) -> DataFrame:
    as_of = F.to_date(F.lit(as_of_date))
    return (
        df_orders.filter(F.col("order_date").between(F.date_sub(as_of, 30), as_of))
        .groupBy("customer_id")
        .agg(F.count("*").alias("orders_last_30d"))
    )


def compute_avg_order_value_90d(df_orders: DataFrame, as_of_date: str) -> DataFrame:
    as_of = F.to_date(F.lit(as_of_date))
    return (
        df_orders.filter(F.col("order_date").between(F.date_sub(as_of, 90), as_of))
        .groupBy("customer_id")
        .agg(F.avg("amount").alias("avg_order_value_90d"))
    )


def build_customer_features(
    df_customers: DataFrame,
    df_orders: DataFrame,
    as_of_date: str,
) -> DataFrame:
    as_of = F.to_date(F.lit(as_of_date))

    last_order = compute_last_order_date(df_orders)
    orders_30 = compute_orders_last_30d(df_orders, as_of_date)
    aov_90 = compute_avg_order_value_90d(df_orders, as_of_date)

    return (
        df_customers.select("customer_id")
        .join(last_order, on="customer_id", how="left")
        .join(orders_30, on="customer_id", how="left")
        .join(aov_90, on="customer_id", how="left")
        .withColumn("orders_last_30d", F.coalesce(F.col("orders_last_30d"), F.lit(0)))
        .withColumn(
            "avg_order_value_90d", F.coalesce(F.col("avg_order_value_90d"), F.lit(0.0))
        )
        .withColumn(
            "days_since_last_order", F.datediff(as_of, F.col("last_order_date"))
        )
        .withColumn(
            "is_churn_risk",
            F.when(F.col("days_since_last_order").isNull(), F.lit(1))
            .when(F.col("days_since_last_order") >= 60, F.lit(1))
            .otherwise(F.lit(0)),
        )
        .select(
            "customer_id",
            "days_since_last_order",
            "orders_last_30d",
            "avg_order_value_90d",
            "is_churn_risk",
        )
    )
