import argparse

from customer_churn_pipeline.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Build customer churn features")
    parser.add_argument("--customers", required=True, help="Path to customers CSV")
    parser.add_argument("--orders", required=True, help="Path to orders CSV")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument(
        "--as-of-date", required=True, help="Feature reference date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--app-name", default="customer-churn-pipeline", help="Spark app name"
    )
    args = parser.parse_args()

    run_pipeline(
        customers_path=args.customers,
        orders_path=args.orders,
        output_path=args.output,
        as_of_date=args.as_of_date,
        app_name=args.app_name,
    )


if __name__ == "__main__":
    main()
