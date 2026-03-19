"""
Drops and recreates all Athena external tables pointing to exact CSV files in S3.

Usage:
    python ingestion/athena_uploader.py
"""

import time
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

# Table names match exactly what generate_demo_data.py saves
TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "customers_dataset": [
        ("customer_id",              "string"),
        ("customer_unique_id",       "string"),
        ("customer_zip_code_prefix", "string"),
        ("customer_city",            "string"),
        ("customer_state",           "string"),
    ],
    "sellers_dataset": [
        ("seller_id",              "string"),
        ("seller_zip_code_prefix", "string"),
        ("seller_city",            "string"),
        ("seller_state",           "string"),
    ],
    "products_dataset": [
        ("product_id",                 "string"),
        ("product_category_name",      "string"),
        ("product_name_lenght",        "string"),
        ("product_description_lenght", "string"),
        ("product_photos_qty",         "string"),
        ("product_weight_g",           "string"),
        ("product_length_cm",          "string"),
        ("product_height_cm",          "string"),
        ("product_width_cm",           "string"),
    ],
    "orders_dataset": [
        ("order_id",                       "string"),
        ("customer_id",                    "string"),
        ("order_status",                   "string"),
        ("order_purchase_timestamp",       "string"),
        ("order_approved_at",              "string"),
        ("order_delivered_carrier_date",   "string"),
        ("order_delivered_customer_date",  "string"),
        ("order_estimated_delivery_date",  "string"),
    ],
    "order_items_dataset": [
        ("order_id",            "string"),
        ("order_item_id",       "string"),
        ("product_id",          "string"),
        ("seller_id",           "string"),
        ("shipping_limit_date", "string"),
        ("price",               "string"),
        ("freight_value",       "string"),
    ],
    "order_payments_dataset": [
        ("order_id",             "string"),
        ("payment_sequential",   "string"),
        ("payment_type",         "string"),
        ("payment_installments", "string"),
        ("payment_value",        "string"),
    ],
    "order_reviews_dataset": [
        ("review_id",               "string"),
        ("order_id",                "string"),
        ("review_score",            "string"),
        ("review_comment_title",    "string"),
        ("review_comment_message",  "string"),
        ("review_creation_date",    "string"),
        ("review_answer_timestamp", "string"),
    ],
    "geolocation_dataset": [
        ("geolocation_zip_code_prefix", "string"),
        ("geolocation_lat",             "string"),
        ("geolocation_lng",             "string"),
        ("geolocation_city",            "string"),
        ("geolocation_state",           "string"),
    ],
    "product_category_name_translation": [
        ("product_category_name",         "string"),
        ("product_category_name_english", "string"),
    ],
}


def get_athena_client():
    return boto3.client(
        "athena",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def run_query(athena, query: str, results_bucket: str) -> None:
    resp         = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={"OutputLocation": results_bucket},
    )
    execution_id = resp["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            print(f"  ⚠️  {state}: {reason}")
            return
        time.sleep(1)


def register_all_tables() -> None:
    athena         = get_athena_client()
    database       = "raw_olist"
    results_bucket = os.getenv("ATHENA_RESULTS_BUCKET")
    bucket         = os.getenv("S3_BUCKET_RAW")
    prefix         = "olist/raw"

    # Create database
    print(f"Creating database '{database}'...")
    run_query(athena, f"CREATE DATABASE IF NOT EXISTS {database}", results_bucket)
    print(f"  ✓ Done\n")

    # Drop and recreate each table
    print(f"Registering {len(TABLE_SCHEMAS)} tables...\n")
    for table, columns in TABLE_SCHEMAS.items():
        col_defs    = ",\n  ".join(f"`{col}` {dtype}" for col, dtype in columns)
        s3_location = f"s3://{bucket}/{prefix}/{table}/"

        run_query(athena, f"DROP TABLE IF EXISTS {database}.{table}", results_bucket)

        ddl = f"""
        CREATE EXTERNAL TABLE {database}.{table} (
          {col_defs}
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ('separatorChar'=',', 'quoteChar'='"')
        STORED AS TEXTFILE
        LOCATION '{s3_location}'
        TBLPROPERTIES ('skip.header.line.count'='1')
        """
        run_query(athena, ddl, results_bucket)
        print(f"  ✓ {table}  →  {s3_location}")

    print(f"\n✓ All tables registered in '{database}'")
    print(f"\nTest it:")
    print(f"  SELECT COUNT(*) FROM {database}.orders_dataset;")


if __name__ == "__main__":
    register_all_tables()