"""
Shared Athena query helper.
Reads AWS credentials directly from environment variables.
"""

import os
import pandas as pd
from pyathena import connect
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return connect(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        s3_staging_dir=os.getenv("ATHENA_RESULTS_BUCKET"),
        schema_name="dbt_dev",
    )


def query(sql: str) -> pd.DataFrame:
    """Run a SQL query against Athena and return a DataFrame."""
    with get_connection() as conn:
        return pd.read_sql(sql, conn)