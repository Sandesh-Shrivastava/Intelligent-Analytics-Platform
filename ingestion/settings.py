"""
Config loader — reads from .env file and returns plain dicts.
Use get_settings() to access all config in one place.
"""

import os
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Missing required env variable: {key}")
    return val.strip().strip("'\"")


@lru_cache(maxsize=1)
def get_settings() -> dict:
    return {
        "aws": {
            "access_key_id":     _require("AWS_ACCESS_KEY_ID"),
            "secret_access_key": _require("AWS_SECRET_ACCESS_KEY"),
            "region":            os.getenv("AWS_REGION", "us-east-1"),
        },
        "s3": {
            "bucket_raw": _require("S3_BUCKET_RAW"),
        },
        "athena": {
            "database":       os.getenv("ATHENA_DATABASE", "raw_olist"),
            "results_bucket": _require("ATHENA_RESULTS_BUCKET"),
        },
        "mlflow": {
            "tracking_uri": os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlflow.db"),
        },
        "ai": {
            "openai_api_key": os.getenv("OPENAI_API_KEY"),
        },
        "app": {
            "env":       os.getenv("APP_ENV", "development"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
        },
    }
