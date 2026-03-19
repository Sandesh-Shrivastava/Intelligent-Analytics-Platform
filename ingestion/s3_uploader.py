"""
Uploads all raw CSVs from data/raw/ to S3.

Usage:
    python ingestion/s3_uploader.py
"""

import boto3
import sys
from pathlib import Path
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
)

# Make project root importable even when launched from ingestion/ or elsewhere.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.settings import get_settings

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
S3_PREFIX    = "olist/raw/"


def get_s3_client():
    cfg = get_settings()
    return boto3.client(
        "s3",
        aws_access_key_id=cfg["aws"]["access_key_id"],
        aws_secret_access_key=cfg["aws"]["secret_access_key"],
        region_name=cfg["aws"]["region"],
    )


def upload_raw_files() -> None:
    try:
        cfg = get_settings()
    except EnvironmentError as exc:
        raise EnvironmentError(
            f"{exc}. Ensure .env exists at '{PROJECT_ROOT / '.env'}' and has required keys."
        ) from exc

    # Guard against accidental quoting in .env values, e.g. S3_BUCKET_RAW='my-bucket'
    bucket    = cfg["s3"]["bucket_raw"].strip().strip("'\"")
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSVs in '{RAW_DATA_DIR}'. Run generate_demo_data.py first.")

    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "Unknown"))
        if code in {"301", "PermanentRedirect", "AuthorizationHeaderMalformed"}:
            raise RuntimeError(
                f"S3 bucket region mismatch for '{bucket}'. Check AWS_REGION in .env."
            ) from exc
        if code in {"403", "AccessDenied"}:
            raise RuntimeError(
                f"Access denied to bucket '{bucket}'. Ensure s3:PutObject and s3:ListBucket permissions."
            ) from exc
        if code in {"404", "NoSuchBucket"}:
            raise RuntimeError(f"S3 bucket '{bucket}' does not exist.") from exc
        raise RuntimeError(f"Unable to access S3 bucket '{bucket}': {exc}") from exc

    print(f"Uploading {len(csv_files)} files -> s3://{bucket}/{S3_PREFIX}\n")

    for path in csv_files:
        table_name = path.stem   # filename without .csv
        key = f"{S3_PREFIX}{table_name}/{path.name}"
        try:
            s3.upload_file(str(path), bucket, key)
            print(f"  [OK] {path.name}")
        except S3UploadFailedError as exc:
            raise RuntimeError(
                f"Upload failed for '{path.name}' to s3://{bucket}/{key}: {exc}"
            ) from exc
        except (NoCredentialsError, PartialCredentialsError) as exc:
            raise RuntimeError(
                "AWS credentials are missing or incomplete. Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
            ) from exc
        except EndpointConnectionError as exc:
            raise RuntimeError(
                "Cannot reach S3 endpoint. Check network access and AWS_REGION."
            ) from exc
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", "Unknown"))
            raise RuntimeError(
                f"AWS error while uploading '{path.name}' (code={code}): {exc}"
            ) from exc

    print(f"\nDone. {len(csv_files)} files uploaded.")


if __name__ == "__main__":
    upload_raw_files()
