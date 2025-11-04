# Storage Service - MinIO/S3 Integration for fetching raw student demographic data

import boto3
import pandas as pd
import io

# MinIO connection settings (local S3-compatible object storage)
MINIO_URL = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "raw"


def _get_s3_client():
    # Creates S3 client configured for MinIO using boto3 (S3-compatible)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_URL,  # Point to MinIO instead of AWS
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    return s3


def get_students():
    # Fetches student demographic data from MinIO 'raw' bucket (students.csv)
    s3 = _get_s3_client()
    file_name = "students.csv"

    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_name)
    file_content = response["Body"].read()

    # Convert bytes to DataFrame using BytesIO buffer
    students = pd.read_csv(io.BytesIO(file_content))
    return students
