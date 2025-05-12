from dganalytics.utils.utils import get_secret
import boto3
import botocore
from datetime import datetime
import os


def getS3Client():
    accessKey = get_secret("S3AccessKey")
    secretKey = get_secret("S3SecretKey")
    region = get_secret("S3Region")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=accessKey,
        aws_secret_access_key=secretKey,
        region_name=region,
    )
    return s3_client


def push_file(folder_name: str, file_path: str, tenant: str, suffix: str,logger):
    try:
        bucket_name = get_secret("S3BucketName")
        s3_client = getS3Client()
        s3_key = os.path.join(
            folder_name,
            tenant,
            datetime.now().strftime("%Y%m%d"),
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.{suffix}.csv",
        )  # Path in the S3 bucket

        with open(file_path, "rb") as file_data:
            s3_client.upload_fileobj(file_data, bucket_name, s3_key)
        logger.info(f"Batch file uploaded successfully to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.exception(f"Error uploading batch {file_path} to S3: {e}")
        raise Exception