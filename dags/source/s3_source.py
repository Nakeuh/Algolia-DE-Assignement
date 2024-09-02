import boto3
import botocore
import logging
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)


def get_s3_client(access_key: str, secret_key: str):
    return boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )


# Retrieve data from S3
def get_data_func(ti, **kwargs):
    s3_client = kwargs["s3_client"]
    bucket_name = kwargs["bucket_name"]
    filepath = kwargs["filepath"]

    logger.info(f"Retrieving data from {bucket_name}/{filepath}.")

    # Read an object from the bucket
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=filepath)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"Object {filepath} doesn't exist in bucket {bucket_name}.")
            raise AirflowSkipException
    else:
        # Read the object's content as text
        object_content = response["Body"].read().decode("utf-8")
        # Process or use the content as needed
        logger.info(f"Data downloaded.")
        ti.xcom_push(key="data", value=object_content)
