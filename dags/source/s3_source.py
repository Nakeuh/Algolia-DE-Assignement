import boto3
import botocore
import logging
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

# Create a S3 client using given AK/SK
def get_s3_client(access_key: str, secret_key: str):
    return boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )

# Generic function to read file from a S3 bucket
# TODO : Add unit tests using a Mock of S3 client as 's3_client' parameter
#
# Retrieve data from S3
# expected kwargs :
#   - s3_client : the boto3 s3 client that will be used to read data
#   - bucket_name : the bucket name to read data from
#   - filepath : the path of the file to read in the bucket
# return (XCOM):
#   - data: the content of the file as string
def get_data(ti, **kwargs):
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
        object_content = response["Body"].read().decode("utf-8")    # TODO : check for error while reading/decoding the response
        # Process or use the content as needed
        logger.info(f"Data downloaded.")
        ti.xcom_push(key="data", value=object_content)
