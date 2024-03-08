import boto3
from .base import BaseStorage
import tenacity
import requests
from loguru import logger
from ..errors import StorageFileNotFound
import botocore
import io


retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
    retry=tenacity.retry_if_exception_type(
        (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError)
    ),
    reraise=True,
)

download_retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=4),
    retry=tenacity.retry_if_exception_type(
        (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError)
    ),
    reraise=True,
)


class S3(BaseStorage):
    def __init__(
        self,
        bucket_name: str,
    ):
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name

    # @download_retry
    def download(self, output, key: str = None, skip_not_found=False) -> str:
        try:
            self.s3_client.download_file(self.bucket_name, key, output)
            return output
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "NoSuchKey" or error_code == "404":
                if skip_not_found:
                    logger.error(
                        f"File s3://{self.bucket_name}/{key} not found in aws s3"
                    )
                else:
                    logger.error(f"{key} error_code: {error_code}")
                    raise StorageFileNotFound(
                        f"File s3://{self.bucket_name}/{key} not found in aws s3"
                    )
        except Exception as e:
            logger.error(e)
            raise e

    # @download_retry
    def load(
        self,
        key,
        skip_not_found=False,
    ):
        try:
            s3_response_object = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=key
            )
            object_content = s3_response_object["Body"].read()
            return io.BytesIO(object_content)
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(error_code)
            if error_code == "NoSuchKey" or error_code == "404":
                if skip_not_found:
                    logger.error(
                        f"File s3://{self.bucket_name}/{key} not found in aws s3"
                    )
                else:
                    raise StorageFileNotFound(
                        f"File s3://{self.bucket_name}/{key} not found in aws s3"
                    )
        except Exception as e:
            logger.error(e)
            raise e

    # @retry
    def upload(self, key, file):
        # Upload the file to S3
        try:
            return self.s3_client.put_object(
                Body=file, Bucket=self.bucket_name, Key=key
            )
        except Exception as e:
            logger.error(e)
            raise e
