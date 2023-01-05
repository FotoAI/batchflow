import os
from typing import List, Optional

import loguru
import tenacity

from ..errors import StorageFileNotFound
from .base import BaseStorage
from concurrent.futures import ThreadPoolExecutor

try:
    import b2sdk
    from b2sdk.v1 import B2Api, DownloadDestLocalFile, InMemoryAccountInfo
except:
    raise (
        "Install Backblaze python client to use BackBlazeStorage `pip install b2sdk`"
    )

logger = loguru.logger

import requests


class BackBlazeStorage(BaseStorage):
    b2_api = None

    def __init__(
        self,
        bucket_name: str,
        application_key_id: Optional[str] = None,
        application_key: Optional[str] = None,
        force_new=False,
    ):
        super().__init__()
        logger.info(f"Init backblazestorage")
        self.b2_api = None
        self.bucket = None
        self.bucket_name = bucket_name
        self.application_key_id = application_key_id
        self.application_key = application_key
        self.force_new = force_new

    def authenticate(self):
        logger.info(f"Authenticating BackBlaze")
        self.b2_api = self.get_b2_api(
            self.application_key_id, self.application_key, self.force_new
        )
        self.bucket = self.b2_api.get_bucket_by_name(self.bucket_name)

    @staticmethod
    def get_b2_api(application_key_id, application_key, force_new):

        if BackBlazeStorage.b2_api is None or force_new:
            logger.info("Init singleton b2api object")
            info = InMemoryAccountInfo()  # store credentials, tokens and cache in memor
            b2_api = B2Api(info)
            if application_key_id:
                b2_application_key_id = application_key_id
                logger.info("Overring application key id")
            else:
                logger.info("using env application key id")
                b2_application_key_id = os.getenv("B2_APPLICATION_KEY_ID", None)

            if application_key:
                logger.info("Overring application key")
                b2_application_key = application_key
            else:
                logger.info("using env application key")
                b2_application_key = os.getenv("B2_APPLICATION_KEY", None)

            if b2_application_key is None:
                raise Exception("set your B2_APPLICATION_KEY_ID in environment")
            if b2_application_key is None:
                raise Exception("set your B2_APPLICATION_KEY in environment")

            b2_api.authorize_account(
                "production", b2_application_key_id, b2_application_key
            )
            BackBlazeStorage.b2_api = b2_api

        if force_new:
            return b2_api
        else:
            return BackBlazeStorage.b2_api

    def upload(self, key, file):
        logger.info(f"uploading {file} to b2://{self.bucket_name}/{key}")
        self.bucket.upload_local_file(file, key)
        logger.info(f"uploaded successful {file} to b2://{self.bucket_name}/{key}")
        return f"b2://{self.bucket_name}/{key}"

    def upload2(self, key, file):
        logger.info(f"uploading {file} to b2://{self.bucket_name}/{key}")
        file_info = self.bucket.upload_local_file(file, key)
        logger.info(f"uploaded successful {file} to b2://{self.bucket_name}/{key}")
        return file_info

    # download files from b2
    def download(
        self, output, key=None, id=None, force=False, workers=3, skip_not_found=False
    ) -> str:
        if self.bucket is None:
            logger.error(f"Call authenticate() first")
            raise Exception("Call authenticate first")

        if id:
            if isinstance(id, list):
                assert len(id) == len(output), "num of output should be same as num ids"
                merge_arguments = [
                    [_id, _output, skip_not_found] for _id, _output in zip(id, output)
                ]
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    results = executor.map(self._download_by_id, *zip(*merge_arguments))
                return [r for r in results]
            else:
                return self._download_by_id(
                    id=id, output=output, skip_not_found=skip_not_found
                )
        elif key:
            if isinstance(key, list):
                assert len(key) == len(
                    output
                ), "num of output should be same as num ids"
                merge_arguments = [
                    [_id, _output, skip_not_found] for _id, _output in zip(id, output)
                ]
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    results = executor.map(
                        self._download_by_key, *zip(*merge_arguments)
                    )
                return [r for r in results]
            else:
                return self._download_by_key(
                    key=key, output=output, skip_not_found=skip_not_found
                )

        else:
            raise Exception("Pass key or id")

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception(requests.ReadTimeout),
    )
    def _download_by_key(self, key, output, skip_not_found=False):
        logger.info(f"downloading b2://{self.bucket_name}/{key} to {output}")
        download_dest = DownloadDestLocalFile(output)
        try:
            self.bucket.download_file_by_name(
                file_name=key, download_dest=download_dest
            )
        except b2sdk.exception.FileNotPresent:
            if skip_not_found:
                logger.error(f"File id: {id} not found in backblaze")
            else:

                raise StorageFileNotFound(
                    f"File b2://{self.bucket_name}/{key} not found in backblaze"
                )
        except b2sdk.exception.BadRequest:
            logger.error(
                f"Failed to download file id {id}, raise b2sdk.exception.BadRequest Exception"
            )
        return output

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
        retry=tenacity.retry_if_exception_type(
            requests.ReadTimeout,
        ),
    )
    def _download_by_id(self, id, output, skip_not_found=False):
        logger.info(f"downloading by id: {id} to {output}")
        download_dest = DownloadDestLocalFile(output)
        try:
            self.bucket.download_file_by_id(
                file_id=id,
                download_dest=download_dest,
            )
        except b2sdk.exception.FileNotPresent:
            if skip_not_found:
                logger.error(f"File id: {id} not found in backblaze")
            else:
                raise StorageFileNotFound(f"File id: {id} not found in backblaze")
        except b2sdk.exception.BadRequest:
            # if attempt<retry:
            #     self._download_by_id(id, output, retry=retry, attempt=attempt+1)
            logger.error(
                f"Failed to download file id {id}, raise b2sdk.exception.BadRequest Exception"
            )
        except requests.ReadTimeout:
            logger.error("Timeout")
            raise requests.ReadTimeout("Timeout")
        return output

    def list_files(self, key):
        list_files = []
        for f, _ in self.bucket.ls(key):
            list_files.append(f.file_name)
        logger.debug(f"list files --")
        logger.debug(list_files)
        return list_files

    @staticmethod
    def create_index_bucket_path(admin_user_id, event_name):
        key = os.path.join(admin_user_id, "events", event_name, "index")
        return key

    @staticmethod
    def create_photo_bucket_path(admin_user_id, event_name):
        key = os.path.join(admin_user_id, "events", event_name, "raw")
        return key

    @staticmethod
    def create_request_path(admin_user_id, event_name, request_id):
        key = os.path.join(
            admin_user_id, "events", event_name, "pic_request", request_id
        )
        return key
