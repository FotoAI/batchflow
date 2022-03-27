import os
from typing import List

import loguru

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


class BackBlazeStorage(BaseStorage):
    b2_api = None

    def __init__(self, bucket_name: str):
        super().__init__()
        logger.info(f"Init backblazestorage")
        self.b2_api = None
        self.bucket = None
        self.bucket_name = bucket_name

    def authenticate(self):
        logger.info(f"Authenticating BackBlaze")
        self.b2_api = self.get_b2_api()
        self.bucket = self.b2_api.get_bucket_by_name(self.bucket_name)

    @staticmethod
    def get_b2_api():
        if BackBlazeStorage.b2_api is None:
            logger.info("Init singleton b2api object")
            info = InMemoryAccountInfo()  # store credentials, tokens and cache in memor
            BackBlazeStorage.b2_api = B2Api(info)

            b2_application_key_id = os.getenv("B2_APPLICATION_KEY_ID", None)
            b2_application_key = os.getenv("B2_APPLICATION_KEY", None)
            if b2_application_key is None:
                raise Exception("set your B2_APPLICATION_KEY_ID in environment")
            if b2_application_key is None:
                raise Exception("set your B2_APPLICATION_KEY in environment")

            BackBlazeStorage.b2_api.authorize_account(
                "production", b2_application_key_id, b2_application_key
            )
        return BackBlazeStorage.b2_api

    def upload(self, key, file):
        logger.info(f"uploading {file} to b2://{self.bucket_name}/{key}")
        self.bucket.upload_local_file(file, key)
        logger.info(f"uploaded successful {file} to b2://{self.bucket_name}/{key}")
        return f"b2://{self.bucket_name}/{key}"

    # download files from b2
    def download(
        self, output, key=None, id=None, force=False, workers=3, **kwargs
    ) -> str:
        if self.bucket is None:
            logger.error(f"Call authenticate() first")
            raise Exception("Call authenticate first")

        if id:
            if isinstance(id, list):
                assert len(id) == len(output), "num of output should be same as num ids"
                merge_arguments = [[_id, _output] for _id, _output in zip(id, output)]
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    results = executor.map(self._download_by_id, *zip(*merge_arguments))
                return [r for r in results]
            else:
                return self._download_by_id(id=id, output=output)
        elif key:
            if isinstance(key, list):
                assert len(key) == len(
                    output
                ), "num of output should be same as num ids"
                merge_arguments = [[_id, _output] for _id, _output in zip(id, output)]
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    results = executor.map(
                        self._download_by_key, *zip(*merge_arguments)
                    )
                return [r for r in results]
            else:
                return self._download_by_key(key=key, output=output)

        else:
            raise Exception("Pass key or id")

    def _download_by_key(self, key, output):
        logger.info(f"downloading b2://{self.bucket_name}/{key} to {output}")
        download_dest = DownloadDestLocalFile(output)
        try:
            self.bucket.download_file_by_name(
                file_name=key, download_dest=download_dest
            )
        except b2sdk.exception.FileNotPresent:
            raise StorageFileNotFound(
                f"File b2://{self.bucket_name}/{key} not found in backblaze"
            )
        except b2sdk.exception.BadRequest:
            logger.error(f"Failed to download file id {id}, raise b2sdk.exception.BadRequest Exception")
        return output

    def _download_by_id(self, id, output, retry=2, attempt=0):
        logger.info(f"downloading by id: {id} to {output}")
        download_dest = DownloadDestLocalFile(output)
        try:
            self.bucket.download_file_by_id(file_id=id, download_dest=download_dest)
        except b2sdk.exception.FileNotPresent:
            raise StorageFileNotFound(f"File id: {id} not found in backblaze")
        except b2sdk.exception.BadRequest:
            # if attempt<retry:
            #     self._download_by_id(id, output, retry=retry, attempt=attempt+1)
            logger.error(f"Failed to download file id {id}, raise b2sdk.exception.BadRequest Exception")
            
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
