from .base import BaseStorage
import os
import loguru

try:
    from b2sdk.v1 import B2Api
    from b2sdk.v1 import InMemoryAccountInfo
    from b2sdk.v1 import B2Api
    import b2sdk
    from b2sdk.v1 import DownloadDestLocalFile
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
        logger.info(
            f"uploaded successful {file} to b2://{self.bucket_name}/{key}"
        )
        return f"b2://{self.bucket_name}/{key}"

    def download(self,output, key=None, id=None) -> str:
        if self.bucket is None:
            logger.error(f"Call authenticate() first")
            raise Exception("Call authenticate first")

        if key is not None:
            self._download_by_key(key=key,output=output )

            

    # download files from b2
    def _download_by_key(self, key, output):
        if not self.isfile(output):
            logger.info(f"downloading b2://{self.bucket_name}/{key} to {output}")
            download_dest = DownloadDestLocalFile(output)
            try:
                self.bucket.download_file_by_name(
                    file_name=key, download_dest=download_dest
                )
            except b2sdk.exception.FileNotPresent:
                raise FileNotFoundError(
                    f"File b2://{self.bucket_name}/{key} not found in backblaze"
                )
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
