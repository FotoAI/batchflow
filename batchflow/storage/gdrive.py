from typing import Callable, List, Optional, Union, Dict
from .base import BaseStorage
import loguru

logger = loguru.logger

try:
    import gdown
    from googleapiclient.http import MediaIoBaseDownload
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

except:
    raise Exception("Install gdrive to use GDriveStorage, `pip install gdrive`")
import os
import cv2 as cv
import numpy as np
import io


class GDriveStorage(BaseStorage):
    def __init__(
        self,
    ):
        super().__init__()
        self._service = None

    def authenticate(
        self,
        access_token: str = None,
        refresh_token: str = None,
        update_callback: Optional[Union[Callable, List[Callable]]] = None,
        fail_callback: Optional[Union[Callable, List[Callable]]] = None,
    ):
        self._service = self._get_service(
            creds={"access_token": access_token, "refresh_token": refresh_token},
            update_callback=update_callback,
            fail_callback=fail_callback,
        )

    def download(self, id=None, private=False, *args, **kwargs) -> str:
        if not private:
            logger.debug(f"Downloading from public")
            local_file = self._download_public_file(id=id, *args, **kwargs)
        else:
            logger.debug(f"Downloading access protected file")
            local_file = self._download_access_protected_file(id=id, *args, **kwargs)
        return local_file


    # TODO: pass full local path
    def _download_public_file(
        self,
        filename,
        url=None,
        quiet=False,
        proxy=None,
        speed=None,
        use_cookies=True,
        verify=True,
        id=None,
        fuzzy=False,
        resume=False,
    ) -> str:
        local_file = os.path.join(self.get_download_root(), filename)
        if not self.isfile(local_file):
            logger.info(f"downloading {id} | {url} to {local_file}")
            gdown.download(
                id=id,
                url=url,
                quiet=quiet,
                proxy=proxy,
                speed=speed,
                use_cookies=use_cookies,
                verify=verify,
                fuzzy=fuzzy,
                resume=resume,
                output=local_file,
            )
        return local_file

    def _download_access_protected_file(self, id: str, output: str):
        if self._service is not None:

            request = self._service.files().get_media(fileId=id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            # file_bytes = np.asarray(bytearray(fh.read()), dtype=np.uint8)
            with open(output,"wb") as f:
                f.write(fh.read())
            logger.info(f"downloaded file to {output}")
            return output
            # img = cv.imdecode(file_bytes, cv.IMREAD_COLOR)
        else:
            raise Exception(
                "Cannot download access protected files, Call authenticate first"
            )
        

    def _get_service(self, creds: Dict[str, str], update_callback=None, fail_callback=None):
        client_id = os.environ.get("client_id", None)
        client_secret = os.environ.get("client_secret", None)
        if client_id is None or client_secret is None:
            logger.error(f"client_id or client_secret not found")
            raise Exception(f"client_id or client_secret not found")

        creds["client_id"] = client_id
        creds["client_secret"] = client_secret
        logger.info("Authenticating google drive service")
        credentials = Credentials.from_authorized_user_info(info=creds, scopes=SCOPES)

        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
                access_token = credentials.token
                refresh_token = credentials.refresh_token
                # update_cred(
                #     collaborator_id=collaborator_id,
                #     cred={"access_token": access_token, "refresh_token": refresh_token},
                # )

                # call update callbacks
                if update_callback is not None:
                    if not isinstance(update_callback, List):
                        update_callback = [update_callback]
                    for callback in update_callback:
                        callback(access_token=access_token, refresh_token=refresh_token)

            else:
                # If unable to update call fail callback
                if fail_callback is not None:
                    if not isinstance(fail_callback, List):
                        fail_callback = [fail_callback]
                    # flush_cred(collaborator_id=collaborator_id)
                    for callback in fail_callback:
                        callback()

                return None

        service = build("drive", "v3", credentials=credentials)
        return service

    def upload(self):
        raise Exception("GDriveStorage does not support upload right now")
