from asyncio.log import logger
from .base import BaseStorage
import loguru

logger = loguru.logger

try:
    import gdown
except:
    raise Exception("Install gdrive to use GDriveStorage, `pip install gdrive`")

import os


class GDriveStorage(BaseStorage):
    def __init__(self):
        super().__init__()

    def download(
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

    def upload(self):
        raise Exception("GDriveStorage does not support upload right now")
