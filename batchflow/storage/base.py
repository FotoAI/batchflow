import os
from abc import abstractmethod
from pathlib import Path

import loguru

from batchflow import constants as C

logger = loguru.logger


class BaseStorage:
    def __init__(self):
        self.initalize_paths()

    def initalize_paths(self):
        root = self.get_download_root()
        if not os.path.isdir(root):
            logger.info(f"Creating model download directory {root}")
            os.makedirs(root, exist_ok=True)

    def get_download_root(self) -> str:
        download_root = os.path.join(C.BATCHFLOW_HOME, C.MODEL_DOWNLOAD_FOLDER)
        return download_root

    def isfile(self, path):
        if os.path.isfile(path):
            logger.info(f"Skip download, File {path} already exist")
            return True
        return False

    @abstractmethod
    def download(self) -> str:
        NotImplementedError("Implement this method in subclass")

    @abstractmethod
    def upload(self):
        NotImplementedError("Implement this method in subclass")
