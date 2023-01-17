from typing import Any, Dict, List

import numpy as np

from batchflow.core.node import ProcessorNode
from batchflow.storage import get_storage
from batchflow.storage.base import BaseStorage


class ModelProcessor(ProcessorNode):
    def __init__(
        self,
        model_path: str = None,
        model_source: Dict[str, str] = None,
        *args,
        **kwargs,
    ) -> None:
        """
        model_path: Model Path
        model_source:
            source: S3 / backblaze / gdrive

            =================================
                       BackBlaze
            =================================

            model_source:
                source: str backblaze
                bucket_name: str Name of the Bucket
                key: str Key to the target file
                filename: str local filepath with name after download
        """
        super().__init__(*args, **kwargs)
        if model_path is not None:
            self.model_path = model_path
        elif model_source is not None:
            self.model_path = self.download_model(model_source)
        self.model = None

    def preprocess(self, image: np.asarray):
        self._logger.warning(f"No preprocessing applied passed input image as it is")
        return image

    def postprocess(self, input: Any):
        self._logger.warning(f"No post processing applied passed input as it is")
        return input

    def predict(self, input: Any):
        raise NotImplemented("Implement this to predict model output in subclass")

    def download_model(self, model_source: Dict[str, str]) -> str:
        source: str = model_source["source"].lower()
        if source == "backblaze":
            bucket_name = model_source["bucket_name"]

            storage: BaseStorage = get_storage("backblaze", bucket_name=bucket_name)
            storage.authenticate()

            model_key: str = model_source["key"]
            filename: str = model_source["filename"]
            model_path = storage.download(key=model_key, output=filename)
        elif source == "gdrive":
            storage: BaseStorage = get_storage("gdrive")
            storage.authenticate()

            id = model_source.get("id", None)
            url = model_source.get("url", None)
            filename: str = model_source.get("filename")
            model_path = storage.download(id=id, url=url, filename=filename)
        else:
            raise Exception(f"Storage {source} not supported for model download")

        return model_path
