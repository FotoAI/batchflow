from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import glob
import os
from typing import List, Optional, Union

import cv2
import numpy as np

from batchflow.core.node import ProducerNode
from batchflow.decorators import log_time
from loguru import logger

def _read_image(img_path) -> np.array:
    logger.debug(f"producing {img_path}")
    image = cv2.imread(img_path)
    # BGR to RGB
    image = image[..., ::-1]
    return img_path, image

class ImageFolderReader(ProducerNode):
    def __init__(
        self,
        path: Union[str, List[str]],
        formats: Optional[List[str]] = None,
        max: int = -1,
        *args,
        **kwargs,
    ):
        """
        Reads image from folder

        Args:
            path (Union[str,List[str]]): path to image folder
            formats (Optional[List[str]], optional): allowed image formats. Defaults to ["jpg", "jpeg", "png", "JPG", "JPEG", "bmp", "webp"].
            max (int, optional): max images to read, pass -1 to read all the images. Defaults to -1.
        """
        super().__init__(*args, **kwargs)
        self.path = path
        if not formats:
            self.formats = [".jpg", ".jpeg", ".png", ".JPG", ".JPEG"]
        else:
            self.formats = formats
        self.max = max

    def _is_image_file(self, filename):
        if os.path.splitext(filename)[-1] in self.formats:
            return True
        return False

    def __len__(self):
        return len(self.images)

    def open(self):
        self.images = [
            p
            for p in glob.glob(os.path.join(self.path, f"*"))
            if self._is_image_file(os.path.basename(p))
        ]

        self._idx = 0
        if len(self.images) == 0:
            _formats = ",".join(self.formats)
            raise Exception(
                f"Zero images in the folder {self.path} with extension {_formats}  "
            )

        self._max_idx = (
            len(self.images) if self.max == -1 else min(len(self.images), self.max)
        )
        self.images_arr = np.array(self.images)
        self._logger.info(f"Producing {self._max_idx} images")

    def close(self):
        self._idx = 0

    def _read_image(self) -> np.array:
        if self._idx < self._max_idx:
            img_path = self.images[self._idx]
            self._logger.debug(f"producing {img_path}")
            image = cv2.imread(img_path)
            # BGR to RGB
            image = image[..., ::-1]
            self._idx += 1
            return img_path, image
        else:
            raise StopIteration()

    @log_time
    def next(self) -> np.array:
        img_path, image = self._read_image()
        return {
            "image": image,
            "filename": os.path.basename(img_path),
            "filepath": img_path,
        }

    @log_time
    def next_batch(self) -> any:
        image_batch = {"image": [], "filename": [], "filepath": [], "batch_size": 0}
        self._end_batch = False

        # for i in range(self.batch_size):
        img_paths = self.images_arr[self._idx: self._idx+self.batch_size]
        self._idx += len(img_paths)
        if len(img_paths)!=0:
            with ThreadPoolExecutor(self.batch_size) as e:
                images = e.map(_read_image, img_paths)
            
            for img_path, image in images:
                image_batch["image"].append(image)
                image_batch["filename"].append(os.path.basename(img_path))
                image_batch["filepath"].append(img_path)
                image_batch["batch_size"] += 1
            return image_batch
        else:
            raise StopIteration