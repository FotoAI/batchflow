from batchflow.core.node import ProducerNode
from typing import Union, List, Optional
import glob
import os
import numpy as np
import cv2
from batchflow.decorators import log_time


class ImageFolderReader(ProducerNode):
    def __init__(self, path: Union[str,List[str]], formats:Optional[List[str]]=None, max: int=-1, *args, **kwargs):
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
            self.formats = ["jpg", "jpeg", "png", "JPG", "JPEG", "bmp", "webp"]
        else:
            self.formats = formats
        _formats = ",".join(self.formats)
        self.images = glob.glob(os.path.join(path, f"*[{_formats}]"))
        self.max = max

    def __len__(self):
        return len(self.images)

    def open(self):
        self._idx = 0
        self._max_idx = len(self.images) if self.max == -1 else self.max
        self._logger.info(f"Producing {self._max_idx} images")
        if self._max_idx == 0:
            _formats = ",".join(self.formats)
            raise Exception(f"Zero images in the folder {self.path} with extension {_formats}  ")
    
    def close(self):
        self._idx = 0
    
    def _read_image(self) -> np.array:
        if self._idx < self._max_idx:
            img_path = self.images[self._idx]
            image = cv2.imread(img_path)
            # BGR to RGB
            image = image[...,::-1]
            self._idx += 1
            return img_path, image
        else:
            raise StopIteration()

    @log_time    
    def next(self) -> np.array:
        img_path, image = self._read_image()
        return {"image":image, "filename":os.path.basename(img_path), "filepath":img_path}


    @log_time
    def next_batch(self) -> any:
        image_batch = {"image":[], "filename":[], "filepath":[], "batch_size":0}
        self._end_batch = False

        for i in range(self.batch_size):
            try:
                img_path, image = self._read_image()
                image_batch["image"].append(image)
                image_batch["filename"].append(os.path.basename(img_path))
                image_batch["filepath"].append(img_path)
                image_batch["batch_size"]+=1
            except StopIteration:
                self._end_batch = True
                break
        if image_batch["batch_size"]>0:
            return image_batch
        else:
            raise StopIteration
            

    