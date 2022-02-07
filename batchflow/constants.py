import os
from http.client import PROCESSING
from pathlib import Path

BATCHFLOW_HOME = os.getenv(
    "BATCHFLOW_HOME", default=os.path.join(Path.home(), ".batchflow")
)
MODEL_DOWNLOAD_FOLDER = "models"
GPU = "cuda:0"
CPU = "cpu"
DEVICE_TYPES = [GPU, CPU]

BATCH = 0
REALTIME = 1
MODE = [BATCH, REALTIME]
