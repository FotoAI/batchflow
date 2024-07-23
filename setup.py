"""Setup script for Batchflow"""

import os.path

import setuptools
from setuptools import setup

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()

__version__ = "1.1.1"  # set __version__ in this exec() call
exec(open("batchflow/version.py").read())
# This call to setup() does all the work

setup(
    name="batchflow",
    version=__version__,
    description="Foto Owl Computer Vision Library",
    long_description=README,
    long_description_content_type="text/markdown",
    url="",
    author="Tushar Kolhe",
    author_email="tusharkolhe08@gmail.com",
    license="MIT",
    packages=setuptools.find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        "numpy==1.26.4",
        # "opencv-python-headless==4.10.0.84",
        "loguru==0.5.3",
        # "Pillow==10.4.0",
        "requests==2.32.3",
        "catalogue==2.0.0",
        "tenacity==8.5.0",
        "b2sdk==1.21.0",
        "boto3==1.34.32",
    ],
)
