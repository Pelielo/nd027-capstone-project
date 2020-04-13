import logging
from zipfile import ZipFile

import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DownloadAndUnzip(BaseOperator):
    """
    Custom Airflow Operator to download a file via http and unzip specific files to filesystem.

    :param url: Url of the source file do download
    :type url: str

    :param files_to_extract: List of file names to extract from the downloaded file.
    :type files_to_extract: list(str)
    """

    ui_color = "#03f4fc"

    @apply_defaults
    def __init__(self, url="", files_to_extract=[""], *args, **kwargs):

        super(DownloadAndUnzip, self).__init__(*args, **kwargs)
        self.url = url
        self.files_to_extract = files_to_extract

    def download_url(self, url, save_path, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, "wb") as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

    def extract_file_from_zip(self, source_file, files_to_extract):
        # Create a ZipFile Object
        with ZipFile(source_file, "r") as zip_obj:
            # Extract the necessary files
            for file in files_to_extract:
                zip_obj.extract(file)

    def execute(self, context):
        temp_file = "temp.zip"

        logging.info(f"Downloading file from {self.url} and saving to {temp_file}")

        self.download_url(self.url, temp_file)

        logging.info(f"Extracting files {self.files_to_extract} from {temp_file}")

        self.extract_file_from_zip(temp_file, self.files_to_extract)
