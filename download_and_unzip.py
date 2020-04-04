import requests
from zipfile import ZipFile
import os

os.environ["KAGGLE_USERNAME"] = "pelielo"
os.environ["KAGGLE_KEY"] = "ace0359d7c3cd1cf2d024cb0f76e5966"

import kaggle  # uses KAGGLE_USERNAME and KAGGLE_KEY


def download_and_unzip(dataset_owner, dataset_name, save_path, files_to_extract):
    kaggle.api.dataset_download_files(
        dataset=f"{dataset_owner}/{dataset_name}",
        path=save_path,
        force=True
    )

    extract_file_from_zip(f"{dataset_name}.zip", files_to_extract)


def extract_file_from_zip(source_file, files_to_extract):
    # Create a ZipFile Object
    with ZipFile(source_file, "r") as zip_obj:
        # Extract the necessary files
        for file in files_to_extract:
            zip_obj.extract(file)


dataset_owner = "berkeleyearth"
dataset_name = "climate-change-earth-surface-temperature-data"
save_path = "./"
files_to_extract = ["GlobalLandTemperaturesByCity.csv"]

kaggle.api.authenticate()  # uses KAGGLE_USERNAME and KAGGLE_KEY
download_and_unzip(dataset_owner, dataset_name, save_path, files_to_extract)