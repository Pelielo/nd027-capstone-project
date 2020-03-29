import requests 
from zipfile import ZipFile

def download_and_unzip(url, files_to_extract, temp_file='temp.zip'):
    download_url(url, temp_file)
    extract_file_from_zip(temp_file, files_to_extract)


def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def extract_file_from_zip(source_file, files_to_extract):
    # Create a ZipFile Object
    with ZipFile(source_file, 'r') as zip_obj:
        # Extract the necessary files
        for file in files_to_extract:
            zip_obj.extract(file)


url = 'https://simplemaps.com/static/data/us-cities/1.6/basic/simplemaps_uscities_basicv1.6.zip'
csv_file = ['uscities.csv']

download_and_unzip(url, csv_file)

