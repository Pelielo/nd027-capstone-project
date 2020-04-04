from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import LoadS3
from zipfile import ZipFile
from airflow.models import Variable

os.environ["KAGGLE_USERNAME"] = Variable.get("kaggle_username")
os.environ["KAGGLE_KEY"] = Variable.get("kaggle_api_key")

import kaggle  # uses KAGGLE_USERNAME and KAGGLE_KEY

default_args = {
    "owner": "pelielo",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "weather",
    default_args=default_args,
    description="Loads data to S3 and processes it via Spark into Redshift",
    schedule_interval="@once",
)


def job():
    def download_and_unzip(dataset_owner, dataset_name, save_path, files_to_extract):
        kaggle.api.dataset_download_files(
            dataset=f"{dataset_owner}/{dataset_name}", path=save_path, force=True
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


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_dataset_and_unizp = PythonOperator(
    task_id="download_dataset_and_unizp", dag=dag, python_callable=job
)

upload_to_s3 = LoadS3(
    task_id="load_s3",
    dag=dag,
    filename="weather.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="dend-bucket-2a95",
    s3_key="capstone-project/weather/weather.csv",
)


end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_dataset_and_unizp >> upload_to_s3 >> end_operator
