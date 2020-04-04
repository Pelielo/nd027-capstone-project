from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadS3, DownloadAndUnzip)


default_args = {
    'owner': 'pelielo',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('cities',
          default_args=default_args,
          description='Loads data to S3 and processes it via Spark into Redshift',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

download_and_unizp_csv = DownloadAndUnzip(
    task_id='download_and_unzip',
    dag=dag,
    url='https://simplemaps.com/static/data/us-cities/1.6/basic/simplemaps_uscities_basicv1.6.zip',
    files_to_extract=['uscities.csv']
)

upload_to_s3 = LoadS3(
    task_id='load_s3',
    dag=dag,
    filename="uscities.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="dend-bucket-2a95",
    s3_key="capstone-project/cities/uscities.csv"
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> download_and_unizp_csv >> upload_to_s3 >> end_operator