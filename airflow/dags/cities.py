from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadS3, DownloadAndUnzip
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
    DateType,
)

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
    "cities",
    default_args=default_args,
    description="Loads data to S3 and processes it via Spark into Redshift",
    schedule_interval="@once",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_and_unizp_csv = DownloadAndUnzip(
    task_id="download_and_unzip",
    dag=dag,
    url="https://simplemaps.com/static/data/us-cities/1.6/basic/simplemaps_uscities_basicv1.6.zip",
    files_to_extract=["uscities.csv"],
)

upload_to_s3 = LoadS3(
    task_id="load_s3",
    dag=dag,
    filename="uscities.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/cities/uscities.csv",
)

spark_submit = BashOperator(
    task_id="spark_submit",
    dag=dag,
    bash_command=f"""
        AWS_ACCESS_KEY_ID={AwsHook("aws_credentials").get_credentials().access_key} \
        AWS_SECRET_ACCESS_KEY={AwsHook("aws_credentials").get_credentials().secret_key} \
        spark-submit --master spark://spark-master:7077 --packages org.apache.hadoop:hadoop-aws:2.7.1 --name test /usr/local/airflow/dags/scripts/spark_script.py""",
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_and_unizp_csv >> upload_to_s3 >> spark_submit >> end_operator
