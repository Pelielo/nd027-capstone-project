import logging
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import (
    CopyToRedshiftOperator,
    DownloadAndUnzip,
    LoadS3,
    DataQualityOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

CITIES_VALIDATION = {
    "query": "select count(*) from dim_cities where city is null",
    "result": 0,
}

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


def spark_job(ds, **kwargs):
    """
    Processes all of the necessary steps using Spark and is used as entrypoint for a Python Operator task.
    Can use Airflow context.
    """

    os.environ["AWS_ACCESS_KEY_ID"] = (
        AwsHook("aws_credentials").get_credentials().access_key
    )
    os.environ["AWS_SECRET_ACCESS_KEY"] = (
        AwsHook("aws_credentials").get_credentials().secret_key
    )

    logging.info("Loaded AWS credentials to environment variables")

    def create_spark_session():
        """
        Creates a spark session connecting to the master node and adds necessary packages.
        """

        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("cities_processor")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
            .getOrCreate()
        )
        return spark

    spark = create_spark_session()

    logging.info("Created spark session")

    input_data_path = "s3a://udacity-dend-14b1/capstone-project/"

    # get path to cities data file
    cities_data_path = "cities/uscities.csv"

    # read cities data file
    df = spark.read.csv(input_data_path + cities_data_path, header=True)

    logging.info(f"Read {input_data_path + cities_data_path} into spark dataframe")

    geography_df = (
        df.selectExpr(
            "city_ascii as city",
            "state_name as state",
            "state_id as state_code",
            "'United States of America' as country",
            "lat as latitude",
            "lng as longitude",
            "density",  # population per square kilometer
        )
        .filter(
            col("state_name").isin(["Puerto Rico", "District of Columbia"])
            == False  # using only the 50 states
        )
        .dropDuplicates()
    )

    logging.info("Filtered dataframe and renamed columns")

    logging.info("Resulting dataframe:")
    geography_df.show(10)

    geography_df.toPandas().to_csv("uscities-processed.csv", header=True, index=False)

    logging.info("Dumped dataframe to CSV file in local filesystem")


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_and_unizp_csv = DownloadAndUnzip(
    task_id="download_and_unzip",
    dag=dag,
    url="https://simplemaps.com/static/data/us-cities/1.6/basic/simplemaps_uscities_basicv1.6.zip",
    files_to_extract=["uscities.csv"],
)

upload_to_s3_raw = LoadS3(
    task_id="upload_to_s3_raw",
    dag=dag,
    filename="uscities.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/cities/uscities.csv",
)

spark_processor = PythonOperator(
    task_id="spark_processor", dag=dag, python_callable=spark_job, provide_context=True
)

upload_to_s3_processed = LoadS3(
    task_id="upload_to_s3_processed",
    dag=dag,
    filename="uscities-processed.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/cities/uscities-processed.csv",
)

copy_redshift = CopyToRedshiftOperator(
    task_id="copy_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="dim_cities",
    column_list="city, state, state_code, country, latitude, longitude, density",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/cities/uscities-processed.csv",
)

quality_checks = DataQualityOperator(
    task_id="data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    queries_and_results=[CITIES_VALIDATION],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_and_unizp_csv >> upload_to_s3_raw >> spark_processor >> upload_to_s3_processed >> copy_redshift >> quality_checks >> end_operator
