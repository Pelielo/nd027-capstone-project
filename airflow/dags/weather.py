import logging
import json
import os
from datetime import datetime, timedelta
from zipfile import ZipFile

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators import LoadS3, CopyToRedshiftOperator, DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

os.environ["KAGGLE_USERNAME"] = Variable.get("kaggle_username")
os.environ["KAGGLE_KEY"] = Variable.get("kaggle_api_key")

import kaggle  # uses KAGGLE_USERNAME and KAGGLE_KEY

WEATHER_VALIDATION = {
    "query": "select count(*) from fact_weather where avg_temp is null",
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
    "weather",
    default_args=default_args,
    description="Loads data to S3 and processes it via Spark into Redshift",
    schedule_interval="@once",
)


def fetch_job():
    """
    Downloads a dataset file from Kaggle using Kaggle's API authenticated with
    `KAGGLE_USERNAME` and `KAGGLE_KEY` envioronment variables. Then, unzips the
    necessary file and saves it to the local filesystem.
    """

    def download_and_unzip(dataset_owner, dataset_name, save_path, files_to_extract):
        kaggle.api.dataset_download_files(
            dataset=f"{dataset_owner}/{dataset_name}", path=save_path, force=True
        )
        extract_file_from_zip(f"{dataset_name}.zip", files_to_extract)

    def extract_file_from_zip(source_file, files_to_extract):
        # create a ZipFile Object
        with ZipFile(source_file, "r") as zip_obj:
            # extract the necessary files
            for file in files_to_extract:
                zip_obj.extract(file)

    dataset_owner = "berkeleyearth"
    dataset_name = "climate-change-earth-surface-temperature-data"
    save_path = "./"
    files_to_extract = ["GlobalLandTemperaturesByCity.csv"]

    logging.info("Authenticating to Kaggle")
    kaggle.api.authenticate()  # uses KAGGLE_USERNAME and KAGGLE_KEY

    logging.info("Downloading dataset file and unzipping into filesystem")
    download_and_unzip(dataset_owner, dataset_name, save_path, files_to_extract)


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

    opencage_api_key = Variable.get("opencage_api_key")

    def create_spark_session():
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName("cities_processor")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
            .config("spark.sql.broadcastTimeout", 1200)
            .getOrCreate()
        )
        return spark

    def concat_coordinates(latitude, longitude):
        return str(latitude) + "," + str(longitude)

    def reverse_geocode(latitude, longitude, api_key):
        """
        Uses Open Cage Web API to reverse geocode a tuple of
        latitude and logitude, resulting in the state in which
        those coordinates are located.

        :param latitude: the latitude part of the coordinate
        :type latitude: str

        :param longitude: the longitude part of the coordinate
        :type longitude: str

        :param api_key: OpenCage's API key to authenticate requests
        :type api_key: str
        """

        url = "https://api.opencagedata.com/geocode/v1/json"
        lat_long = concat_coordinates(latitude, longitude)
        params = {"q": lat_long, "key": api_key}
        return requests.request("GET", url, params=params)

    def state_parser(json_str):
        """
        Parses the response of the reverse geocode request and extracts the state name.

        :param json_str: json response to be parsed
        :type json_str: str
        """
        try:
            return json.loads(json_str.text)["results"][0]["components"]["state"]
        except KeyError:
            return None

    spark = create_spark_session()

    logging.info("Created spark session")

    input_data_path = "s3a://udacity-dend-14b1/capstone-project/"

    # get path to weather data file
    weather_data_path = "weather/usweather.csv"

    # read weather data file
    df = spark.read.csv(input_data_path + weather_data_path, header=True)

    geo_udf = udf(
        lambda x: float(x[0:-1]) if (x[-1] == "N" or x[-1] == "E") else -float(x[0:-1]),
        DoubleType(),
    )
    round_udf = udf(lambda x: round(float(x), 3), DoubleType())
    location_enricher_udf = udf(
        lambda lat, long: state_parser(reverse_geocode(lat, long, opencage_api_key))
    )

    # filters and cleans dataset
    cleaned_df = df.select(
        round_udf(df.AverageTemperature).alias("avg_temp"),
        round_udf(df.AverageTemperatureUncertainty).alias("avg_temp_uncertainty"),
        df.City.alias("city"),
        geo_udf(df.Latitude).alias("latitude"),
        geo_udf(df.Longitude).alias("longitude"),
        df.dt.alias("date"),
    ).where((df.Country == "United States") & df.AverageTemperature.isNotNull())

    # separetes the distinct coordinates to request reverse geocoding
    location_df = cleaned_df.select(
        cleaned_df.latitude.alias("latitude"), cleaned_df.longitude.alias("longitude")
    ).dropDuplicates()

    enriched_df = location_df.withColumn(
        "state", location_enricher_udf(location_df.latitude, location_df.longitude)
    )

    # joins back the source dataframe with the enriched dataframe containing state names
    weather_df = cleaned_df.join(
        enriched_df,
        (
            (cleaned_df.latitude == enriched_df.latitude)
            & (cleaned_df.longitude == enriched_df.longitude)
        ),
        how="inner",
    ).drop("latitude", "longitude")

    logging.info("Filtered dataframe and renamed columns")

    logging.info("Resulting dataframe:")
    weather_df.show(10)

    weather_df.toPandas().to_csv("usweather-processed.csv", header=True, index=False)

    logging.info("Dumped dataframe to CSV file in local filesystem")


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_dataset_and_unizp = PythonOperator(
    task_id="download_dataset_and_unizp", dag=dag, python_callable=fetch_job
)

upload_to_s3_raw = LoadS3(
    task_id="upload_to_s3_raw",
    dag=dag,
    filename="GlobalLandTemperaturesByCity.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/weather/weather.csv",
)

spark_processor = PythonOperator(
    task_id="spark_processor", dag=dag, python_callable=spark_job, provide_context=True
)

upload_to_s3_processed = LoadS3(
    task_id="upload_to_s3_processed",
    dag=dag,
    filename="usweather-processed.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/weather/usweather-processed.csv",
)

copy_redshift = CopyToRedshiftOperator(
    task_id="copy_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="fact_weather",
    column_list="avg_temp, avg_temp_uncertainty, city, date, state",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/weather/usweather-processed.csv",
)

quality_checks = DataQualityOperator(
    task_id="data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    queries_and_results=[WEATHER_VALIDATION],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_dataset_and_unizp >> upload_to_s3_raw >> spark_processor >> upload_to_s3_processed >> copy_redshift >> quality_checks >> end_operator
