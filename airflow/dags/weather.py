import json
import os
from datetime import datetime, timedelta
from zipfile import ZipFile

import requests
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators import LoadS3
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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


def fetch_job():
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


def spark_job():
    os.environ["AWS_ACCESS_KEY_ID"] = (
        AwsHook("aws_credentials").get_credentials().access_key
    )
    os.environ["AWS_SECRET_ACCESS_KEY"] = (
        AwsHook("aws_credentials").get_credentials().secret_key
    )

    opencage_api_key = Variable.get("opencage_api_key")

    conf = (
        SparkConf()
        .setAppName("weather_processor")
        .setMaster("spark://spark-master:7077")  # TODO change to connection
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
    )

    def create_spark_session():
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    def concat_coordinates(latitude, longitude):
        return str(latitude) + "," + str(longitude)

    def reverse_geocode(latitude, longitude, api_key):
        url = "https://api.opencagedata.com/geocode/v1/json"
        lat_long = concat_coordinates(latitude, longitude)
        params = {"q": lat_long, "key": api_key}
        return requests.request("GET", url, params=params)

    def state_parser(json_str):
        try:
            return json.loads(json_str.text)["results"][0]["components"]["state"]
        except KeyError:
            return None

    spark = create_spark_session()

    input_data_path = "s3a://udacity-dend-14b1/capstone-project/"
    # input_data_path = "data/"
    # output_data_path = "s3a://udacity-dend-14b1/output_data_project4/"
    # output_data_path = "output_data/"
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

    cleaned_df = df.select(
        round_udf(df.AverageTemperature).alias("avg_temp"),
        round_udf(df.AverageTemperatureUncertainty).alias("avg_temp_uncertainty"),
        df.City.alias("city"),
        geo_udf(df.Latitude).alias("latitude"),
        geo_udf(df.Longitude).alias("longitude"),
        df.dt.alias("date"),
    ).where((df.Country == "United States") & df.AverageTemperature.isNotNull())

    location_df = cleaned_df.select(
        cleaned_df.latitude.alias("tmp_lat"), cleaned_df.longitude.alias("tmp_long")
    ).dropDuplicates()

    # TODO remove limit
    enriched_df = location_df.limit(5).withColumn(
        "state", location_enricher_udf(location_df.tmp_lat, location_df.tmp_long)
    )

    weather_df = cleaned_df.join(
        enriched_df,
        (
            (cleaned_df.latitude == enriched_df.tmp_lat)
            & (cleaned_df.longitude == enriched_df.tmp_long)
        ),
        how="inner",
    ).drop("tmp_lat", "tmp_long")

    weather_df.show()


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

download_dataset_and_unizp = PythonOperator(
    task_id="download_dataset_and_unizp", dag=dag, python_callable=fetch_job
)

upload_to_s3 = LoadS3(
    task_id="load_s3",
    dag=dag,
    filename="GlobalLandTemperaturesByCity.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="udacity-dend-14b1",
    s3_key="capstone-project/weather/weather.csv",
)

spark_processor = PythonOperator(
    task_id="spark_processor", dag=dag, python_callable=spark_job, provide_context=True
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_dataset_and_unizp >> upload_to_s3 >> spark_processor >> end_operator
