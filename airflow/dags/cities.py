import os
from datetime import datetime, timedelta

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators import DownloadAndUnzip, LoadS3
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

    os.environ["AWS_ACCESS_KEY_ID"] = AwsHook("aws_credentials").get_credentials().access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = AwsHook("aws_credentials").get_credentials().secret_key

    conf = (
        SparkConf()
        .setAppName("cities_processor")
        .setMaster("spark://spark-master:7077")
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
    )

    def create_spark_session():
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    spark = create_spark_session()

    input_data_path = "s3a://udacity-dend-14b1/capstone-project/"
    # input_data_path = "data/"
    # output_data_path = "s3a://udacity-dend-14b1/output_data_project4/"
    # output_data_path = "output_data/"

    # get path to cities data file
    cities_data_path = "cities/uscities.csv"

    # read cities data file
    df = spark.read.csv(input_data_path + cities_data_path, header=True)

    geography_table = (
        df.selectExpr(
            "city_ascii as city",
            "state_name as state",
            "state_id",
            "'United States' as country",
            "lat as latitude",
            "lng as longitude",
            "density",
        )
        .filter(
            col("state_name").isin(["Puerto Rico", "District of Columbia"]) == False
        )
        .dropDuplicates()
    )
    geography_table.show(5)


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

spark_processor = PythonOperator(
    task_id="spark_processor", dag=dag, python_callable=spark_job, provide_context=True
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> download_and_unizp_csv >> upload_to_s3 >> spark_processor >> end_operator
