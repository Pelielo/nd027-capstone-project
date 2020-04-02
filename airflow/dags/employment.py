from datetime import datetime, timedelta
import os
from dateutil.parser import parse
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import LoadS3
import json
from airflow import AirflowException


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
    "employment",
    default_args=default_args,
    description="Loads data to S3 and processes it via Spark into Redshift",
    schedule_interval="@once",
)


def job(ds, **kwargs):
    reference_date = parse(ds).date()

    start_year = "2019"
    end_year = "2020"

    prefix = "SM"  # State and Area Employment
    seasonal_adjustment_code = "U"
    state_codes = ["01", "02", "04", "05", "06", "08", "09", "10", "12", "13", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"]
    area_code = "00000"  # State-wide
    supersector_industry_code = "00000000"  # Total non-farm
    data_type_code = "01"  # All Employees, In Thousands

    series_id = []
    for state_code in state_codes:
        series_id = series_id + [
            (
                prefix
                + seasonal_adjustment_code
                + state_code
                + area_code
                + supersector_industry_code
                + data_type_code
            )
        ]

    print(series_id)

    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    payload = {"seriesid": series_id, "startyear": start_year, "endyear": end_year}
    headers = {"Content-type": "application/json"}

    print(payload)
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))

    print(response.status_code)
    if response.status_code != 200:
        raise AirflowException

    with open("employment.json", "w") as outfile:
        json.dump(json.loads(response.text), outfile)


start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

http_request = PythonOperator(
    task_id="http_request", dag=dag, python_callable=job, provide_context=True
)

upload_to_s3 = LoadS3(
    task_id="load_s3",
    dag=dag,
    filename="uscities.csv",
    s3_credentials_id="s3_conn",
    s3_bucket="dend-bucket-2a95",
    s3_key="capstone-project/uscities.csv",
)


end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> http_request >> end_operator
