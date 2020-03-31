from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
import os

class LoadS3(BaseOperator):
    
    ui_color = '#03f4fc'

    # template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 filename="",
                 s3_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(LoadS3, self).__init__(*args, **kwargs)
        self.filename = filename
        self.s3_credentials_id = s3_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        # aws_hook = AwsHook(self.aws_credentials_id)
        # credentials = aws_hook.get_credentials()

        print(os.listdir())

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Uploading file {self.filename} to {s3_path}")
        # session = boto3.Session(
        #     aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
        #     aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
        # )

        # s3 = session.resource('s3')
        # s3.Bucket(bucket).upload_file(source, key)

        s3 = S3Hook(self.s3_credentials_id)
        s3.load_file(self.filename, self.s3_key, self.s3_bucket)




