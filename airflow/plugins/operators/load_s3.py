from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3

class LoadS3(BaseOperator):
    
    ui_color = '#03f4fc'

    template_fields = ("filename","s3_key")

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
        rendered_filename = self.filename.format(**context)
        rendered_s3_key = self.s3_key.format(**context)

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_key)
        self.log.info(f"Uploading file {rendered_filename} to {s3_path}")

        s3 = S3Hook(self.s3_credentials_id)
        s3.load_file(rendered_filename, rendered_s3_key, self.s3_bucket, replace=True)




