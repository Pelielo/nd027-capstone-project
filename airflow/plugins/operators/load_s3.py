from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadS3(BaseOperator):
    """
    Custom Airflow Operator to load a file from the local filesystem to a S3 Bucket.

    :param filename: name of the source file. Can be formatted used Airflow `context`.
    :type filename: str

    :param s3_credentials_id: Connection to S3 containing Access Key and Secret Key.
        Must be configured as a S3 Connection via the Airflow webserver UI.
    :type s3_credentials_id: str

    :param s3_bucket: Name of the source S3 bucket.
    :type s3_bucket: str

    :param s3_key: Remaining path to the source file in S3.
    :type s3_key: str
    """

    ui_color = "#03f4fc"

    template_fields = ("filename", "s3_key")

    @apply_defaults
    def __init__(
        self,
        filename="",
        s3_credentials_id="",
        s3_bucket="",
        s3_key="",
        *args,
        **kwargs,
    ):

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
