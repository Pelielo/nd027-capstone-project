from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CopyToRedshiftOperator(BaseOperator):
    """
    Custom Airflow Operator to copy a csv file to a Redshift table using COPY SQL command.
    Uses AWS Access Key and Secret Key as authentication method.

    :param redshift_conn_id: Connection ID used to connect to Redshift database. 
        Must be configured as a Postgres Connection via the Airflow webserver UI.
    :type redshift_conn_id: str

    :param aws_credentials_id: Connection to AWS containing Access Key and Secret Key.
        Must be configured as a AWS Connection via the Airflow webserver UI.
    :type aws_credentials_id: str

    :param table: Table name of the destination
    :type table: str

    :param column_list: Comma separated list of the destination column names used
        to map the csv to the table.
    :type column_list: str

    :param s3_bucket: Name of the source S3 bucket.
    :type s3_bucket: str

    :param s3_key: Remaining path to the source file in S3.
    :type s3_key: str
    """

    ui_color = "#03f4fc"

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        ({})
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        IGNOREHEADER 1;
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        column_list="",
        s3_bucket="",
        s3_key="",
        *args,
        **kwargs,
    ):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.column_list = column_list

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)
        self.log.info(
            f"Copying data from {rendered_key} to Redshift table {self.table}"
        )
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = CopyToRedshiftOperator.copy_sql.format(
            self.table,
            self.column_list,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )
        redshift.run(formatted_sql)
