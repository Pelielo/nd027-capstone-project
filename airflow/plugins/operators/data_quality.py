from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Custom Airflow Operator to run data quality checks. Runs the query provided
    and compares its result with the expected result. If there isn't a match,
    raises an `Exception`.

    :param redshift_conn_id: Connection ID used to connect to Redshift database. 
        Must be configured as a Postgres Connection via the Airflow webserver UI.
    :type redshift_conn_id: str

    :param queries_and_results: List of maps where each item holds a query string
        to be executed and the expected result.
    :type queries_and_results: list(map('query': str, 'result': int))
    """

    ui_color = "#0384fc"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        queries_and_results=[{"query": "", "result": 0}],
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.queries_and_results = queries_and_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for quality_check in self.queries_and_results:
            self.log.info("Running data validation query")
            result = redshift.get_first(quality_check["query"])
            self.log.info(f"result: {result}")
            if result[0] != quality_check["result"]:
                raise ValueError
