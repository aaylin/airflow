""" code for data quality checks"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    quality check operator for data quality checks of the Redshift data
    :param redshift_conn_id: Connection id for the Connection to Redshift
    :param quality_queries: Data quality check queries
    :param expected_results: expressions to validate the data quality query results
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 id_conn="",
                 quality_queries="",
                 expected_results="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.id_conn = id_conn
        self.quality_queries = quality_queries
        self.expected_results = expected_results

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for i, query in enumerate(self.quality_queries):
            self.log.info(f"Running quality check {i}: {query}")
            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Quality check failed. No results returned by {query}.")
            num_records = records[0][0]
            if not self.expected[i](num_records) < 1:
                raise ValueError(f"Quality check failed. Expected value does not fit to returned       value of {num_records}")
        logging.info(f"Data quality on {query} passed with {records[0][0]} records")
