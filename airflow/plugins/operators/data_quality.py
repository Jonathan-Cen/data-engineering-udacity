"""
Course: Udacity Data Engineering
Project: Data Pipeline with Apache Airflow
Student: Jonathan Cen

The DataQualityOperator is used to run checks on the data. The operator's main functionality is to receive one or more SQL based
test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to
be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

For example, one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows
that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL
statement's outcome to the expected result.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 table_names: list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        self.log.info('Executing DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # --- Ensure each table has at least one record --- #
        for table in self.table_names:
            self.log.info(f"Performing data quality check on table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results.")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows.")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records.")