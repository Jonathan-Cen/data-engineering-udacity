"""
Course: Udacity Data Engineering
Project: Data Pipeline with Apache Airflow
Student: Jonathan Cen

The LoadDimensionOperator utilises the provided SQL helper class to run data transformation. Most of the logic is within
the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run
the query against. A target table which contains the results of the transformation could also be created.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus a
parameter could be created to distinguish between insert mode and append mode.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 target_table: str = "",
                 drop_table_query: str = "",
                 create_table_query: str = "",
                 insert_table_query: str = "",
                 delete_load: bool = True,
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.drop_table_query = drop_table_query
        self.create_table_query = create_table_query
        self.insert_table_query = insert_table_query
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('Executing LoadDimensionOperator')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_load:
            self.log.info(f"Using delete-load mode")
            self.log.info(f"Dropping table {self.target_table} if exist")
            redshift.run(self.drop_table_query)
        else:
            self.log.info("Using append-only mode")

        self.log.info(f"Creating table {self.target_table}.")
        redshift.run(self.create_table_query)

        self.log.info(f"Inserting data into table {self.target_table}")
        redshift.run(self.insert_table_query)

        self.log.info(f"Finished populating table {self.target_table}")