"""
Course: Udacity Data Engineering
Project: Data Pipeline with Apache Airflow
Student: Jonathan Cen

The LoadFactOperator utilises the provided SQL helper class to run data transformation. Most of the logic is within
the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run
the query against. A target table which contains the results of the transformation could also be created.

Fact tables are usually so massive that they only allow append type functionality.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 target_table: str = "",
                 drop_table_query: str = "",
                 create_table_query: str = "",
                 insert_table_query: str = "",
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.drop_table_query = drop_table_query
        self.create_table_query = create_table_query
        self.insert_table_query = insert_table_query

    def execute(self, context):
        self.log.info('Executing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Dropping table {self.target_table} if exist")
        redshift.run(self.drop_table_query)
        
        self.log.info(f"Creating table {self.target_table}.")
        redshift.run(self.create_table_query)

        self.log.info(f"Inserting data into table {self.target_table}")
        redshift.run(self.insert_table_query)

        self.log.info(f"Finished populating table {self.target_table}")