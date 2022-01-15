"""
Course: Udacity Data Engineering
Project: Data Pipeline with Apache Airflow
Student: Jonathan Cen

This StageToRedshiftOperator loads any JSON formatted files from AWS S3 to Amazon Redshift data warehouse.json
The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters
should specify where in S3 the file is loaded and what is the target table

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is
containing a templated field that allows it to load timestamped files from s3 based on execution time and run backfill.

log_data: https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&prefix=log_data/&showversions=false
log_data will be loaded into stage_events table.

song_data: https://s3.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&prefix=song-data/&showversions=false
song_data will be loaded into stage_songs table.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    log_json_path = 's3://udacity-dend/log_json_path.json'
    copy_sql = """
            COPY {target_table}
            FROM '{data_source}'
            ACCESS_KEY_ID '{access_key_id}'
            SECRET_ACCESS_KEY '{secret_access_key}' 
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 target_table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 is_events: bool = False,
                 drop_table_query: str = None,
                 create_table_query: str = None,
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.is_events = is_events
        self.drop_table_query = drop_table_query
        self.create_table_query = create_table_query

    def execute(self, context):
        self.log.info('Executing StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Dropping table {self.target_table} if exists")
        redshift.run(self.drop_table_query)

        self.log.info(f"Creating table {self.target_table}")
        redshift.run(self.create_table_query)

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info("Copying data from s3 to Redshift.")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            target_table=self.target_table,
            data_source=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key
        )

        if self.is_events:
            formatted_sql += f"format as json '{StageToRedshiftOperator.log_json_path}'"
        else:
            formatted_sql += "json 'auto'"

        redshift.run(formatted_sql)
        self.log.info(f"Finished loading {self.target_table}")


