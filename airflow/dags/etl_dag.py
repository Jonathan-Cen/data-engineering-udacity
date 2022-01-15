from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,  # the DAG does not have dependencies on past runs.
    'retries': 3,  # on failure, the task are retried 3 times.
    'retry_delay': timedelta(minutes=5),  # Retries happen every 5 minutes.
    'email_on_retry': False,  # do not email on retry.
    'catchup': False  # no catchup
}


dag = DAG(
    dag_id='udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    is_events=True,
    drop_table_query=SqlQueries.staging_events_table_drop,
    create_table_query=SqlQueries.staging_events_table_create,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    is_events=False,
    drop_table_query=SqlQueries.staging_songs_table_drop,
    create_table_query=SqlQueries.staging_songs_table_create,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songplays",
    drop_table_query=SqlQueries.songplay_table_drop,
    create_table_query=SqlQueries.songplay_table_create,
    insert_table_query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="users",
    drop_table_query=SqlQueries.user_table_drop,
    create_table_query=SqlQueries.user_table_create,
    insert_table_query=SqlQueries.user_table_insert,
    delete_load=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songs",
    drop_table_query=SqlQueries.song_table_drop,
    create_table_query=SqlQueries.song_table_create,
    insert_table_query=SqlQueries.song_table_insert,
    delete_load=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="artists",
    drop_table_query=SqlQueries.artist_table_drop,
    create_table_query=SqlQueries.artist_table_create,
    insert_table_query=SqlQueries.artist_table_insert,
    delete_load=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="time",
    drop_table_query=SqlQueries.time_table_drop,
    create_table_query=SqlQueries.time_table_create,
    insert_table_query=SqlQueries.time_table_insert,
    delete_load=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_names=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_song_dimension_table] >> run_quality_checks >> end_operator
