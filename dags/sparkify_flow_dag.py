from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook


AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'Ahmed_Amin',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_flow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
)

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "s3://udacity-dend",
    s3_key = "log_data/",
    json_path = "s3://udacity-dend/log_json_path.json",
    region = "us-west-2",
    table = "staging_events"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = "s3://udacity-dend",
    s3_key = "song_data/",
    json_path= "auto",
    region = "us-west-2",
    table = "staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table= "songplays",
    load_sql_stmt= SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table= "users",
    load_sql_stmt= SqlQueries.user_table_insert,
    append_only = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table= "songs",
    load_sql_stmt= SqlQueries.song_table_insert,
    append_only = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table= "artists",
    load_sql_stmt= SqlQueries.artist_table_insert,
    append_only = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table= "time",
    load_sql_stmt= SqlQueries.time_table_insert,
    append_only = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    dq_checks = [
            {'test_sql': "SELECT COUNT(*) FROM public.songplays WHERE sessionid IS NULL", 'expected_result': 0},
            {'test_sql': "SELECT COUNT(*) FROM public.songs WHERE song_id IS NULL" , 'expected_result': 0},
            {'test_sql': "SELECT COUNT(*) FROM public.artists WHERE artist_name IS NULL", 'expected_result': 0},
            {'test_sql': "SELECT COUNT(*) FROM public.users WHERE last_name IS NULL", 'expected_result':0 },
            {'test_sql': "SELECT COUNT(*) FROM public.time WHERE month IS NULL", 'expected_result': 0 }
        ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks 
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator