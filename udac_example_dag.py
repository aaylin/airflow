from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)


from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 9, 1),
    'end_date': datetime(2018, 9, 30),
    'email_on_retry': False,
    'Catchup': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id='aws_credentials',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
)

""" DAGs for load Tables """
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    id_conn="redshift",
    query_show = SqlQueries.songplay_table_insert,
    insert_table = 'public.songplays',
    fact_insert_col = 'start_time, userid, level, songid, artistid, sessionid, location, user_agent',
    bool_table = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    id_conn="redshift",
    insert_table='public.users',
    query_show=SqlQueries.user_table_insert, 
    dim_insert_col = 'userid, first_name, last_name, gender, level',
    bool_table = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    id_conn="redshift",
    insert_table='public.songs',
    query_show=SqlQueries.song_table_insert,
    dim_insert_col = 'songid, title, artistid, year, duration',
    bool_table = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    id_conn="redshift",
    insert_table='public.artists',
    query_show=SqlQueries.artist_table_insert,
    dim_insert_col='artistid, name, location, lattitude, longitude',
    bool_table = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    id_conn="redshift",
    insert_table='public.time',
    query_show=SqlQueries.time_table_insert,
    dim_insert_col='start_time, hour, day, week, month, year, weekday',
    bool_table = True
)

""" DAG for quality checks """
run_quality_checks = DataQualityOperator(
       task_id='Run_data_quality_checks',
    dag=dag,
    id_conn="redshift",
    quality_queries=["SELECT COUNT(*) FROM songs WHERE songid IS NULL", \
                       "SELECT COUNT(*) FROM songs", \
                       "SELECT COUNT(*) FROM songplays", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM artists", \
                       "SELECT COUNT(*) FROM time" \
                      ],
    expected_results=[lambda num_records: num_records==0, \
                      lambda num_records: num_records>0, \
                      lambda num_records: num_records>0, \
                      lambda num_records: num_records>0, \
                      lambda num_records: num_records>0, \
                      lambda num_records: num_records>0],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

""" DAG Workflow """

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator