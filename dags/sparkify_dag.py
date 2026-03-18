from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator

from plugins.helpers.sql_queries import SqlQueries



default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="sparkify_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule='@hourly',
    catchup=False,
) as dag:

    start_operator = EmptyOperator(task_id="Begin_execution")

    stage_events = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="brand-sparkify-airflow-2026",
        s3_key="log-data",
        json_path="s3://brand-sparkify-airflow-2026/log_json_path.json",
    )

    stage_songs = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="brand-sparkify-airflow-2026",
        s3_key="song-data",
        json_path="auto",
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql_statement=SqlQueries.songplay_table_insert,
    )

    load_user_dim_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql_statement=SqlQueries.user_table_insert,
        append_only=False,
    )

    load_song_dim_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql_statement=SqlQueries.song_table_insert,
        append_only=False,
    )

    load_artist_dim_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql_statement=SqlQueries.artist_table_insert,
        append_only=False,
    )

    load_time_dim_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql_statement=SqlQueries.time_table_insert,
        append_only=False,
    )

data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    tests=[
        {"check_sql": "SELECT COUNT(*) FROM songplays", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM users", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM songs", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM artists", "expected_result": 0},
        {"check_sql": "SELECT COUNT(*) FROM time", "expected_result": 0}
    ]
)

    end_operator = EmptyOperator(task_id="Stop_execution")

    start_operator >> [stage_events, stage_songs]
    [stage_events, stage_songs] >> load_songplays_fact_table
    load_songplays_fact_table >> [
        load_user_dim_table,
        load_song_dim_table,
        load_artist_dim_table,
        load_time_dim_table,
    ]
    [
        load_user_dim_table,
        load_song_dim_table,
        load_artist_dim_table,
        load_time_dim_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator
