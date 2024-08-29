from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import logging

from datetime import datetime, timedelta
import pandas as pd
import os
import uuid

import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'connection_test',
    default_args=default_args,
    description='Postgres Test',
    schedule_interval='@once',
    catchup=False
)


# Define dag tasks

run_this = PostgresOperator(
    task_id='run_sql',
    postgres_conn_id='postgres',
    sql='SELECT * FROM tabela_exemplo;',
    dag=dag,
)
# Configure the task dependencies 

run_this




