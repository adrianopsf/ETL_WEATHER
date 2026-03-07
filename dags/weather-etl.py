import os
import uuid
import logging

import pandas as pd
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from scripts.weather_data import (
    get_cities_to_weather,
    create_current_weather_file,
    create_historical_weather_file,
    create_forecast_weather_file,
)
from scripts.queries import (
    create_curated_current,
    update_curated_current,
    create_curated_timeline,
    update_curated_timeline,
)

# ---------------------------------------------------------------------------
# Database configuration — read from environment variables
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))


def get_db_conn():
    """Return a psycopg2 connection using environment-variable credentials."""
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )


# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_data",
    default_args=default_args,
    description="Pipeline ETL de dados meteorologicos para as 26 capitais brasileiras",
    schedule_interval="@once",
    catchup=False,
)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------
def print_hello():
    logging.info("Hello World — iniciando pipeline ETL de clima")


def bye_bye():
    logging.info("Bye Bye — pipeline ETL de clima concluido")


def download_current_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + "/dags/"
    logging.info(f"Downloading current weather data for {cities}")
    create_current_weather_file(cities, save_path=save_path)
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + "current_raw.csv"
    df = pd.read_csv(read_path)
    logging.info(df.head())


def download_forecast_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + "/dags/"
    logging.info(f"Downloading forecast weather data for {cities}")
    create_forecast_weather_file(cities, save_path=save_path)
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + "forecast_raw.csv"
    df = pd.read_csv(read_path)
    logging.info(df.head())


def download_historical_weather_data():
    cities = get_cities_to_weather()
    save_path = os.getcwd() + "/dags/"
    logging.info(f"Downloading historical weather data for {cities}")
    create_historical_weather_file(cities, save_path=save_path)
    logging.info(save_path)
    logging.info(os.listdir(save_path))
    read_path = save_path + "history_raw.csv"
    df = pd.read_csv(read_path)
    logging.info(df.head())


def current_weather_to_raw():
    current = pd.read_csv("./dags/current_raw.csv")
    current["uuid"] = [uuid.uuid4() for _ in range(len(current))]
    cols = current.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    current = current[cols]
    current["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current.columns = [c.replace("localtime", "local_time") for c in current.columns]
    logging.info(current.head())
    columns = current.columns
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS current_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS current_weather_raw ({' text,'.join(columns)} text)")
        for _, row in current.iterrows():
            cur.execute(
                f"INSERT INTO current_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})",
                row,
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table current_weather_raw created successfully")
    except Exception as e:
        logging.error(f"Error creating current_weather_raw: {e}")
        raise


def forecast_weather_to_raw():
    forecast = pd.read_csv("./dags/forecast_raw.csv")
    forecast["day"] = forecast["day"].str.replace("'", '"')
    forecast["uuid"] = [uuid.uuid4() for _ in range(len(forecast))]
    cols = forecast.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    forecast = forecast[cols]
    forecast["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    forecast.columns = [c.replace("localtime", "local_time") for c in forecast.columns]
    logging.info(forecast.head())
    columns = forecast.columns
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS forecast_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS forecast_weather_raw ({' text,'.join(columns)} text)")
        for _, row in forecast.iterrows():
            cur.execute(
                f"INSERT INTO forecast_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})",
                row,
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table forecast_weather_raw created successfully")
    except Exception as e:
        logging.error(f"Error creating forecast_weather_raw: {e}")
        raise


def history_weather_to_raw():
    history = pd.read_csv("./dags/history_raw.csv")
    history["day"] = history["day"].str.replace("'", '"')
    history["uuid"] = [uuid.uuid4() for _ in range(len(history))]
    cols = history.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    history = history[cols]
    history["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history.columns = [c.replace("localtime", "local_time") for c in history.columns]
    logging.info(history.head())
    columns = history.columns
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS history_weather_raw")
        cur.execute(f"CREATE TABLE IF NOT EXISTS history_weather_raw ({' text,'.join(columns)} text)")
        for _, row in history.iterrows():
            cur.execute(
                f"INSERT INTO history_weather_raw ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})",
                row,
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table history_weather_raw created successfully")
    except Exception as e:
        logging.error(f"Error creating history_weather_raw: {e}")
        raise


def current_to_curated():
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS current_weather")
        cur.execute(create_curated_current)
        cur.execute(update_curated_current)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table current_weather updated successfully")
    except Exception as e:
        logging.error(f"Error updating current_weather: {e}")
        raise


def timeline_to_curated():
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS timeline_weather")
        cur.execute(create_curated_timeline)
        cur.execute(update_curated_timeline)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table timeline_weather updated successfully")
    except Exception as e:
        logging.error(f"Error updating timeline_weather: {e}")
        raise


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------
t_hello = PythonOperator(task_id="hello", python_callable=print_hello, dag=dag)
t_bye = PythonOperator(task_id="bye_bye", python_callable=bye_bye, dag=dag)

t_dl_current = PythonOperator(
    task_id="download_current_weather_data",
    python_callable=download_current_weather_data,
    dag=dag,
)
t_dl_forecast = PythonOperator(
    task_id="download_forecast_weather_data",
    python_callable=download_forecast_weather_data,
    dag=dag,
)
t_dl_history = PythonOperator(
    task_id="download_historical_weather_data",
    python_callable=download_historical_weather_data,
    dag=dag,
)

t_current_raw = PythonOperator(
    task_id="current_weather_to_raw",
    python_callable=current_weather_to_raw,
    dag=dag,
)
t_forecast_raw = PythonOperator(
    task_id="forecast_weather_to_raw",
    python_callable=forecast_weather_to_raw,
    dag=dag,
)
t_history_raw = PythonOperator(
    task_id="history_weather_to_raw",
    python_callable=history_weather_to_raw,
    dag=dag,
)

t_current_curated = PythonOperator(
    task_id="current_to_curated",
    python_callable=current_to_curated,
    dag=dag,
)
t_timeline_curated = PythonOperator(
    task_id="timeline_to_curated",
    python_callable=timeline_to_curated,
    dag=dag,
)

# ---------------------------------------------------------------------------
# Task dependencies
# ---------------------------------------------------------------------------
t_hello >> [t_dl_current, t_dl_forecast, t_dl_history]
t_dl_current >> t_current_raw >> t_current_curated >> t_bye
t_dl_forecast >> t_forecast_raw >> t_timeline_curated >> t_bye
t_dl_history >> t_history_raw >> t_timeline_curated >> t_bye
