from airflow.sdk import DAG
from datetime import datetime
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def csv_to_table():
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df[df['out/in'] == 'In']
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date
    lower = df['temp'].quantile(0.05)
    upper = df['temp'].quantile(0.95)
    df = df[(df['temp'] > lower) & (df['temp'] < upper)]
    df = df.groupby('noted_date')['temp'].mean().reset_index()
    df['temp'] = df['temp'].round(1)
    df = df.sort_values('temp')
    cold_days = df.head(5)
    hot_days = df.tail(5)
    cold_days['type'] = 'cold'
    hot_days['type'] = 'hot'
    extreme_days = pd.concat([cold_days, hot_days])
    rows = list(extreme_days.itertuples(index=False, name=None))

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS extreme_days (
        noted_date DATE PRIMARY KEY,
        temp INT,
        type TEXT
    );
    """

    truncate_table_sql = """
        TRUNCATE TABLE extreme_days;
    """

    pg_hook.run(create_table_sql)
    pg_hook.run(truncate_table_sql)

    pg_hook.insert_rows(table="extreme_days", rows=rows)



with DAG('extract_csv', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    history = PythonOperator(
        task_id="csv_to_table",
        python_callable=csv_to_table
    )


