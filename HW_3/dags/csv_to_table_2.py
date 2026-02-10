from airflow.sdk import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def prepare_csv(incremental = False):
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
    df = df[df['out/in'] == 'In']
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M').dt.date
    lower = df['temp'].quantile(0.05)
    upper = df['temp'].quantile(0.95)
    df = df[(df['temp'] > lower) & (df['temp'] < upper)]

    if incremental:
        cutoff_date = (datetime.now() - timedelta(days=3)).date()
        df = df[df['noted_date'] >= cutoff_date]

    df_grouped = df.groupby('noted_date')['temp'].mean().reset_index()
    df_grouped['temp'] = df_grouped['temp'].round(1)
    return df_grouped

def calc_extreme_days(df):
    df = df.sort_values('temp')
    cold_days = df.head(5)
    hot_days = df.tail(5)
    cold_days['type'] = 'cold'
    hot_days['type'] = 'hot'
    extreme_days = pd.concat([cold_days, hot_days])
    return extreme_days

def load_full():
    df = prepare_csv()
    rows = list(df.itertuples(index=False, name=None))

    pg_hook = PostgresHook(postgres_conn_id="postgres")


    pg_hook.run("""
    CREATE TABLE IF NOT EXISTS daily_temps (
        noted_date DATE PRIMARY KEY,
        temp INT
    );
    """)

    pg_hook.run("""
    TRUNCATE TABLE daily_temps;
    """)

    pg_hook.insert_rows(table="daily_temps", rows=rows)

def load_incremental():
    df = prepare_csv(incremental=True)
    rows = list(df.itertuples(index=False, name=None))
    pg_hook = PostgresHook(postgres_conn_id="postgres")

    pg_hook.run("""
    CREATE TABLE IF NOT EXISTS daily_temps (
        noted_date DATE PRIMARY KEY,
        temp INT
    );
    """)
    dates_to_update = tuple(str(x[0]) for x in rows)
    for date in dates_to_update:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                DELETE FROM daily_temps 
                WHERE noted_date = %s"
                """, (date,))

    pg_hook.insert_rows(table="daily_temps", rows=rows)

def create_view():
    pg_hook = PostgresHook(postgres_conn_id="postgres")

    pg_hook.run("""
       CREATE OR REPLACE VIEW extreme_days_view AS
        (
            SELECT noted_date, temp, 'cold' as type 
            FROM daily_temps 
            ORDER BY temp ASC 
            LIMIT 5
        )
        UNION ALL
        (
            SELECT noted_date, temp, 'hot' as type 
            FROM daily_temps 
            ORDER BY temp DESC 
            LIMIT 5
        );
        """)


with DAG('extract_csv_2', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    load_full_operator = PythonOperator(
        task_id="load_full",
        python_callable=load_full
    )
    load_incremental_operator = PythonOperator(
        task_id="load_incremental",
        python_callable=load_incremental
    )
    create_view_operator = PythonOperator(
        task_id="create_view",
        python_callable=create_view
    )
    load_full_operator >> load_incremental_operator >> create_view_operator


