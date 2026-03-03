from airflow.sdk import DAG
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def build_user_activity_mart():
    pg_hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    DROP MATERIALIZED VIEW IF EXISTS mart_user_activity CASCADE;

    CREATE MATERIALIZED VIEW mart_user_activity AS
    SELECT 
        DATE(us.start_time) AS activity_date,
        COUNT(DISTINCT us.session_id) AS total_sessions,
        COUNT(DISTINCT us.user_id) AS unique_users,
        EXTRACT(MINUTES FROM AVG(us.end_time - us.start_time)) AS avg_session_duration_min,
        COUNT(DISTINCT usp.id) AS total_page_views,
        COUNT(DISTINCT usa.id) AS total_actions
    FROM user_session us
    LEFT JOIN user_session_page usp ON us.session_id = usp.session_id
    LEFT JOIN user_session_action usa ON us.session_id = usa.session_id
    GROUP BY DATE(us.start_time);
    """
    pg_hook.run(sql)


def build_support_efficiency_mart():

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    DROP MATERIALIZED VIEW IF EXISTS mart_support_efficiency CASCADE;

    CREATE MATERIALIZED VIEW mart_support_efficiency AS
    SELECT 
        DATE(created_at) AS report_date,
        issue_type,
        status,
        COUNT(ticket_id) AS total_tickets,
        EXTRACT(MINUTES FROM AVG(updated_at - created_at)) AS avg_resolution_time_min
    FROM support_ticket
    GROUP BY DATE(created_at), issue_type, status;
    """
    pg_hook.run(sql)


with DAG('build_marts', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:

    build_user_activity_mart_operator = PythonOperator(
        task_id="build_user_activity_mart",
        python_callable=build_user_activity_mart
    )

    build_support_efficiency_mart_operator = PythonOperator(
        task_id="build_support_efficiency_mart",
        python_callable=build_support_efficiency_mart
    )

