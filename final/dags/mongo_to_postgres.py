from airflow.sdk import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.standard.operators.python import PythonOperator

def load_user_sessions():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    raw_data = list(mongo_hook.get_conn()['etl_db']['UserSessions'].find({}))
    if not raw_data:
        return

    user_session_rows = []
    user_session_page_rows = []
    user_session_action_rows = []
    for row in raw_data:
        session_id = row['session_id']
        user_id = row['user_id']
        start_time = row['start_time']
        end_time = row['end_time']
        device = row['device']
        for i, page in enumerate(row['pages_visited']):
            user_session_page_rows.append((session_id, page, i))
        for i, action in enumerate(row['actions']):
            user_session_action_rows.append((session_id, action, i))
        user_session_rows.append((session_id, user_id, start_time, end_time, device))

    pg_hook = PostgresHook(postgres_conn_id="postgres")
    # pg_hook.insert_rows(table="user_session", rows=user_session_rows)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO user_session (
                session_id,
                user_id,
                start_time,
                end_time,
                device
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
            """, user_session_rows)
    # pg_hook.insert_rows(table="user_session_page", rows=user_session_page_rows)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO user_session_page (
                session_id,
                page,
                visit_order
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (session_id, visit_order) DO NOTHING
            """, user_session_page_rows)
    # pg_hook.insert_rows(table="user_session_action", rows=user_session_action_rows)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO user_session_action (
                session_id,
                action,
                action_order
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (session_id, action_order) DO NOTHING
            """, user_session_action_rows)

def load_event_logs():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    raw_data = list(mongo_hook.get_conn()['etl_db']['EventLogs'].find({}))
    if not raw_data:
        return

    df = pd.DataFrame(raw_data)
    df_clean = df[["event_id", "timestamp", "event_type", "details"]]

    pg_hook = PostgresHook(postgres_conn_id="postgres")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO event_log (
                event_id,
                timestamp,
                event_type,
                details
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """, list(df_clean.itertuples(index=False, name=None)))
    # pg_hook.insert_rows(table="event_log", rows=list(df_clean.itertuples(index=False, name=None)))

def load_support_tickets():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    raw_data = list(mongo_hook.get_conn()['etl_db']['SupportTickets'].find({}))
    if not raw_data:
        return

    support_ticket_rows = []
    support_ticket_message_rows = []
    for row in raw_data:
        ticket_id = row['ticket_id']
        user_id = row['user_id']
        status = row['status']
        issue_type = row['issue_type']
        created_at = row['created_at']
        updated_at = row['updated_at']
        for i, message in enumerate(row['messages']):
            support_ticket_message_rows.append((ticket_id, message['sender'], message['message'], message['timestamp']))
        support_ticket_rows.append((ticket_id, user_id, status, issue_type, created_at, updated_at))

    pg_hook = PostgresHook(postgres_conn_id="postgres")
    # pg_hook.insert_rows(table="support_ticket", rows=support_ticket_rows)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO support_ticket (
                ticket_id,
                user_id,
                status,
                issue_type,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE
            SET updated_at = GREATEST(support_ticket.updated_at, EXCLUDED.updated_at), status = EXCLUDED.status
            """, support_ticket_rows)
    # pg_hook.insert_rows(table="support_ticket_message", rows=support_ticket_message_rows)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO support_ticket_message (
                ticket_id,
                sender,
                message,
                timestamp
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticket_id, sender, message, timestamp) DO NOTHING
            """, support_ticket_message_rows)


def load_user_recommendations():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    raw_data = list(mongo_hook.get_conn()['etl_db']['UserRecommendations'].find({}))
    if not raw_data:
        return

    df = pd.DataFrame(raw_data)
    df_clean = df[["user_id", "recommended_products", "last_updated"]].explode('recommended_products')

    user_ids = df_clean["user_id"].unique().tolist()

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    pg_hook.run("""
    DELETE FROM user_recommendation
    WHERE user_id = ANY(%s)
    """, parameters = (user_ids,))

    pg_hook.insert_rows(table="user_recommendation", rows=list(df_clean.itertuples(index=False, name=None)), target_fields = ["user_id", "recommended_product", "last_updated"])

def load_moderation_queue():
    mongo_hook = MongoHook(mongo_conn_id='mongo')
    raw_data = list(mongo_hook.get_conn()['etl_db']['ModerationQueue'].find({}))
    if not raw_data:
        return

    df = pd.DataFrame(raw_data)
    df_clean = df[["review_id", "user_id", "product_id", "review_text", "rating", "moderation_status", "flags", "submitted_at"]]

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO moderation_queue (
                review_id,
                user_id,
                product_id,
                review_text,
                rating,
                moderation_status,
                flags,
                submitted_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING
            """, list(df_clean.itertuples(index=False, name=None)))

    # pg_hook.insert_rows(table="moderation_queue", rows=list(df_clean.itertuples(index=False, name=None)))


with DAG('mongo_to_pg', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    load_user_sessions_operator = PythonOperator(
        task_id="load_user_sessions",
        python_callable=load_user_sessions
    )
    load_event_logs_operator = PythonOperator(
        task_id="load_event_logs",
        python_callable=load_event_logs
    )
    load_support_tickets_operator = PythonOperator(
        task_id="load_support_tickets",
        python_callable=load_support_tickets
    )
    load_user_recommendations_operator = PythonOperator(
        task_id="load_user_recommendations",
        python_callable=load_user_recommendations
    )
    load_moderation_queue_operator = PythonOperator(
        task_id="load_moderation_queue",
        python_callable=load_moderation_queue
    )


