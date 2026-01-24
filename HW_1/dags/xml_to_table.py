import requests
from airflow.sdk import DAG
from datetime import datetime, timedelta
import json
import os
import xml.etree.ElementTree as ET

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def xml_to_table():
    url = "https://gist.githubusercontent.com/pamelafox/3000322/raw/nutrition.xml"
    response = requests.get(url)

    root = ET.fromstring(response.content)

    food_rows = []
    for i, food in enumerate(root.findall('food')):
        food_id = i
        name = food.find('name').text
        mfr = food.find('mfr').text

        serving = food.find('serving').text
        serving_units = food.find('serving').get('units')
        calories_total = food.find('calories').get('total')
        calories_fat = food.find('calories').get('fat')

        total_fat = food.find('total-fat').text
        saturated_fat = food.find('saturated-fat').text
        cholesterol = food.find('cholesterol').text
        sodium = food.find('sodium').text
        carb = food.find('carb').text
        fiber = food.find('fiber').text
        protein = food.find('protein').text

        vitamin_a = food.find('vitamins').find('a').text
        vitamin_c = food.find('vitamins').find('c').text

        mineral_ca = food.find('minerals').find('ca').text
        mineral_fe = food.find('minerals').find('fe').text

        food_rows.append((
            food_id, name, mfr, serving, serving_units,
            calories_total, calories_fat, total_fat, saturated_fat, cholesterol, sodium,
            carb, fiber, protein, vitamin_a, vitamin_c, mineral_ca, mineral_fe
        ))

    create_foods_table_sql = """
        CREATE TABLE IF NOT EXISTS foods (
            id INT PRIMARY KEY,
            name TEXT,
            mfr TEXT,
            serving FLOAT,
            serving_units TEXT,
            calories_total INT,
            calories_fat INT,
            total_fat FLOAT,
            saturated_fat FLOAT,
            cholesterol FLOAT,
            sodium FLOAT,
            carbs FLOAT,
            fiber FLOAT,
            protein FLOAT,
            vitamin_a FLOAT,
            vitamin_c FLOAT,
            mineral_ca FLOAT,
            mineral_fe FLOAT
        );
        """
    truncate_foods_table_sql = """
    TRUNCATE TABLE foods;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres")

    pg_hook.run(truncate_foods_table_sql)

    pg_hook.run(create_foods_table_sql)

    pg_hook.insert_rows(
          table="foods",
          rows=food_rows,
          target_fields=[
              'id', 'name', 'mfr', 'serving', 'serving_units',
              'calories_total', 'calories_fat', 'total_fat', 'saturated_fat' ,'cholesterol', 'sodium',
              'carbs', 'fiber', 'protein', 'vitamin_a', 'vitamin_c', 'mineral_ca', 'mineral_fe'
          ]
      )


with DAG('extract_xml', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    history = PythonOperator(
        task_id="xml_to_table",
        python_callable=xml_to_table
    )


