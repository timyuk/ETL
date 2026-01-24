from airflow.sdk import DAG
from datetime import datetime
import requests

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def json_to_table():
    url = "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
    response = requests.get(url)
    data = response.json()

    pets = data['pets']

    pet_rows = []
    food_rows = []
    for i, pet in enumerate(pets):
        pet_id = i
        name = pet.get("name")
        species = pet.get("species")
        fav_foods = pet.get("favFoods", [])
        birth_year = pet.get("birthYear")
        photo_url = pet.get("photo")
        pet_rows.append((pet_id, name, species, birth_year, photo_url))
        for food in fav_foods:
            food_rows.append((pet_id, food))

    pg_hook = PostgresHook(postgres_conn_id="postgres")

    create_pets_table_sql = """
    CREATE TABLE IF NOT EXISTS pets (
        pet_id INT PRIMARY KEY,
        name TEXT,
        species TEXT,
        birth_year INT,
        photo_url TEXT
    );
    """

    create_foods_table_sql = """
        CREATE TABLE IF NOT EXISTS fav_foods (
            pet_id INT,
            food TEXT,
            PRIMARY KEY (pet_id, food),
            FOREIGN KEY (pet_id) REFERENCES pets(pet_id) ON DELETE CASCADE
        );
        """
    truncate_tables_sql = """
        TRUNCATE TABLE pets, fav_foods;
    """
    pg_hook.run(truncate_tables_sql)

    pg_hook.run(create_pets_table_sql)
    pg_hook.run(create_foods_table_sql)

    pg_hook.insert_rows(table="pets", rows=pet_rows)
    pg_hook.insert_rows(table="fav_foods", rows=food_rows)



with DAG('extract_json', schedule=None, start_date=datetime(2026, 1, 1), catchup=False) as dag:
    history = PythonOperator(
        task_id="json_to_table",
        python_callable=json_to_table
    )


