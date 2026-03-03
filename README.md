## HW_1

**Студент:** Юхнов Тимофей Максимович  
**Тема:** Источники данных


## HW_2

**Студент:** Юхнов Тимофей Максимович  
**Тема:** Основы трансформации данных


## HW_3

**Студент:** Юхнов Тимофей Максимович  
**Тема:** Загрузка данных в целевую систему

## FINAL

**Студент:** Юхнов Тимофей Максимович  
**Тема:** Итоговое задание по модулю 3

### Шаг 1: Создать окружение
#### Создать виртуальное окружение:
```bash
python -m venv venv
```

#### Активировать его:
```bash
.\venv\Scripts\activate
```
или
```bash
source ./venv/Scripts/activate
```

#### Установить pymongo:
```bash
pip install pymongo
```

### Шаг 2: Инитиализировать базу данных mongo и сгенерировать данные
#### Запустить файл:
```bash
python .\generate_data.py  
```
Скрипт каждый раз генерирует новые рандомные данные

### Шаг 3: Инитиализировать базу данных postgres
#### Скопировать файл создающий струтктуру базы данных postgres в контейнер:
```bash
docker cp generate_pg.sql final-postgres-1:/generate_pg.sql   
```

#### Запустить файл:
```bash
docker exec -it final-postgres-1 psql -U airflow -d airflow -f generate_pg.sql
```

### Шаг 4: Выполнить Airflow пайплайны
#### Настроить postgres и mongodb соединения в Airflow

#### Запустить DAGи mongo_to_pg и build_marts
<img width="960" height="456" alt="image" src="https://i.gyazo.com/e4244b4d1dfa51eeee46feb35ca63716.png" />
<img width="960" height="456" alt="image" src="https://i.gyazo.com/4ec40c1b84325c1b92e19e50b58e7118.png" />

### Шаг 5: Проверить работу пайплайнов
#### Подключиться к контейнеру:
```bash
docker exec -it final-postgres-1 psql -U airflow -d airflow
```
#### Проверить построенные витрины:
```SQL
SELECT * FROM mart_support_efficiency;

SELECT * FROM mart_user_activity;
```

<img width="960" height="456" alt="image" src="https://i.gyazo.com/1d8f2ed51cb3da01a0232a56f73b68bf.png" />

