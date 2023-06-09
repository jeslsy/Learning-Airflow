from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import pandas as pd
import logging

import requests

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()



@task
def get_world_infos():
    # 세계나라 정보 받아와서
    url = 'https://restcountries.com/v3.1/all'

    try:
        response = requests.get(url)
        data_json = response.json()

    except requests.exceptions.RequestException as e:
        print('Error:', e)
    # 3개 정보 추출 
    
    records = []

    for data in data_json:
        country = data["name"]["official"]
        population = data["population"]
        area = data["area"]
        records.append([country, population, area])

    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    country text,
    population integer,
    area numeric
);""")
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ({r[0]}, {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")


with DAG(
    dag_id = 'WorldInfo',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '30 6 * * 6'
) as dag:

    results = get_world_infos()
    load("jeslsy0507", "world_info", results)