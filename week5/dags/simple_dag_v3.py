# Use Airflow Xcom, Variable, Connection

from datetime import datetime
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2


def get_redshift_connection():
    hook = PostgresHook()
    hook.postgres_conn_id = 'redshift_dev_db'
    return hook.get_conn().cursor()


def extract(**context):
    url = context['params']['url']
    f = requests.get(url)
    return f.text


def transform(**context):
    text = context['task_instance'].xcom_pull(key='return_value', task_ids='extract')
    lines = text.split("\n")[1:]
    return lines


def load(**context):
    table = context['params']['table']

    # cur = test()
    cur = get_redshift_connection()
    data = context['task_instance'].xcom_pull(key='return_value', task_ids='transform')

    try:
        cur.execute('BEGIN;')
        cur.execute(f'DELETE FROM {table};')
        for r in data:
            if r != '':
                name, gender = r.split(",")
                sql = f"INSERT INTO {table} VALUES ('{name}', '{gender}');"
                print(sql)
                cur.execute(sql)
        cur.execute('END;')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")


# Make DAG
dag = DAG(
    dag_id='simple_dag_v3',
    start_date=datetime(2020, 12, 7),
    schedule_interval=None,
    catchup=False,
    tags=['donghee']
)

# Make task
extract = PythonOperator(
    dag=dag,
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    }
)

transform = PythonOperator(
    dag=dag,
    task_id='transform',
    python_callable=transform,
)

load = PythonOperator(
    dag=dag,
    task_id='load',
    python_callable=load,
    params={
        'table': 'name_gender'
    }
)

extract >> transform >> load
