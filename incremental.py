from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import subprocess

default_args = {
    'owner': 'Sara',
    'retry': 2,
    'retry_delay': timedelta(minutes=5)
}

def install_dependencies():
    subprocess.run(['pip','install','numpy'])
    subprocess.run(['pip','install','psycopg2'])

    
def check_last_order():
    import pandas as pd
    import os
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True

    cursor = conn.cursor()
    query = "SELECT MAX(r_id) FROM orders_india;"
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    print("single_inserts() done")
        
    
with DAG(
    default_args=default_args,
    dag_id="Incremental_Load",
    start_date=datetime(2024, 10, 7),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='Installing_Dependencies',
        python_callable=install_dependencies
    )
    task2 = PythonOperator(
        task_id='Checking_he-last_order_date',
        python_callable=check_last_order
    )

    task1 >> task2