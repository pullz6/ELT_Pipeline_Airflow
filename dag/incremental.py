from datetime import datetime 
from datetime import timedelta
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import subprocess
import re
import os
import glob

default_args = {
    'owner': 'Sara',
    'retry': 2,
    'retry_delay': timedelta(minutes=5)
}

def install_dependencies():
    subprocess.run(['pip','install','numpy'])
    subprocess.run(['pip','install','psycopg2'])
    subprocess.run(['pip','install','pandas'])

    
def check_last_order(ti):
    import os
    import pandas as pd
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True

    cursor = conn.cursor()
    query = "SELECT max(time_added) from orders_india;"
    cursor.execute(query)
    results = cursor.fetchall()
    results = results[0]
    last_date = results[0]
    print(last_date)
    print("Last Date taken")
    ti.xcom_push(key='last_date', value=last_date)

def getting_lastest_data(ti):
    import pandas as pd
    
    #Getting the latest date
    last_date = ti.xcom_pull(task_ids='Checking_the-last_order_date', key='last_date')
    print(last_date)
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True 
    cursor = conn.cursor()
    
    #Importing the CSV
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'india-pipeline-2.xlsx')
    xlsx = pd.ExcelFile(csv_path)
    frames = []
    for sheet_name in xlsx.sheet_names: 
        temp = pd.read_excel(xlsx, sheet_name)
        frames.append(temp)
    df = pd.concat(frames)
    df['Order Received Date'] = pd.to_datetime(df['Order Received Date']).dt.date
    df = df[df['Order Received Date']>last_date]
    print(df['Order Received Date'])
    
    #Getting today's date to put to the postgres
    today = datetime.today()
    today = today.strftime("%Y-%m-%d")
    print(today)
    
    #Creating a dataframe with the required dataframe columns
    df = df[['Order Received Date', 'Style No','Brand Name','Product Name','Order Qty','Order Value (USD)','Job Status','Production Status','Delivery Date']]
    df['Order Qty'].fillna(0,inplace=True)
    df['Order Value (USD)'].fillna(0,inplace=True)
    df['Order Received Date'].fillna('None_yet',inplace=True)
    df['Style No'].fillna('None_yet',inplace=True)
    df['Brand Name'].fillna('None_yet',inplace=True)
    df['Product Name'].fillna('None_yet',inplace=True)
    df['Job Status'].fillna('None_yet',inplace=True)
    df['Production Status'].fillna('None_yet',inplace=True)
    df['Delivery Date'].fillna('None_yet',inplace=True)
    
    df = df.astype({'Order Received Date': str, 'Style No': str, 'Brand Name': str, 'Product Name': str, 'Order Qty': int,'Order Value (USD)': str, 'Job Status': str,'Production Status': str, 'Delivery Date': str})
    
    for index, row in df.iterrows():
        query = "INSERT INTO orders_india(order_rec_date, style_no, brand, product_code, order_qty, order_value, job_status, production_status, delivery_date,time_added) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}','{9}')".format(row['Order Received Date'], row['Style No'], row['Brand Name'],row['Product Name'],row['Order Qty'],row['Order Value (USD)'],row['Job Status'],row['Production Status'],row['Delivery Date'],today)
        cursor.execute(query)
        print("single_inserts() done")

with DAG(
    default_args=default_args,
    dag_id="Incremental_Load_V3",
    start_date=datetime(2024, 10, 7),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='Installing_Dependencies',
        python_callable=install_dependencies
    )
    task2 = PythonOperator(
        task_id='Checking_the-last_order_date',
        python_callable=check_last_order
    )
    
    task3 = PythonOperator(
        task_id='getting_dataframe',
        python_callable=getting_lastest_data
    )

    task1 >> task2 >> task3