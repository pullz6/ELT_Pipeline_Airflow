#Importing the libraries
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

#Setting the default arguments
default_args = {
    'owner': 'Sara',
    'retry': 2,
    'retry_delay': timedelta(minutes=5)
}

def install_dependencies():
    #This function installs a few dependencies that was not included in the requirements.txt when the docker image was built. 
    subprocess.run(['pip','install','numpy'])
    subprocess.run(['pip','install','psycopg2'])
    subprocess.run(['pip','install','pandas'])
    
def check_last_order(ti):
    #This function gets the last entry of the postgres table and pushes it to the xcomms. 
    import os
    import pandas as pd
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='username',password='password',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    cursor = conn.cursor()

    query = "SELECT max(time_added) from table;"
    cursor.execute(query)
    results = cursor.fetchall()

    #Formatting the result
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
    conn = psycopg2.connect(database='orders',user='username',password='password',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True 
    cursor = conn.cursor()
    
    #Importing the CSV
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'yourcsv-2.xlsx')
    xlsx = pd.ExcelFile(csv_path)
    frames = []
    for sheet_name in xlsx.sheet_names: 
        temp = pd.read_excel(xlsx, sheet_name)
        frames.append(temp)
    df = pd.concat(frames)

    #Converting the date field into datetime
    df['date'] = pd.to_datetime(df['date']).dt.date

    #Filtering the dataframe to only have the items that was added after last addition to the postgres table.
    df = df[df['date']>last_date]
    print(df['date'])
    
    #Getting today's date to put to the postgres
    today = datetime.today()
    today = today.strftime("%Y-%m-%d")
    print(today)

    #Creating a dataframe with the required dataframe columns
    df = df[['var1', 'var2','var3','var4','var5','var6','var7','var8','var9']]
    df['var1'].fillna(0,inplace=True)
    df['var2'].fillna(0,inplace=True)
    df['var3'].fillna('None_yet',inplace=True)
    df['var4'].fillna('None_yet',inplace=True)
    df['var5'].fillna('None_yet',inplace=True)
    df['var6'].fillna('None_yet',inplace=True)
    df['var7'].fillna('None_yet',inplace=True)
    df['var8'].fillna('None_yet',inplace=True)
    df['var9'].fillna('None_yet',inplace=True)

    #Converting the dataframe columns into required datatypes
    df = df.astype({'var1': str, 'var2': str, 'var3': str, 'var4': str, 'var5': int,'var6': str, 'var7': str,'var8': str, 'var9': str})
    print(df.info())

    #Inserting the dataframe into the sql, row by row. 
    for index, row in df.iterrows():
        query = "INSERT INTO table(var1, var2, var3, var4, var5, var6, var7, var8, var9,time_added) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}','{9}')".format(row['var1'], row['var2'], row['var3'],row['var4'],row['var5'],row['var6'],row['var7'],row['var8'],row['var9'],today)
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
