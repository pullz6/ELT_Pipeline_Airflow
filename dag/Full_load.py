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
    
def testing_connection():
    conn = psycopg2.connect(database='orders',user='username',password='password',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    if conn is None:
        print("Error connecting to the MySQL database")
    else:
        print("MySQL connection established!")
    conn.close()
 
def single_inserts():
    import pandas as pd
    import os
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='username',password='password',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    
    #Importing the CSV
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'yourcsv.xlsx')
    xlsx = pd.ExcelFile(csv_path)
    print(xlsx.sheet_names)
    #You can use the below if you have specific sheets to extract from and not all. 
    df1 = pd.read_excel(xlsx, "sheet_1")
    df2 = pd.read_excel(xlsx, "sheet_2")
    frames = [df1,df2]
    df = pd.concat(frames)
    
    cursor = conn.cursor()
    
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
    
    df = df.astype({'var1': str, 'var2': str, 'var3': str, 'var4': str, 'var5': int,'var6': str, 'var7': str,'var8': str, 'var9': str})
    print(df.info())
    
    for index, row in df.iterrows():
        query = "INSERT INTO table(var1, var2, var3, var4, var5, var6, var7, var8, var9) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}')".format(row['var1'], row['var2'], row['var3'],row['var4'],row['var5'],row['var6'],row['var7'],row['var8'],row['var9'])
        cursor.execute(query)
        print("single_inserts() done")
        
    
with DAG(
    default_args=default_args,
    dag_id="test44",
    start_date=datetime(2024, 9, 26),
    schedule_interval='@daily'
) as dag:
    
    task1 = PostgresOperator(
        task_id='check_connection',
        postgres_conn_id='order_postgres',
        sql="""
            select * from table; 
        """
    )
    
    task2 = PythonOperator(
        task_id='check_1',
        python_callable=install_dependencies,
    )
    
    task3 = PythonOperator(
        task_id='testing_connection',
        python_callable=testing_connection,
    )
    
    task4 = PythonOperator(
        task_id='Copying_data',
        python_callable=single_inserts
    )
    

    task1 >> task2 >> task3 >> task4
