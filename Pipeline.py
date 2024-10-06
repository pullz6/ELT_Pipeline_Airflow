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

def load_csv(): 
    import pandas as pd
    import os
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'india-pipeline-1.xlsx')
    df = pd.read_excel(csv_path)
    print(df)
    
def testing_connection():
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    if conn is None:
        print("Error connecting to the MySQL database")
    else:
        print("MySQL connection established!")
    conn.close()
    
""" def copy_csv_to_table():
    import pandas as pd
    import os
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    
    #Importing the CSV
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'india-pipeline-1.xlsx')
    df = pd.read_excel(csv_path)
    
    #Creating a dataframe with the required dataframe columns
    df = df[['Order Received Date', 'Style No','Brand Name','Product Name','Order Qty','Order Value (USD)','Job Status','Production Status','Delivery Date']]
    
    #Creating tuples from the dataframe 
    tpls = [tuple(x) for x in df.to_numpy()]
    print(tpls)
    
    # SQL query to execute
    sql = "INSERT INTO orders_india(order_rec_date, style_no, brand, product_code, order_qty, order_value, job_status, production_status, delivery_date ) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)"

    cursor = conn.cursor()
    print(tpls)
    #Running the sql with all the dataframe
    try:
        cursor.executemany(sql, tpls)
        print("Data inserted using execute_many() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
    cursor.close() """
    
def single_inserts():
    import pandas as pd
    import os
    
    #Creating the connection with the postgres
    conn = psycopg2.connect(database='orders',user='pulsaragunawardhana',password='',
                            host='host.docker.internal',port='5432')
    conn.autocommit = True
    
    #Importing the CSV
    current_dag_directory = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dag_directory, 'india-pipeline-1.xlsx')
    xlsx = pd.ExcelFile(csv_path)
    print(xlsx.sheet_names)
    df1 = pd.read_excel(xlsx, "AUGUST ")
    df2 = pd.read_excel(xlsx, "SEPTEMBER")
    frames = [df1,df2]
    df = pd.concat(frames)
    
    cursor = conn.cursor()
    
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
    print(df.info())
    
    for index, row in df.iterrows():
        query = "INSERT INTO orders_india(order_rec_date, style_no, brand, product_code, order_qty, order_value, job_status, production_status, delivery_date ) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}')".format(row['Order Received Date'], row['Style No'], row['Brand Name'],row['Product Name'],row['Order Qty'],row['Order Value (USD)'],row['Job Status'],row['Production Status'],row['Delivery Date'])
        cursor.execute(query)
        print("single_inserts() done")
        
    
    """ for i in df.index:
        cols  = ','.join(list(df.columns))
        vals  = [df.at[i,col] for col in list(df.columns)]
        print(type(vals))
        query = "INSERT INTO orders_india(order_rec_date, style_no, brand, product_code, order_qty, order_value, job_status, production_status, delivery_date ) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}')".format(vals[0], vals[1], vals[2],vals[3],vals[4],vals[5],vals[6],vals[7],vals[8])
        cursor.execute(query)
    print("single_inserts() done") """
    
with DAG(
    default_args=default_args,
    dag_id="test44",
    start_date=datetime(2024, 9, 26),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='loading_csv',
        python_callable=load_csv
    )
    
    task2 = PostgresOperator(
        task_id='check_connection',
        postgres_conn_id='order_postgres',
        sql="""
            select * from orders_india; 
        """
    )
    
    task3 = PythonOperator(
        task_id='check_1',
        python_callable=install_dependencies,
    )
    
    task4 = PythonOperator(
        task_id='testing_connection',
        python_callable=testing_connection,
    )
    
    task5 = PythonOperator(
        task_id='Copying_data',
        python_callable=single_inserts
    )
    

    task1 >> task2 >> task3 >> task4 >> task5