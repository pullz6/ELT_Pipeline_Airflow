# Creating a ELT pipeline in Airflow

This is project is to create ELT pipeline to maintain supply chains in airflow. You can use the dags created inside the dag folder to understand how to connect to postgres, how to insert data from a csv file by creating a pandas dataframe and how to retrieve data from the postgres server. 

## Pre-requisities 
Please ensure that the below are installed: 
1. Docker 
2. Docker Compose
3. Postgres
4. Postgres Server, Database and a Table

## Installation
You can pull the Airflow Docker Image as per the instructions available in the Apache Airflow documentation -> https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## Explanation 
Here we are tackling a full load of data and a incremental load of data. 

### Full Load
Here we are loading an entire dataset into the database. For example when you have first created the pipeline you might want to add previous data into the server. The .py file named as Full_load.py performs a full_load. 

### Incremental Load
Here we are loading only a certain portion of a dataset into the database. For example, when you have some data collection over time after an inital load, you will only upload the data that is newly added. 
The .py file named as Incremental_load.py performs a incremental load. 





