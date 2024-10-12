# Creating a ELT pipeline in Airflow

This is project was used to create ELT pipeline to maintain supply chains in airflow. You can use the dags created inside the dag folder to understand how to implement a full and incremental data load, how to connect to postgres, how to insert data from a csv file by creating a pandas dataframe and how to retrieve data from the postgres server. 

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

The first screenshot is the final execution of the incremental_load, you can see that final run is successful and the event logs confirming that the single inserts by the dataframe's rows are complete. Consequently data has being loaded into the postgres table as shown in the next screenshot. 

![image](https://github.com/user-attachments/assets/c37d97ff-e0dd-44be-a255-694bb5d0d008)

<img width="372" alt="Screenshot 2024-10-12 at 15 29 35" src="https://github.com/user-attachments/assets/b07e1dd6-5285-4acf-ac74-100c2a9fa0a0">

#### ITS IMPORTANT TO NOTE THAT YOU CANNOT RUN THE LOADS FOR THE SAME SET OF DATA SEVERAL TIMES SINCE IT WILL CAUSE A PRIMARY KEY ERROR!




