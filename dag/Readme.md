## Notes

As mentioned, this folder has the dags for a full and incremental dag. You can use these codes as templates for your full and incremental dags.

Here to establish the connection between Airflow and Postgres, the psycopy2 python library was used. However, you can also use the Apache Airflow connection tab to create a connection. 

### Remember, when you are creating a connection between the docker container and a server to use 'host.docker.internal' when a host is requested. 
