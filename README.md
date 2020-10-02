## Airflow Apache Logs Data Mining Pipeline (ETL)

The project demonstrate mining data from apache logs using airflow.

1) Apache Log --> 2) Extract User/Request Details --> 3) Save transformed data to csv

We are using 2 DAGs

1. Generate sample apache logs
2. Extract request/user information from logs and transform into a csv file

To make it simple, there's a DAG for generating sample apache logs based on fake data.

For simplicity, we use a file watcher that monitors the log directory.
Once a new log file is created, the watcher script triggers the DAG to extract and transform the log file.
