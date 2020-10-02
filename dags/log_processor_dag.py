import csv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

try:
    from dags import load_env, models
except:
    import load_env
    import models


def logs_to_csv(log_file_path):
    csv_file = log_file_path + ".csv"

    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = models.WebLog.CSV_HEADER
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        with open(log_file_path, 'r') as f:
            line = f.readline()
            while line:
                log = models.WebLog.parse(line)
                data = log.to_dict()
                writer.writerow(data)
                line = f.readline()
            f.close()
        csvfile.close()


def process_last_file(*args, **kwargs):
    conf = kwargs.get('dag_run').conf

    if 'process_file_name' in conf:
        file = conf['process_file_name']
        print('Processing file: ', file)
        logs_to_csv(file)
        return 'Done', file

    return None


default_args2 = {
    'owner': 'airlfow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 5),
    'email': ['badu.boahen@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag3 = DAG(dag_id='process_log_file',
           default_args=default_args2,
           schedule_interval=None)
t2 = PythonOperator(task_id='process_log_task',
                    provide_context=True,
                    python_callable=process_last_file, dag=dag3)
