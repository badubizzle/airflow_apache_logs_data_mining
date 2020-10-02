setenv:;export AIRFLOW_HOME=`pwd`
run-init:; export AIRFLOW_HOME=`pwd` && airflow initdb
run-server:; export AIRFLOW_HOME=`pwd` && airflow webserver -p 8090
run-scheduler:; export AIRFLOW_HOME=`pwd` && airflow scheduler
run-watcher:; export AIRFLOW_HOME=`pwd` && python dags/watcher.py

run:; nohup make run-server > server.log & nohup make run-scheduler > scheduler.log & make run-watcher