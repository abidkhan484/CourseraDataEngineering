"""
Commands:

cp ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags
airflow dags list
airflow tasks list ETL-server-access-log-processing

"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner': 'Iron Man',
  'start_date': days_ago(0),
  'email': ['ironman@somemail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'ETL-server-access-log-processing',
  default_args=default_args,
  description='ETL server access log processing assignment of coursera course',
  schedule_interval=timedelta(days=1)
)

download = BashOperator(
  task_id='download',
  bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt',
  dag=dag,
)

extract = BashOperator(
  task_id='extract',
  bash_command='cut -d"#" -f1,4 web-server-access-log.txt > /home/project/airflow/dags/extracted-data.txt',
  dag=dag,
)

transform = BashOperator(
  task_id='transform',
  bash_command='tr [:lower:] [:upper:] < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.txt',
  dag=dag,
)

load = BashOperator(
  task_id='load',
  bash_command='tar -czf /home/project/airflow/dags/ETL_server_access_log_processing.tar.gz /home/project/airflow/dags/extracted-data.txt /home/project/airflow/dags/transformed-data.txt',
  dag=dag,
)

download >> extract >> transform >> load