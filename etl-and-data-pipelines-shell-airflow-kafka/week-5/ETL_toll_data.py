
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow import DAG
import pendulum
from os import chdir


source_dir = '/infinity/codes/data_engineering/etl-and-data-pipelines-shell-airflow-kafka/tools/apache-workflow/dags/finalassignment'

default_args = {
  'owner': 'Iron Man',
  'start_date': pendulum.now('Asia/Dhaka'),
  'email': ['ironman@somemail.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

chdir(source_dir)

dag = DAG(
  'ETL_toll_data',
  default_args=default_args,
  description='Apache Airflow Final Assignment',
  schedule=timedelta(days=1)
)

unzip_data = BashOperator(
  task_id='unzip_data',
  bash_command='tar -xzf tolldata.tgz',
  dag=dag,
)

# Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type
extract_data_from_csv = BashOperator(
  task_id='extract_data_from_csv',
  bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
  dag=dag,
)

# Number of axles, Tollplaza id, and Tollplaza code
extract_data_from_tsv = BashOperator(
  task_id='extract_data_from_tsv',
  bash_command='tr "\t" "," < tollplaza-data.tsv | cut -d"," -f5-7 > tsv_data.csv',
  dag=dag,
)

# Type of Payment code, Vehicle Code
extract_data_from_fixed_width = BashOperator(
  task_id='extract_data_from_fixed_width',
  bash_command='cut -c 59-62,63-68 payment-data.txt | tr " " "," > fixed_width_data.csv',
  dag=dag,
)

# Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code
consolidate_data = BashOperator(
  task_id='consolidate_data',
  bash_command='paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
  dag=dag,
)

# transform the vehicle_type field
transform_data = BashOperator(
  task_id='transform_data',
  bash_command='cut -d","" -f4 extracted_data.csv | tr ""[:lower:]" "[:upper:]" > transformed_data.csv',
  dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv \
>> extract_data_from_fixed_width >> consolidate_data >> transform_data

