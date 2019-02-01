"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'tw',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 30),
    'email': ['test@thoughtworks.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

date_now = datetime.now().isoformat()
folder = '/Users/cpatel/src/data-training/batch-data-pipeline/tw-pipeline/target/citibike-{}'.format(date_now)
input_csv_file = '/Users/cpatel/src/data-training/batch-data-pipeline/sample-data/citibike.csv'
parquet_file = "{}/citibike.parquet".format(folder)
parquet_transformed_file = '/citibike-transformed.parquet'

dag = DAG('citibike', default_args=default_args, schedule_interval=timedelta(minutes=3), max_active_runs = 1)

package_path = '/Users/cpatel/src/data-training/batch-data-pipeline/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar'


t1 = BashOperator(
    task_id='ingest_to_datalake',
    bash_command='mkdir {0} && spark-submit --class com.thoughtworks.ca.de.batch.ingest_to_data_lake.DailyDriver --master local {1} {2} {3} && echo {0} '.format(folder, package_path, input_csv_file, parquet_file),
    xcom_push=True,
    dag=dag)

t2 = BashOperator(
    task_id='transform',
    bash_command="spark-submit --class com.thoughtworks.ca.de.batch.citibike.CitibikeTransformer --master local " + package_path + " {{ task_instance.xcom_pull(task_ids='ingest_to_datalake') }}/citibike.parquet {{ task_instance.xcom_pull(task_ids='ingest_to_datalake') }}/" + parquet_transformed_file,
    dag=dag)

t1.set_downstream(t2)

