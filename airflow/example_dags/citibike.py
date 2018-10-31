import json
import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval':'@once',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('cc_bicycle_2', default_args=default_args)


read_csv_cmd = """
export AWS_DEFAULT_REGION=ap-southeast-1
step=$(aws emr add-steps --cluster-id j-OPIZPHB8D8Q4 --steps Type=Spark,Name="emr read bicycle csv",ActionOnFailure=CONTINUE,Args=[--class,com.thoughtworks.ca.de.batch.citibike.CitibikeReadCSV,--jars,/home/hadoop/cchuang/config-1.3.2.jar,/home/hadoop/cchuang/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar,/user/hadoop/cchuang/raw/sample_citybikes.csv,/user/hadoop/cchuang/canonical/sample_citybikes.parquet] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
echo '========='$step
aws emr wait step-complete --cluster-id j-OPIZPHB8D8Q4 --step-id $step
"""

cal_distance_cmd = """
export AWS_DEFAULT_REGION=ap-southeast-1
step=$(aws emr add-steps --cluster-id j-OPIZPHB8D8Q4 --steps Type=Spark,Name="calc bicycle distance",ActionOnFailure=CONTINUE,Args=[--class,com.thoughtworks.ca.de.batch.citibike.CitibikeReadCSV,--jars,/home/hadoop/cchuang/config-1.3.2.jar,/home/hadoop/cchuang/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar,/user/hadoop/cchuang/canonical/sample_citybikes.parquet,/user/hadoop/cchuang/mart/sample_citybikes.parquet] | python -c 'import json,sys;obj=json.load(sys.stdin);print obj.get("StepIds")[0];')
echo '========='$step
aws emr wait step-complete --cluster-id j-OPIZPHB8D8Q4 --step-id $step
"""

read_csv_task = BashOperator(
    task_id='read_csv_task',
    bash_command=read_csv_cmd,
    dag=dag)

calc_distance_task = BashOperator(
    task_id='calc_distance_task',
    bash_command=cal_distance_cmd,
    dag=dag)

read_csv_task.set_downstream(calc_distance_task)
