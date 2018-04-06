from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import date
import os
import json

today = date.today().strftime("%Y%m%d")
dag = DAG('tw-pipeline', description='ThoughtWorks Data Engineering Development Program - Pipeline',
          schedule_interval='0 10 * * *',
          start_date=datetime.now(),
          catchup=False)
filePath = os.path.dirname(os.path.abspath('config.json'))+ "/config/config.json"
with open(filePath, 'r') as f:
    config = json.load(f)

def ingest_cleanup_factory(parentDag,config,dateStr,upstreamTask,downstreamTask):
    print(config['ingest']['output']['hdfs']['dataSets'])
    targetMap = config['ingest']['output']['hdfs']['dataSets']
    for source in config['ingest']['sources']:
        path=config['ingest']['output']['hdfs']['uri'] %(config['common']['hdfs']['lake1Path']
            , targetMap[source]
            ,dateStr)
        path = path.replace('file://','')
        print('Removing'+path)
        task=BashOperator(
            task_id='cleanup_dir_'+source,
             bash_command='rm -r '+path,
            retries=3,
            dag=parentDag)
        task.set_upstream(upstreamTask)
        task.set_downstream(downstreamTask)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

sparkTask =  SparkSubmitOperator(
    task_id='spark_ingest'+ today,
    application='/usr/local/target/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar',
    jars='/usr/local/target/config-1.3.2.jar',
    java_class='com.thoughtworks.ca.de.batch.ingest_to_data_lake.DailyDriver',
    dag=dag)

# ingest_cleanup_factory(dag,config,today,dummy_operator,sparkTask)

# dummy_operator
sparkTask