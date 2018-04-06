from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date
import json
import shutil
import os

today = date.today().strftime("%Y%m%d")
dag = DAG('bike_share_pipeline', description='ThoughtWorks Data Engineering Development Program - Bikeshare Pipeline',
          schedule_interval='0 10 * * *',
          start_date=datetime.now(),
          catchup=False)

with open('/Users/alpandy/dev-eng/newrepo/twde-capabilities/airflow/config.json', 'r') as f:
    config = json.load(f)

#Constants
SPARK_SUBMIT='spark-submit'
MODE='--deploy-mode client'
OPTIONS='--jars /Users/alpandy/dev-eng/first-cohort/twde-capabilities/tw-pipeline/config-1.3.2.jar --master local'
INGEST_DRIVER = '--class com.thoughtworks.ca.de.batch.ingest.DailyDriver'
TRANSFORM_DRIVER = '--class com.thoughtworks.ca.de.batch.ingest.DailyDriver'
APP = '/Users/alpandy/dev-eng/newrepo/twde-capabilities/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar'

def cleanup_dir(path):
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)

def ingest_cleanup_factory(parentDag,config,dateStr,upstreamTask,downstreamTask):
    print(config['ingest']['output']['hdfs']['dataSets'])
    targetMap = config['ingest']['output']['hdfs']['dataSets']
    sourcePath = config['ingest']['sources']['bikeShareData']
    ingestPath = config['ingest']['output']['hdfs']['uri'] %(config['common']['hdfs']['lake1Path']
        , targetMap['bikeShareData']
        ,dateStr)
    ingestCleanupPath='/'+config['common']['hdfs']['lake1Path']+'/'+targetMap['bikeShareData']+'/'+dateStr
    transformDatasetId = config['transform']['hdfs']['dataSets']['bikeShareData']
    transformPath = config['transform']['hdfs']['uri'] %(config['common']['hdfs']['lake2Path']
        , transformDatasetId
        ,dateStr)
    transformCleanupPath = '/'+config['common']['hdfs']['lake2Path']+'/'+transformDatasetId+'/'+dateStr

    print("Cleaning up ingest: "+ingestCleanupPath)
    # ingestCleanupTask=BashOperator(
    #     task_id='cleanup_ingest_dir_'+transformDatasetId,
    #     bash_command='rm -r '+ingestCleanupPath,
    #     retries=3,
    #     dag=parentDag)
    ingestCleanupTask = PythonOperator(
        task_id='cleanup_ingest_dir_'+transformDatasetId,
        python_callable=cleanup_dir,
        op_kwargs={'path': ingestCleanupPath},
        dag=parentDag)
    ingestCleanupTask.set_upstream(upstreamTask)

    print("Ingest spark command: "+SPARK_SUBMIT+' '+MODE+' '+OPTIONS+' '+INGEST_DRIVER+' '+APP+' {{ params.sourcePath }} {{ params.ingestPath }}')
    ingestSparkTask=BashOperator(
        task_id='spark_ingest_'+transformDatasetId,
        bash_command=SPARK_SUBMIT+' '+MODE+' '+OPTIONS+' '+INGEST_DRIVER+' '+APP+' '+sourcePath + ' ' +ingestPath ,
        retries=3,
        params={'sourcePath': sourcePath,
                'ingestPath': ingestPath},
        dag=parentDag)
    ingestSparkTask.set_upstream(ingestCleanupTask)

    print("Cleaning up transform: "+transformCleanupPath)
    # transformCleanupTask=BashOperator(
    #     task_id='cleanup_transform_dir_'+transformDatasetId,
    #     bash_command='rm -r '+transformCleanupPath,
    #     retries=3,
    #     dag=parentDag)
    transformCleanupTask = PythonOperator(
        task_id='cleanup_transform_dir_'+transformDatasetId,
        python_callable=cleanup_dir,
        op_kwargs={'path': transformCleanupPath},
        dag=parentDag)
    transformCleanupTask.set_upstream(ingestSparkTask)

    print("Transform Spark command: "+SPARK_SUBMIT+' '+MODE+' '+OPTIONS+' '+TRANSFORM_DRIVER+' '+APP+' '+ingestPath+' '+ transformPath +' '+ transformDatasetId )
    transformSparkTask=BashOperator(
        task_id='spark_transform_'+transformDatasetId,
        bash_command=SPARK_SUBMIT+' '+MODE+' '+OPTIONS+' '+TRANSFORM_DRIVER+' '+APP+' {{ params.ingestPath }} {{ params.transformPath }}  {{ params.transformDatasetId }}',
        retries=3,
        params={'ingestPath': ingestPath,
                'transformPath': transformPath,
                'transformDatasetId': transformDatasetId},
        dag=parentDag)
    transformSparkTask.set_upstream(transformCleanupTask)
    transformSparkTask.set_downstream(downstreamTask)


start_operator = DummyOperator(task_id='start_task', retries=3, dag=dag)
end_operator = DummyOperator(task_id='end_task', retries=3, dag=dag)


ingest_cleanup_factory(dag,config,today,start_operator,end_operator)

start_operator
