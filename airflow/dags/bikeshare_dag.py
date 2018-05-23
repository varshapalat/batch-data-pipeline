from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import date
import json
import shutil
import os

today = date.today().strftime("%Y%m%d")
dag = DAG('bike_share_pipeline', description='ThoughtWorks Data Engineering Development Program - Bikeshare Pipeline',
          schedule_interval='0 10 * * *',
          start_date=datetime.now(),
          catchup=False)
filePath = os.path.dirname(os.path.abspath('config.json')) + "/config/config.json"
with open(filePath, 'r') as f:
    config = json.load(f)

# Constants
JARS = '/usr/local/target/config-1.3.2.jar'
INGEST_CLASS = 'com.thoughtworks.ca.de.batch.ingest_to_data_lake.DailyDriver'
TRANSFORM_CLASS = 'com.thoughtworks.ca.de.batch.citibike.CitibikeTransformer'
APP = 'file:///usr/local/target/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar'


def cleanup_dir(path):
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)


def ingest_cleanup_factory(parentDag, config, dateStr, upstreamTask, downstreamTask):
    print(config['ingest']['output']['hdfs']['dataSets'])
    targetMap = config['ingest']['output']['hdfs']['dataSets']
    sourcePath = config['ingest']['sources']['bikeShareData']
    ingestPath = config['ingest']['output']['hdfs']['uri'] % (config['common']['hdfs']['lake1Path']
                                                              , targetMap['bikeShareData']
                                                              , dateStr)
    ingestCleanupPath = '/' + config['common']['hdfs']['lake1Path'] + '/' + targetMap['bikeShareData'] + '/' + dateStr
    transformDatasetId = config['transform']['hdfs']['dataSets']['bikeShareData']
    transformPath = config['transform']['hdfs']['uri'] % (config['common']['hdfs']['lake2Path']
                                                          , transformDatasetId
                                                          , dateStr)
    transformCleanupPath = '/' + config['common']['hdfs']['lake2Path'] + '/' + transformDatasetId + '/' + dateStr

    print("Cleaning up ingest: " + ingestCleanupPath)
    ingestCleanupTask = PythonOperator(
        task_id='cleanup_ingest_dir_' + transformDatasetId,
        python_callable=cleanup_dir,
        op_kwargs={'path': ingestCleanupPath},
        dag=parentDag)
    ingestCleanupTask.set_upstream(upstreamTask)

    ingestSparkTask = SparkSubmitOperator(
        task_id='spark_ingest_' + transformDatasetId,
        application=APP,
        jars=JARS,
        conn_id='spark_default',
        java_class=INGEST_CLASS,
        retries=3,
        application_args=[sourcePath, ingestPath],
        verbose=True,
        dag=parentDag)
    ingestSparkTask.set_upstream(ingestCleanupTask)

    print("Cleaning up transform: " + transformCleanupPath)
    transformCleanupTask = PythonOperator(
        task_id='cleanup_transform_dir_' + transformDatasetId,
        python_callable=cleanup_dir,
        op_kwargs={'path': transformCleanupPath},
        dag=parentDag)
    transformCleanupTask.set_upstream(ingestSparkTask)

    transformSparkTask = SparkSubmitOperator(
        task_id='spark_transform_' + transformDatasetId,
        application=APP,
        jars=JARS,
        conn_id='spark_default',
        java_class=TRANSFORM_CLASS,
        retries=3,
        application_args=[ingestPath, transformPath, transformDatasetId],
        verbose=True,
        dag=parentDag)
    transformSparkTask.set_upstream(transformCleanupTask)
    transformSparkTask.set_downstream(downstreamTask)


start_operator = DummyOperator(task_id='start_task', retries=3, dag=dag)
end_operator = DummyOperator(task_id='end_task', retries=3, dag=dag)

ingest_cleanup_factory(dag, config, today, start_operator, end_operator)

start_operator
