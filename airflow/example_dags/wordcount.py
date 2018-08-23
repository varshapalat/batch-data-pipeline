from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('wordcount_dag', default_args=default_args)

spark_submit_wordcount_locally = """
    spark-submit --class com.thoughtworks.ca.de.batch.wordcount.WordCount --jars ./batch-data-pipeline/config-1.3.2.jar --conf spark.sql.shuffle.partitions=1 ./batch-data-pipeline/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar ./batch-data-pipeline/sample-data/words.txt ./batch-data-pipeline/wordcountoutput
"""

t1 = BashOperator(
    task_id='count_words_task',
    bash_command=spark_submit_wordcount_locally,
    dag=dag)
