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

dag = DAG('wordcount_emr_dag', default_args=default_args)

spark_submit_wordcount_locally = """
export AWS_DEFAULT_REGION=eu-west-2
aws emr add-steps --cluster-id j-ABCD12345ABC --steps Type=Spark,Name="Wordcount Program With EMR Step",ActionOnFailure=CONTINUE,Args=[--jars,/home/hadoop/trainee/config-1.3.2.jar,--class,com.thoughtworks.ca.de.batch.wordcount.WordCount,--master,local,/home/hadoop/trainee/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar,hdfs:///user/hadoop/trainee/raw/words.txt,hdfs:///user/hadoop/trainee/canonical/wordcount/]
"""

t1 = BashOperator(
    task_id='count_words_task',
    bash_command=spark_submit_wordcount_task,
    dag=dag)
