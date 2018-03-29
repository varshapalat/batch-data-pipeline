from airflow import utils
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import date

today = datetime.date.today().strftime("%Y%m%d")
