import random

import pytz
import cloudpickle as pickle
import os

from datetime import datetime, timedelta

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from lib.aws import S3
from my_sub_dag import random_search_subdag
from pyspark import SparkConf

PARENT_DAG_NAME = 'hyperparameter_search'
default_args = {
    'owner': 'lucas.silva',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
}


main_dag = DAG(
  dag_id=PARENT_DAG_NAME,
  schedule_interval='@once',
  start_date=datetime(2019, 10, 29),
  default_args=default_args
)


s3_client = S3(endpoint_url='http://localstack:4572')


def download_data(path):
    import requests
    URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00492/Metro_Interstate_Traffic_Volume.csv.gz'
    file = requests.get(url=URL)
    s3_client.put_object(data=file.content, s3_path=path, bucket='test')

download_data_task = PythonOperator(
        python_callable=download_data,
        op_kwargs=dict(path='data/RAW_DATA_Metro_Interstate_Traffic_Volume.csv.gz'),
        task_id='download_data_task',
        dag=main_dag
)

conf = dict()
conf["spark.jars.packages"] = "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.2"
s3_client.download_s3_file('functions/etl_job.py', '/usr/local/airflow/dags/etl_job.py')
spark_etl_task = SparkSubmitOperator(
    conf=conf,
    application='/usr/local/airflow/dags/etl_job.py',
    task_id='spark_etl_task',
    conn_id='spark_test',
    dag=main_dag
)


def hyper_pick(path, **context):
    training = pickle.loads(s3_client.get_object(s3_path=path))
    print('TRAINING: {}'.format(training))
    hyperparams = training['hyperparameters']
    n_iterations = training['n_iterations']

    hypers_picked = [{k:random.choice(v) for k,v in hyperparams.items()} for _ in range(n_iterations)]
    print('HYPERS_PICKED: {}'.format(hypers_picked))

    print(context)

    context['task_instance'].xcom_push(key='hypers', value=hypers_picked)

pick_hyper_task = PythonOperator(
        python_callable=hyper_pick,
        op_kwargs=dict(path='functions/training.pickle'),
        task_id='pick_hyper_task',
        dag=main_dag,
        provide_context=True
)

training_task = SubDagOperator(
    subdag=random_search_subdag(PARENT_DAG_NAME, 'hyperparameter_optmization', main_dag.start_date, main_dag.schedule_interval, main_dag.default_args,
                                path='functions/training.pickle'),
    task_id='hyperparameter_optmization',
    dag=main_dag
)


pack_it_up_bois_task = DummyOperator(
        task_id='pack_it_up_bois',
        dag=main_dag,
)

download_data_task >> spark_etl_task >> pick_hyper_task >> training_task >> pack_it_up_bois_task


if __name__ == "__main__":
    DEFAULT_DATE = datetime.now()
    DEFAULT_DATE = DEFAULT_DATE.replace(tzinfo=pytz.utc)
    print(DEFAULT_DATE)
    print('START')
    # sub_dag_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    # test_select.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
