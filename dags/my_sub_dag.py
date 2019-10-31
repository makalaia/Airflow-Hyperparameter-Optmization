import random
import logging
import cloudpickle as pickle

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from lib.aws import S3

# from .random_search_dag import PARENT_DAG_NAME


def train_net(train_method, iteration_n, **context):
    hypers_picked = context['task_instance'].xcom_pull(key='hypers', task_ids='pick_hyper_task', dag_id='hyperparameter_search')
    hyper_picked = hypers_picked[iteration_n]

    train_method('data/train_preprocessed.parquet', **hyper_picked)


def random_search_subdag(parent_dag_name, child_dag_name, start_date, schedule_interval, default_args,
                         path='functions/training.pickle'):
    # dag is built based on S3
    s3_client = S3(endpoint_url='http://localstack:4572')
    training = pickle.loads(s3_client.get_object(s3_path=path))

    train_method = training['callable']
    n_iterations = training['n_iterations']

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args=default_args
    )



    for i in range(n_iterations):
        train_task = PythonOperator(
            python_callable=train_net,
            op_kwargs=dict(train_method=train_method, iteration_n=i),
            task_id='train_task_{}'.format(i),
            dag=dag,
            provide_context=True
        )

    return dag

if __name__ == '__main__':
    s3_client = S3()
    s3_client.get_object('functions/training.pickle')
    import pan
    pop