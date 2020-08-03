from datetime import datetime, timedelta
from random import choice

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.ergo import ErgoTaskProducerOperator
from airflow.sensors.ergo import ErgoJobResultSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'start_date': days_ago(1),
}

SAMPLE_TASK_IDS = ['noArg', 'oneArg', 'instance_noArg', 'spring_noArg']
SAMPLE_TASK_DATA = [{}, {'value': 2}, {'val': 'sa'}]


def random_task_decider():
    return choice(SAMPLE_TASK_IDS), choice(SAMPLE_TASK_DATA)


with DAG(
    'example_sqs',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
) as dag:
    start_task = DummyOperator(task_id="start")
    stop_task = DummyOperator(task_id="stop")

    push_task_to_sqs = ErgoTaskProducerOperator(
        task_id='example_task_pusher',
        ergo_task_callable=random_task_decider
    )

    wait_job_result = ErgoJobResultSensor(
        task_id='example_job_sensor',
        pusher_task_id='example_task_pusher'
    )


start_task >> push_task_to_sqs >> wait_job_result >> stop_task
