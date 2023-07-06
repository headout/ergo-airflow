from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.utils import timezone
from airflow.utils.dates import days_ago

from ergo.config import Config
from ergo.operators.sqs.result_from_messages import \
    JobResultFromMessagesOperator

TASK_ID_SQS_COLLECTOR = "collect_sqs_messages"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=2),
    'start_date': days_ago(1),
}

sqs_queue_url = Config.sqs_result_queue_url
selenium_sqs_queue_url =  Config.selenium_sqs_result_queue_url
poke_interval_collector = Config.poke_interval_result_collector

with DAG(
    'ergo_job_collector',
    default_args=default_args,
    is_paused_upon_creation=False,
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    max_active_runs=Config.max_runs_dag_job_collector
) as dag:
    sqs_collector = SQSSensor(
        task_id=TASK_ID_SQS_COLLECTOR,
        sqs_queue=sqs_queue_url,
        max_messages=10,
        wait_time_seconds=10,
        poke_interval=poke_interval_collector
    )

    result_transformer = JobResultFromMessagesOperator(
        task_id='process_job_result',
        sqs_sensor_task_id=TASK_ID_SQS_COLLECTOR
    )

with DAG(
        'ergo_selenium_job_collector',
        default_args=default_args,
        is_paused_upon_creation=False,
        schedule_interval=timedelta(seconds=10),
        catchup=False,
        max_active_runs=Config.max_runs_dag_job_collector
) as dag:
    selenium_sqs_collector = SQSSensor(
        task_id='collect_selenium_sqs_messages',
        sqs_queue=selenium_sqs_queue_url,
        max_messages=10,
        wait_time_seconds=10,
        poke_interval=poke_interval_collector
    )

    selenium_result_transformer = JobResultFromMessagesOperator(
        task_id='process_selenium_job_result',
        sqs_sensor_task_id=TASK_ID_SQS_COLLECTOR
    )

sqs_collector >> result_transformer
selenium_sqs_collector >> selenium_result_transformer
