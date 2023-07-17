from datetime import timedelta

from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago

from ergo.config import Config
from ergo.operators.sqs.sqs_task_pusher import SqsTaskPusherOperator
from ergo.sensors.task_requests_batcher import TaskRequestBatchSensor

XCOM_REQUEST_TASK_KEY = "selenium.request.tasks"
XCOM_REQUEST_SQS_QUEUE_URL = "selenium.request.sqs_url"
TASK_ID_REQUEST_SENSOR = "selenium_collect_requests"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}

max_requests = Config.max_requests

max_concurrent_runs = Config.max_runs_dag_task_queuer
poke_interval_collector = Config.poke_interval_task_collector

with DAG(
        'selenium_ergo_task_queuer',
        default_args=default_args,
        is_paused_upon_creation=False,
        schedule_interval=timedelta(seconds=10),
        catchup=False,
        max_active_runs=max_concurrent_runs
) as dag:
    selenium_collector = TaskRequestBatchSensor(
        task_id=TASK_ID_REQUEST_SENSOR,
        max_requests=max_requests,
        xcom_sqs_queue_url_key=XCOM_REQUEST_SQS_QUEUE_URL,
        poke_interval=poke_interval_collector,
        timeout=timedelta(minutes=10).total_seconds()
    )

    selenium_pusher = SqsTaskPusherOperator(
        task_id="selenium_push_tasks",
        task_id_collector=TASK_ID_REQUEST_SENSOR,
        max_requests=max_requests,
        xcom_sqs_queue_url_key=XCOM_REQUEST_SQS_QUEUE_URL,
        use_row_lock=(max_concurrent_runs > 1)
    )

selenium_collector >> selenium_pusher
