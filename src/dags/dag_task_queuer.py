from datetime import timedelta

from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago

from ergo.config import Config
from ergo.operators.sqs.sqs_task_pusher import SqsTaskPusherOperator
from ergo.sensors.task_requests_batcher import TaskRequestBatchSensor

XCOM_REQUEST_TASK_KEY = "request.tasks"
TASK_ID_REQUEST_SENSOR = "collect_requests"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(1),
}

max_requests = Config.max_requests
sqs_queue_url = Config.sqs_request_queue_url

with DAG(
    'ergo_task_queuer',
    default_args=default_args,
    is_paused_upon_creation=False,
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    max_active_runs=1
) as dag:
    collector = TaskRequestBatchSensor(
        task_id=TASK_ID_REQUEST_SENSOR,
        max_requests=max_requests,
        xcom_tasks_key=XCOM_REQUEST_TASK_KEY,
        poke_interval=timedelta(minutes=2).total_seconds(),
        timeout=timedelta(minutes=10).total_seconds()
    )

    pusher = SqsTaskPusherOperator(
        task_id="push_tasks",
        task_id_collector=TASK_ID_REQUEST_SENSOR,
        xcom_tasks_key=XCOM_REQUEST_TASK_KEY,
        sqs_queue_url=sqs_queue_url
    )

collector >> pusher
