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
    'retry_delay': timedelta(seconds=30),
    'start_date': days_ago(1),
    'priority_weight': 900,
}

sqs_queue_url = Config.sqs_result_queue_url
poke_interval_collector = Config.poke_interval_result_collector

for i in range(3):
    dag_id = f'ergo_job_collector_{i}'
    task_id_sqs_collector = f'collect_sqs_messages_{i}'
    task_id_result_transformer = f'process_job_result_{i}'
    with DAG(
        dag_id,
        default_args=default_args,
        is_paused_upon_creation=False,
        schedule_interval=timedelta(seconds=10),
        catchup=False,
        dagrun_timeout=timedelta(minutes=15),
        max_active_runs=Config.max_runs_dag_job_collector
    ) as dag:
        sqs_collector = SQSSensor(
            task_id=task_id_sqs_collector,
            sqs_queue=sqs_queue_url,
            max_messages=10,
            wait_time_seconds=10,
            poke_interval=poke_interval_collector,
            pool='job_collector_pool'
        )

        result_transformer = JobResultFromMessagesOperator(
            task_id=task_id_result_transformer,
            sqs_sensor_task_id=task_id_sqs_collector,
            pool='job_collector_pool'
        )

    sqs_collector >> result_transformer
