from datetime import timedelta

from airflow.configuration import conf

from ergo import SECTION_NAME


class Config(object):
    max_requests = conf.getint(SECTION_NAME, "max_task_requests", fallback=10)
    sqs_request_queue_url = conf.get(SECTION_NAME, "request_queue_url")
    sqs_result_queue_url = conf.get(SECTION_NAME, "result_queue_url")
    queue_wait_threshold = timedelta(minutes=conf.getint(SECTION_NAME, "queue_wait_threshold_mins", fallback=3))

    max_runs_dag_task_queuer = conf.getint(SECTION_NAME, "max_runs_dag_task_queuer", fallback=1)
    max_runs_dag_job_collector = conf.getint(SECTION_NAME, "max_runs_dag_job_collector", fallback=1)
