from airflow.configuration import conf

from ergo import SECTION_NAME


class Config(object):
    max_requests = conf.getint(SECTION_NAME, "max_task_requests", fallback=10)
    sqs_request_queue_url = conf.get(SECTION_NAME, "request_queue_url")
    sqs_result_queue_url = conf.get(SECTION_NAME, "result_queue_url")
