from datetime import timedelta

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from sqlalchemy import func, text

from ergo.config import Config
from ergo.models import ErgoTask


class TaskRequestBatchSensor(BaseSensorOperator):
    """
    TaskRequestBatchSensor is a sensor to wait for and collect all the 'scheduled' Ergo Tasks
    and stores all the relevant ones in XCom to be later pushed by 'SqsTaskPusherOperator'.

    It has certain required conditions for tasks to have:
    - the collected tasks must have the same SQS queue URL
    - the collected tasks must be in SCHEDULED or UP_FOR_RESCHEDULE states
    - the number of tasks collected must not be more than `max_requests` (10)

    However, there's an extra set of rules to finally push these collected tasks:
    - The tasks which are waiting in SCHEDULED state for more than the configured wait threshold
    must be considered urgent tasks and should be pushed without any delay.
    - If there are no urgent tasks, then wait till last retry if the number of tasks is less than `max_requests`.
    - If at any time of poke, there are `max_requests` number of collected tasks, then push these tasks.
    - If the sensor is at last retry, then push all the collected tasks regardless if they number less than `max_requests`.
    """

    filter_ergo_task = ErgoTask.state.in_(
        [State.SCHEDULED, State.UP_FOR_RESCHEDULE])

    @apply_defaults
    def __init__(
        self,
        max_requests: int,
        xcom_sqs_queue_url_key: str,
        *args,
        **kwargs
    ):
        _poke_interval = kwargs.get('retry_delay', timedelta(minutes=1))
        _timeout = _poke_interval * kwargs.get('retries', 10)
        kwargs['soft_fail'] = True
        kwargs['poke_interval'] = kwargs.get(
            'poke_interval', _poke_interval.total_seconds())
        kwargs['timeout'] = kwargs.get('timeout', _timeout.total_seconds())
        super().__init__(*args, **kwargs)
        self.max_requests = max_requests
        self.xcom_sqs_queue_url_key = xcom_sqs_queue_url_key
        self.urgent_task_wait_threshold = Config.queue_wait_threshold

    def poke(self, context):
        now = timezone.utcnow()
        self.log.info('Querying for %s tasks...', State.SCHEDULED)
        task_id = context['task'].task_id
        queue = self.choose_queue(task_id)
        if not queue:
            self.log.info('No task is pending to be queued!')
            return False
        queue_url, cnt_tasks = queue[0], queue[1]
        #if cnt_tasks < self.max_requests and context['ti'].is_eligible_to_retry():
        #    return False
        self.log.info('Found %d tasks', cnt_tasks)
        self.xcom_push(context, self.xcom_sqs_queue_url_key, queue_url)
        return True

    @provide_session
    def choose_queue(self, task_id, session=None) -> tuple:
        if task_id == "selenium_collect_requests":
            queue_url = Config.selenium_sqs_request_queue_url
        elif task_id == "aries_collect_requests":
            queue_url = Config.aries_sqs_request_queue_url
        elif task_id == "calipso_collect_requests":
            queue_url = Config.calipso_sqs_request_queue_url
        else:
            queue_url = Config.sqs_request_queue_url
        self.log.info('Checking for scheduled tasks in %s', queue_url)
        count = (
            session.query(
                ErgoTask.id
            ).filter(self.filter_ergo_task,ErgoTask.queue_url == queue_url).count()
        )
        if count > 0:
            return queue_url, count
        return None
