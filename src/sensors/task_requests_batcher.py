from datetime import timedelta

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State

from ergo.config import Config
from ergo.models import ErgoTask


class TaskRequestBatchSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(
        self,
        max_requests: int,
        xcom_tasks_key: str,
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
        self.xcom_tasks_key = xcom_tasks_key

    @provide_session
    def poke(self, context, session=None):
        now = timezone.utcnow()
        self.log.info('Querying for %s tasks...', State.QUEUED)
        result = session.query(ErgoTask).filter_by(
            state=State.SCHEDULED
        ).order_by(ErgoTask.ti_execution_date).limit(self.max_requests)
        result = list(result)
        urgent_task_idx = None
        for idx, task in enumerate(result):
            if now - task.ti_execution_date > Config.queue_wait_threshold:
                urgent_task_idx = idx
            else:
                break

        self.log.info('Query result: %s', ", ".join(
            map(lambda res: str(res), result)))
        if len(result) < self.max_requests and urgent_task_idx is None and context['ti'].is_eligible_to_retry():
            return False
        elif result:
            self.log.info('Found %d tasks, with %d urgent tasks', len(result), (urgent_task_idx or 0) + 1)
            self.xcom_push(context, self.xcom_tasks_key, result)
            return True
        else:
            return False
