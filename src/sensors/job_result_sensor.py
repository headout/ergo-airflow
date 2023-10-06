from datetime import datetime

from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.exceptions import ErgoFailedResultException
from ergo.models import ErgoJob, ErgoTask
from sqlalchemy.orm import joinedload


class ErgoJobResultSensor(BaseSensorOperator):
    poke_context_fields = ('pusher_task_id', 'wait_for_state')

    @apply_defaults
    def __init__(
        self,
        pusher_task_id: str,
        wait_for_state=list(State.finished),
        *args,
        **kwargs
    ):
        # HACK: In Airflow V2, for smart sensor, added 'ti' info in context but
        # it shouldn't be passed to super constructor
        kwargs.pop('ti_dict', None)
        super().__init__(*args, **kwargs)
        self.pusher_task_id = pusher_task_id
        if not isinstance(wait_for_state, (list, tuple)):
            wait_for_state = (wait_for_state,)
        self.wait_for_state = list(State.finished)
        if self.wait_for_state != wait_for_state:
            self.wait_for_state.extend(wait_for_state)

    @provide_session
    def _get_ergo_task(self, ti_dict, session=None):
        return (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=self.pusher_task_id, ti_dag_id=ti_dict['dag_id'], ti_run_id=ti_dict['run_id'])
        ).one()

    def get_poke_context(self, context):
        result = super().get_poke_context(context)
        result['ti_dict'] = {
            'dag_id': context['ti'].dag_id,
            'run_id': context['ti'].run_id
        }
        return result

    def poke(self, context):
        ti_dict = context.get('ti_dict', dict())
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
        task = self._get_ergo_task(ti_dict)
        self.log.info('Received task - %s... STATE: %s', str(task), task.state)
        job = task.job
        if job is not None:
            self.log.info(
                'Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
        else:
            self.log.info('Waiting for task "%s" to be queued...', str(task))
        if task.state == State.FAILED:
            raise ErgoFailedResultException(job.result_code, job.error_msg)
        return task.state in self.wait_for_state
