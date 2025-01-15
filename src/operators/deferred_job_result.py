from datetime import datetime, timedelta
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from ergo.exceptions import ErgoFailedResultException
from ergo.models import ErgoJob, ErgoTask
from ergo.triggers.task_poll import TaskPollTrigger
from sqlalchemy.orm import joinedload
from airflow.triggers.temporal import TimeDeltaTrigger


class ErgoDeferredJobResult(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            pusher_task_id: str,
            wait_for_state=list(State.finished),
            task_poll_trigger=True,
            *args,
            **kwargs
    ):
        # HACK: In Airflow V2, for smart sensor, added 'ti' info in context but
        # it shouldn't be passed to super constructor
        kwargs.pop('ti_dict', None)
        super().__init__(*args, **kwargs)
        self.pusher_task_id = pusher_task_id
        self.task_poll_trigger = task_poll_trigger
        if not isinstance(wait_for_state, (list, tuple)):
            wait_for_state = (wait_for_state,)
        self.wait_for_state = list(State.finished)
        if self.wait_for_state != wait_for_state:
            self.wait_for_state.extend(wait_for_state)


    def _get_ergo_task(self, ti_dict, session=None):
        return (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=self.pusher_task_id, ti_dag_id=ti_dict['dag_id'], ti_run_id=ti_dict['run_id'])
        ).one()

    @provide_session
    def _get_task_status(self, ti_dict, session=None):
        task = self._get_ergo_task(ti_dict, session=session)
        job = task.job

        # Fallback from triggerer to worker polling if triggerer fails
        while task.state not in self.wait_for_state:
            task = self._get_ergo_task(ti_dict, session=session)
            self.log.info('Received task - %s... STATE: %s', str(task), task.state)
            job = task.job
            if job is not None:
                self.log.info(
                    'Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
            else:
                self.log.info('Waiting for task "%s" to be queued...', str(task))
                self.log.info('Waiting for task "%s" to reach state %s...', str(task), self.wait_for_state)

            if self.task_poll_trigger:
                self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=20)), method_name="execute_complete")

        if task.state == State.FAILED:
            if job is not None:
                self.log.info('Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
                raise ErgoFailedResultException(job.result_code, job.error_msg)
            else:
                raise ErgoFailedResultException(400, "Cron execution failed due to unknown reason")

        self.log.info('Task - %s reached state %s', str(task), task.state)


    def execute(self, context, event=None):
        ti_dict = context.get('ti_dict', dict())
        self.xcom_push(context, "ergo_task_state", "started")
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
        if self.task_poll_trigger:
            self.log.info('Polling DB task status using task poll trigger. Check triggerer logs to get more state info')
            self.defer(trigger=TaskPollTrigger(ti_dict, self.pusher_task_id, self.wait_for_state, 20), method_name="execute_complete")
        else:
            self.log.info('Polling DB task status using airflow worker and timedelta trigger')
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=20)), method_name="execute_complete")
        return

    def execute_complete(self, context, event=None):
        ti_dict = context.get('ti_dict', dict())
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
        self.log.info("Control transferred from trigger to worker for remaining operator execution")
        self._get_task_status(ti_dict)
        self.xcom_push(context, "ergo_task_state", "completed")
        return


