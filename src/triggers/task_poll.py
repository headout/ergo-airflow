import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from airflow.triggers.base import BaseTrigger, TriggerEvent
from ergo.db import provide_session
from airflow.utils.state import State
from ergo.exceptions import ErgoFailedResultException
from ergo.models import ErgoJob, ErgoTask
from sqlalchemy.orm import joinedload

os.environ['PYTHONASYNCIODEBUG'] = '1'


class TaskPollTrigger(BaseTrigger):

    def __init__(
            self,
            ti_dict,
            pusher_task_id: str,
            wait_for_state=list(State.finished),
            poke_interval: float = 20,
    ):
        super().__init__()
        self.ti_dict = ti_dict
        self.pusher_task_id = pusher_task_id
        self.wait_for_state = wait_for_state
        self.poke_interval = poke_interval

    def serialize(self):
        return (
            "ergo.triggers.task_poll.TaskPollTrigger",
            {
                "ti_dict": self.ti_dict,
                "pusher_task_id": self.pusher_task_id,
                "wait_for_state": self.wait_for_state,
                "poke_interval": self.poke_interval,
            },
        )

    async def _get_ergo_task(self, session=None):
        return (
            session.query(ErgoTask)
            .options(joinedload(ErgoTask.job))
            .filter_by(ti_task_id=self.pusher_task_id, ti_dag_id=self.ti_dict['dag_id'], ti_run_id=self.ti_dict['run_id'])
        ).one()


    @provide_session
    async def _check_task_status(self, session=None):
        task = await self._get_ergo_task(session=session)
        job = task.job

        if task.state not in self.wait_for_state:
            self.log.info('Received task - %s... STATE: %s', str(task), task.state)
            job = task.job
            if job is not None:
                self.log.info(
                    'Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
            else:
                self.log.info('Waiting for task "%s" to be queued...', str(task))
                self.log.info('Waiting for task "%s" to reach state %s...', str(task), self.wait_for_state)
            return False

        self.log.info('Task - %s reached state %s', str(task), task.state)
        return True

    async def run(self):
        while True:
            task_completed = await self._check_task_status()
            if task_completed:
                yield TriggerEvent(True)
            await asyncio.sleep(self.poke_interval)
