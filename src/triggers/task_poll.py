import asyncio
from concurrent.futures import ThreadPoolExecutor
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.db import provide_session
from airflow.utils.state import State


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
            "eta.triggers.sql.TaskPollTrigger",
            {
                "ti_dict": self.ti_dict,
                "pusher_task_id": self.pusher_task_id,
                "wait_for_state": self.wait_for_state,
                "poke_interval": self.poke_interval,
            },
        )

    def _get_ergo_task(self, ti_dict, session=None):
        return (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=self.pusher_task_id, ti_dag_id=ti_dict['dag_id'], ti_run_id=ti_dict['run_id'])
        ).one()


    @provide_session
    def _check_task_status(self, session=None):
        task = self._get_ergo_task(ti_dict, session=session)
        job = task.job

        if task.state not in self.wait_for_state:
            task = self._get_ergo_task(ti_dict, session=session)
            self.log.info('Received task - %s... STATE: %s', str(task), task.state)
            job = task.job
            if job is not None:
                self.log.info(
                    'Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
            else:
                self.log.info('Waiting for task "%s" to be queued...', str(task))
                self.log.info('Waiting for task "%s" to reach state %s...', str(task), self.wait_for_state)
            return false

        self.log.info('Task - %s reached state %s', str(task), task.state)
        return true

    async def run(self):
        while True:
            with ThreadPoolExecutor(max_workers=1) as exe:
                future = exe.submit(self._check_task_status)
                task_completed = future.result()
                if task_completed:
                    yield TriggerEvent(True)
            await asyncio.sleep(self.poke_interval)
