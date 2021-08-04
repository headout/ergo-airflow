import json

from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from sqlalchemy.orm import joinedload

from ergo import JobResultStatus
from ergo.models import ErgoJob, ErgoTask


class JobResultFromMessagesOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        sqs_sensor_task_id: str,
        xcom_message_key: str = 'messages',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sensor_task_id = sqs_sensor_task_id
        self.xcom_msg_key = xcom_message_key

    @provide_session
    def execute(self, context, session=None):
        ti = context['ti']
        messages = self.xcom_pull(
            context, self.sensor_task_id, key=self.xcom_msg_key)['Messages']
        self.log.info('Got %d messages...', len(messages))
        results = [
            json.loads(msg['Body'])
            for msg in messages
        ]
        results.sort(key=lambda res: res['jobId'])
        jobs = (
            session.query(ErgoJob)
            .options(joinedload('task'))
            .filter(ErgoJob.id.in_([res['jobId'] for res in results]))
            .order_by(ErgoJob.id)
        )
        jobs = list(jobs)
        for result, job in zip(results, jobs):
            self.log.info('Processing result %s', str(result))
            job.result_code = result['metadata']['status']
            job.result_data = json.dumps(result['data']) if 'data' in result else None
            job._error_msg = result['metadata'].get('error', None)
            if job._error_msg is not None:
                # ensure the saved error message is decoded into latin-1
                # so it is mysql DB compliant text
                try:
                    job._error_msg = json.dumps(job._error_msg)
                except Exception:
                    job._error_msg = str(job._error_msg)
                job._error_msg = job._error_msg.encode(
                    'latin1', 'ignore').decode()
            job.response_at = timezone.utcnow()
            task = job.task
            task.state = JobResultStatus.task_state(job.result_code)
        session.commit()
