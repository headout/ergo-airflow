from typing import List, Tuple
import json
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.models import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask
from ergo.exceptions import ErgoFailedResultException
from airflow.utils import timezone
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from sqlalchemy.orm import joinedload
from ergo import JobResultStatus
from airflow.triggers.temporal import TimeDeltaTrigger


class SQSResultCollector(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            pusher_task_id: str,
            queue_url: str,
            aws_conn_id: str,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.queue_url = queue_url
        self.pusher_task_id = pusher_task_id
        self.aws_conn_id = aws_conn_id

    def _get_ergo_task(self, ti_dict, session=None):
        return (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=self.pusher_task_id, ti_dag_id=ti_dict['dag_id'], ti_run_id=ti_dict['run_id'])
        ).one()

    def mark_success(self,jobId, result, session=None):
        job = (
            session.query(ErgoJob)
            .options(joinedload('task'))
            .filter(ErgoJob.id == jobId)
            .one()
        )
        job.result_code = result['metadata']['status']
        job.result_data = json.dumps(result['data']) if 'data' in result else None
        job._error_msg = result['metadata'].get('error', None)
        if job._error_msg is not None:
            try:
                job._error_msg = json.dumps(job._error_msg)
            except Exception:
                job._error_msg = str(job._error_msg)
        job._error_msg = job._error_msg.encode(
            'latin1', 'ignore').decode()
        job.response_at = timezone.utcnow()
        task = job.task
        task.state = JobResultStatus.task_state(job.result_code)


    def read_sqs_message(self, context, job_id, session=None, event=None):
        sqs_client = sqs_hook.get_client_type('sqs', region_name='us-east-1')
        while True:
            try:
                # Receive messages with the specified message group ID
                response = sqs_client.receive_message(
                    QueueUrl=self.queue_url,
                    MessageAttributeNames=['All'],
                    MaxNumberOfMessages=10,
                    MessageGroupId=job_id,
                    WaitTimeSeconds=10  # Long polling for up to 10 seconds
                )

                # Process received messages
                messages = response.get('Messages', [])
                print(messages)
                if len(messages) == 0:
                    self.defer(
                        trigger=TimeDeltaTrigger(timedelta(seconds=15)),
                        method_name="read_sqs_message",
                        kwargs={
                            "job_id": job_id,
                            "session": session,
                        }
                    )
                else:
                    for message in messages:
                        # Extract the message group ID
                        job_id = message['MessageGroupId']

                        print(f"Received message: {message['Body']}, jobId: {job_id}")

                        # Delete the message from the queue
                        receipt_handle = message['ReceiptHandle']
                        self.mark_success(job_id,message['Body'],session)
                        sqs_client.delete_message(
                            QueueUrl=self.queue_url,
                            ReceiptHandle=receipt_handle
                        )
                    return True
            except Exception as e:
                print(f"An error occurred: {str(e)}")
                return False
        return False

    @provide_session
    def execute(self, context, session=None, event=None):
        ti_dict = context.get('ti_dict', dict())
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
        task = self._get_ergo_task(ti_dict, session=session)
        job = task.job
        job_id = job.id
        if self.read_sqs_message(job_id,self,context,job_id,session=None,event=None):
            raise ErgoFailedResultException(400, "Cron execution failed due to unknown reason")
        elif task.state == State.FAILED:
            if job is not None:
                self.log.info('Job - (%s)' + (f'responded back at {job.response_at}' if job.response_at else ''), str(job))
                raise ErgoFailedResultException(job.result_code, job.error_msg)
            else:
                raise ErgoFailedResultException(400, "Cron execution failed due to unknown reason")
        return