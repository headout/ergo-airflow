from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.operators import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask


class SqsTaskPusherOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        task_id_collector: str,
        xcom_tasks_key: str,
        sqs_queue_url: str,
        aws_conn_id='aws_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_id_collector = task_id_collector
        self.xcom_tasks_key = xcom_tasks_key
        self.sqs_queue_url = sqs_queue_url
        self.aws_conn_id = aws_conn_id

    @provide_session
    def execute(self, context, session=None):
        dag_id = context['ti'].dag_id
        tasks = self.xcom_pull(context, self.task_id_collector, dag_id, self.xcom_tasks_key)
        sqs_client = SQSHook(aws_conn_id=self.aws_conn_id).get_conn()
        self.log.info('SqsTaskPusherOperator trying to push %d messages on queue: %s', len(tasks), self.sqs_queue_url)
        entries = [
            {
                'Id': str(task.id),
                'MessageBody': task.request_data,
                'MessageGroupId': task.task_id,
                'MessageDeduplicationId': str(task.id)
            }
            for task in tasks
        ]
        response = sqs_client.send_message_batch(
            QueueUrl=self.sqs_queue_url,
            Entries=entries
        )
        success_resps = response.get('Successful', list())
        failed_resps = response.get('Failed', list())
        if success_resps:
            self.log.info('SqsTaskPusherOperator successfully pushed %d messages!', len(success_resps))
            success_tasks = session.query(ErgoTask).filter(
                ErgoTask.id.in_([int(resp['Id']) for resp in success_resps])
            )
            for task in success_tasks:
                task.state = State.QUEUED
            jobs = [
                ErgoJob(resp['MessageId'], int(resp['Id']))
                for resp in success_resps
            ]
            session.add_all(jobs)
        if failed_resps:
            self.log.error('SqsTaskPusherOperator failed to push %d messages!', len(failed_resps))
            failed_tasks = session.query(ErgoTask).filter(
                ErgoTask.id.in_([int(resp['Id']) for resp in failed_resps])
            )
            for task in failed_tasks:
                task.state = State.UP_FOR_RETRY
        session.commit()
