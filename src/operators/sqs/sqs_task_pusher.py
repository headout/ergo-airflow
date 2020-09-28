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
        xcom_sqs_queue_url_key: str,
        aws_conn_id='aws_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_id_collector = task_id_collector
        self.xcom_tasks_key = xcom_tasks_key
        self.xcom_sqs_queue_url_key = xcom_sqs_queue_url_key
        self.aws_conn_id = aws_conn_id

    @provide_session
    def execute(self, context, session=None):
        dag_id = context['ti'].dag_id
        tasks = self.xcom_pull(
            context, self.task_id_collector, dag_id, self.xcom_tasks_key)
        queue_url = self.xcom_pull(
            context, self.task_id_collector, dag_id, self.xcom_sqs_queue_url_key)
        sqs_client = SQSHook(aws_conn_id=self.aws_conn_id).get_conn()
        self.log.info('Trying to push %d messages on queue: %s',
                      len(tasks), queue_url)
        entries = [
            {
                'Id': str(task.id),
                'MessageBody': task.request_data,
                'MessageGroupId': task.task_id,
                'MessageDeduplicationId': str(task.id)
            }
            for task in tasks
        ]
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
        except Exception as e:
            self.log.exception(
                'SQS Send message API failed for "%s" queue!\nRequest Entries: %', queue_url, str(entries),
                exc_info=e
            )

            self.log.info("Setting the tasks up for reschedule!")
            self._set_task_states(
                [task.id for task in tasks],
                State.UP_FOR_RESCHEDULE,
                session=session
            )
            session.commit()

            raise

        success_resps = response.get('Successful', list())
        failed_resps = response.get('Failed', list())
        if success_resps:
            self.log.info(
                'Successfully pushed %d messages!', len(success_resps))
            self._set_task_states(
                [int(resp['Id']) for resp in success_resps],
                State.QUEUED,
                session=session
            )
            jobs = [
                ErgoJob(resp['MessageId'], int(resp['Id']))
                for resp in success_resps
            ]
            session.add_all(jobs)
        if failed_resps:
            self.log.error('Failed to push %d messages!', len(failed_resps))
            self._set_task_states(
                [int(resp['Id']) for resp in failed_resps],
                State.UP_FOR_RESCHEDULE,
                session=session
            )
        session.commit()

    @staticmethod
    def _set_task_states(task_ids, state, session=None):
        for task in session.query(ErgoTask).filter(ErgoTask.id.in_(task_ids)):
            task.state = state
