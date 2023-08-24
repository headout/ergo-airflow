from typing import List, Tuple

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.models import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask


class SqsTaskPusherOperator(BaseOperator):
    filter_ergo_task = ErgoTask.state.in_([State.SCHEDULED, State.UP_FOR_RESCHEDULE])

    @apply_defaults
    def __init__(
        self,
        task_id_collector: str,
        max_requests: int,
        xcom_sqs_queue_url_key: str,
        use_row_lock=False,
        aws_conn_id='aws_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.task_id_collector = task_id_collector
        self.max_requests = 30
        self.xcom_sqs_queue_url_key = xcom_sqs_queue_url_key
        self.use_row_lock = use_row_lock
        self.aws_conn_id = aws_conn_id

    @provide_session
    def execute(self, context, session=None):
        dag_id = context['ti'].dag_id
        queue_url = self.xcom_pull(
            context, self.task_id_collector, dag_id, self.xcom_sqs_queue_url_key)
        self.log.info('Queue URL: %s', queue_url)
        tasks = self.collect_tasks(queue_url, session)

        success_resps, failed_resps = self._send_to_sqs(queue_url, tasks)
        if success_resps:
            self.log.info(
                'Successfully pushed %d messages!', len(success_resps))

            self._set_task_states(
                tasks,
                [int(resp['Id']) for resp in success_resps],
                State.QUEUED
            )
            jobs = [
                ErgoJob(resp['MessageId'], int(resp['Id']))
                for resp in success_resps
            ]
            session.add_all(jobs)
        if failed_resps:
            self.log.error('Failed to push %d messages!', len(failed_resps))

            self.log.info("Setting the tasks up for reschedule!")
            self._set_task_states(
                tasks,
                [int(resp['Id']) for resp in failed_resps],
                State.UP_FOR_RESCHEDULE,
            )

        # Commit at last
        session.commit()

    def _send_to_sqs(self, queue_url, query) -> Tuple[List, List]:
        tasks = list(query)
        sqs_client = SQSHook(aws_conn_id=self.aws_conn_id).get_conn()
        self.log.info('Trying to push %d messages on queue: %s\n',
                      len(tasks), queue_url)
        self.log.info('Request tasks: ' + '\n'.join([str(task) for task in tasks]))

        entries_batches = [tasks[i:i+10] for i in range(0, len(tasks), 10)]

        success_resps = []
        failed_resps = []

        for batch in entries_batches:
            entries = [
                {
                    'Id': str(task.id),
                    'MessageBody': task.request_data,
                    'MessageGroupId': task.task_id,
                    'MessageDeduplicationId': str(task.id)
                }
                for task in batch
            ]
            try:
                response = sqs_client.send_message_batch(
                    QueueUrl=queue_url,
                    Entries=entries
                )
                success_resps.extend(response.get('Successful', []))
                failed_resps.extend(response.get('Failed', []))
            except Exception as e:
                self.log.exception(
                    'SQS Send message API failed for "%s" queue!\nRequest Entries: %s', queue_url, str(
                        entries),
                    exc_info=e
                )
                failed_resps.extend(entries)

        return (success_resps, failed_resps)

    @staticmethod
    def _set_task_states(tasks, task_ids, state):
        for task in tasks:
            if task.id in task_ids:
                task.state = state

    def collect_tasks(self, queue_url, session):
        query = (
            session.query(ErgoTask)
            .filter(self.filter_ergo_task, ErgoTask.queue_url == queue_url)
            .order_by(ErgoTask.created_at)
        ).limit(self.max_requests)
        if self.use_row_lock:
            return query.with_for_update(of=ErgoTask, skip_locked=True)
        return query
