from typing import List, Tuple

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.models import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask
for ergo.config import Config


class SqsTaskQueuerOperator(BaseOperator):
    filter_ergo_task = ErgoTask.state.in_([State.SCHEDULED, State.UP_FOR_RESCHEDULE])

    @apply_defaults
    def __init__(
            self,
            max_requests: int,
            use_row_lock=False,
            target_service: string,
            aws_conn_id='aws_default',
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.max_requests = max_requests
        self.use_row_lock = use_row_lock
        self.aws_conn_id = aws_conn_id
        self.target_service = target_service

    @provide_session
    def execute(self, context, session=None):
        dag_id = context['ti'].dag_id
        if self.category == 'selenium':
            queue_url = config.selenium_sqs_request_queue_url
        elif self.target_service == 'calipso':
            queue_url = config.calipso_sqs_request_queue_url
        else:
            queue_url =  config.aries_sqs_request_queue_url
        self.log.info('Queue URL: %s', queue_url)

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
            success_resps = response.get('Successful', list())
            failed_resps = response.get('Failed', list())
        except Exception as e:
            self.log.exception(
                'SQS Send message API failed for "%s" queue!\nRequest Entries: %', queue_url, str(
                    entries),
                exc_info=e
            )
            success_resps = list()
            failed_resps = list(entries)
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
    def create_Task(self, context, session=None):
        ti = context['ti']
        if self.ergo_task_callable is not None:
            result = self.ergo_task_callable()
            if isinstance(result, (tuple, list)):
                task_id, req_data = result
            else:
                task_id = result
                req_data = ''
        else:
            task_id, req_data = self.ergo_task_id, self.ergo_task_data
        if req_data is None:
            req_data = ''
        if not isinstance(req_data, str):
            req_data = json.dumps(req_data)
        self.log.info("Adding task '%s' with data: %s", task_id, req_data)
        task = ErgoTask(task_id, ti, self.ergo_task_sqs_queue_url, req_data)
        session.add(task)
        session.commit()
        self.log.info("Commited task '%s' to %s", str(task), task.state)

