import json
from typing import Union,List, Tuple
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.models import BaseOperator
from ergo.links.ergo_task_detail import ErgoTaskDetailLink
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask
for ergo.config import Config


class ErgoTaskQueuerOperator(BaseOperator):
    template_fields = ['ergo_task_id', 'ergo_task_data']

    operator_extra_links = (ErgoTaskDetailLink(),)

    @apply_defaults
    def __init__(
            self,
            ergo_task_callable: callable = None,
            ergo_task_id: str = '',
            ergo_task_data: Union[dict, str] = {},
            ergo_task_sqs_queue_url: str = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ergo_task_callable = ergo_task_callable
        self.ergo_task_id = ergo_task_id
        self.ergo_task_data = ergo_task_data
        self.ergo_task_sqs_queue_url = ergo_task_sqs_queue_url or Config.sqs_request_queue_url
        if not (ergo_task_id or ergo_task_callable):
            raise ValueError(
                'Provide either static ergo_task_id or callable to get task_id and request_data')

    @provide_session
    def execute(self, context, session=None):
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
        success_resps, failed_resps = self._send_to_sqs(ergo_task_sqs_queue_url, task)
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
        session.commit()
        self.log.info("Commited task '%s' to %s", str(task), task.state)

    def _send_to_sqs(self, queue_url, tasks) -> Tuple[List, List]:
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

