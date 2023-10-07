import json
from typing import Union, List, Tuple
from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.models import BaseOperator
from ergo.links.ergo_task_detail import ErgoTaskDetailLink
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from ergo.models import ErgoJob, ErgoTask
from ergo.config import Config
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload


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
            aws_conn_id: str = "aws_default",
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.ergo_task_callable = ergo_task_callable
        self.ergo_task_id = ergo_task_id
        self.ergo_task_data = ergo_task_data
        self.ergo_task_sqs_queue_url = ergo_task_sqs_queue_url or Config.sqs_request_queue_url
        if not (ergo_task_id or ergo_task_callable):
            raise ValueError(
                'Provide either static ergo_task_id or callable to get task_id and request_data')


    def create_job(self, task, session=None):
        self.log.info(f"Creating job for task: {task}")
        success_resp, failed_resp = self._send_to_sqs(self.ergo_task_sqs_queue_url, task)
        if success_resp:
            self.log.info('Successfully pushed SQS task request message')
            self.log.info(success_resp)
            ids_for_success = []
            for resp in success_resp:
                if resp['Id'] is not None:
                    ids_for_success.append(int(resp['Id']))
                    job = ErgoJob(resp['MessageId'], int(resp['Id']))
                    session.add(job)

        if failed_resp:
            self.log.info('Failed pushing SQS task request message')
            self.log.info(failed_resp)
            task.state = State.UP_FOR_RESCHEDULE
            self.log.info("Task ID: %s, State: %s, Request Data: %s", task.id, task.state, task.request_data)
        session.commit()
        return job

    def create_task(self, context, task_id, req_data, session=None):
        ti = context['ti']
        ti_dict = context.get('ti_dict', dict())
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
        self.log.info(f"Creating task for {task_id}")
        task = ErgoTask(task_id, ti, self.ergo_task_sqs_queue_url, req_data)
        task.state = State.QUEUED
        session.add(task)
        session.commit()
        return task

    def _get_ergo_task(self, ti_task_id, ti_dict, session=None):
        self.log.info(f"Checking if task was created earlier with the following details {ti_task_id} {ti_dict}")
        try:
            return(
                session.query(ErgoTask)
                .options(joinedload('job'))
                .filter_by(ti_task_id=ti_task_id, ti_dag_id=ti_dict['dag_id'], ti_run_id=ti_dict['run_id'])
            ).one()
        except NoResultFound:
            self.log.info("Task was not created earlier")
            # Handle the case where no tasks match the criteria
            return None

    @provide_session
    def execute(self, context, session=None):
        ti = context['ti']
        ti_dict = context.get('ti_dict', dict())
        if not ti_dict:
            ti = context['ti']
            ti_dict['dag_id'] = ti.dag_id
            ti_dict['run_id'] = ti.run_id
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

        prev_task = self._get_ergo_task(ti.task_id, ti_dict, session)
        if prev_task is not None:
            prev_job = prev_task.job
            self.log.info(f"Ergo task has already been created. : {prev_task}")
            if prev_job is not None:
                self.log.info(f"Ergo job was already created : {prev_job}")
            else:
                self.log.info("Job was not created")
                self.create_job(prev_task, session)
        else:
            task = self.create_task(task_id, context, req_data, session)
            job = self.create_job(task, session)

        session.commit()

    def _send_to_sqs(self, queue_url, task) -> Tuple[List, List]:
        sqs_client = SQSHook(aws_conn_id=self.aws_conn_id).get_conn()
        self.log.info('Trying to push a message on queue: %s\n', queue_url)
        self.log.info('Request task: %s', task.task_id)
        entries = [
            {
                'Id': str(task.id),
                'MessageBody': task.request_data,
                'MessageGroupId': task.task_id,
                'MessageDeduplicationId': str(task.id)
            }
        ]
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            success_resp = response.get('Successful', list())
            failed_resp = response.get('Failed', list())
        except Exception as e:
            self.log.exception(
                'SQS Send message API failed for "%s" queue!\nRequest Entries: %', queue_url, str(
                    entries),
                exc_info=e
            )
            success_resp = list()
            failed_resp = list(entries)
            self.log.info(success_resp)
            self.log.info(failed_resp)

        return success_resp, failed_resp
