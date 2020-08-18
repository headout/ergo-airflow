import json
from typing import Union

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.operators import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults

from ergo.config import Config
from ergo.models import ErgoTask


class ErgoTaskProducerOperator(BaseOperator):
    template_fields = ['ergo_task_id', 'ergo_task_data']

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
        session.commit()
        self.log.info("Commited task '%s' to %s", str(task), task.state)
