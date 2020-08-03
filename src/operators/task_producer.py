import json

from airflow.contrib.hooks.aws_sqs_hook import SQSHook
from airflow.operators import BaseOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults

from ergo.models import ErgoTask


class ErgoTaskProducerOperator(BaseOperator):
    template_fields = ['ergo_task_id', 'ergo_task_data']

    @apply_defaults
    def __init__(
        self,
        ergo_task_callable: callable = None,
        ergo_task_id: str = '',
        ergo_task_data: dict = {},
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ergo_task_callable = ergo_task_callable
        self.ergo_task_id = ergo_task_id
        self.ergo_task_data = ergo_task_data
        if not (ergo_task_id or ergo_task_callable):
            raise ValueError('Provide either static ergo_task_id or callable to get task_id and request_data')

    @provide_session
    def execute(self, context, session=None):
        ti = context['ti']
        if self.ergo_task_callable is not None:
            result = self.ergo_task_callable()
            if isinstance(result, (tuple, list)):
                task_id, req_data = result
            else:
                task_id = result
                req_data = {}
        else:
            task_id, req_data = self.ergo_task_id, self.ergo_task_data
        req_data =  json.dumps(req_data) if req_data is not None else ''
        self.log.info("Pushing task '%s' with data: %s", task_id, req_data)
        task = ErgoTask(task_id, ti, req_data)
        session.add(task)
        session.commit()
        self.log.info("Pushed task '%s' to %s", str(task), task.state)
