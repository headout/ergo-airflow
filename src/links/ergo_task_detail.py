from airflow.models.baseoperator import BaseOperatorLink
from flask import url_for


class ErgoTaskDetailLink(BaseOperatorLink):
    """
    Shows details of Ergo Task
    """
    name = 'Ergo'

    def get_link(self, operator, dttm):
        return url_for(
            'ErgoView.task_detail',
            ti_task_id=operator.task_id,
            ti_dag_id=operator.dag_id,
            ti_execution_date=dttm
        )
