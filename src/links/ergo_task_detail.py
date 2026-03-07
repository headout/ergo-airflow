from airflow.sdk import BaseOperatorLink


class ErgoTaskDetailLink(BaseOperatorLink):
    """
    Shows details of Ergo Task
    """
    name = 'Ergo'

    def get_link(self, operator, dttm):
        # In Airflow 3, the www module is gone; return a simple URL path
        return f'/ergo/task_detail?ti_task_id={operator.task_id}&ti_dag_id={operator.dag_id}&ti_execution_date={dttm}'
