from airflow.sdk import BaseOperatorLink


class ErgoTaskDetailLink(BaseOperatorLink):
    """
    Shows details of Ergo Task
    """
    name = 'Ergo'

    def get_link(self, operator, *, ti_key, **kwargs):
        # In Airflow 3, get_link receives ti_key instead of dttm
        return f'/ergo/task_detail?ti_task_id={ti_key.task_id}&ti_dag_id={ti_key.dag_id}&ti_run_id={ti_key.run_id}'
