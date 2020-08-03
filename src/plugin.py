import logging
from os import path

from airflow.models.dagbag import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin

from ergo.operators.task_producer import ErgoTaskProducerOperator
from ergo.sensors.job_result_sensor import ErgoJobResultSensor

DAG_FOLDER = path.join(path.dirname(__file__), 'dags')
TAG = 'ergo'

class ErgoPlugin(AirflowPlugin, LoggingMixin):
    name = 'ergo'
    operators = (ErgoTaskProducerOperator,)
    sensors = (ErgoJobResultSensor,)
    log = logging.root.getChild(f'{__name__}.{"ErgoPlugin"}')

    @classmethod
    def validate(cls):
        super().validate()
        # FIXME: Hack since on_load calls only for entrypoint plugins
        # cls.log.info('%s: Loading DAGS from %s...', TAG, DAG_FOLDER)
        # dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        # for dag_id, dag in dag_bag.dags.items():
        #     cls.log.info('%s: Found dag %s', TAG, dag_id)
        #     globals()[dag_id] = dag
