import logging
from os import path

from airflow.models.dagbag import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin

from ergo.migrations.utils import initdb
from ergo.operators.task_producer import ErgoTaskProducerOperator
from ergo.sensors.job_result_sensor import ErgoJobResultSensor


class ErgoPlugin(AirflowPlugin, LoggingMixin):
    name = 'ergo'
    operators = (ErgoTaskProducerOperator,)
    sensors = (ErgoJobResultSensor,)
    log = logging.root.getChild(f'{__name__}.{"ErgoPlugin"}')
