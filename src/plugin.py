import logging

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from ergo.links.ergo_task_detail import ErgoTaskDetailLink
from ergo.migrations.utils import initdb
from ergo.operators.task_producer import ErgoTaskProducerOperator
from ergo.operators.ergo_task_producer import ErgoTaskQueuerOperator
from ergo.sensors.job_result_sensor import ErgoJobResultSensor


class ErgoPlugin(AirflowPlugin, LoggingMixin):
    name = 'ergo'
    operators = (ErgoTaskProducerOperator, ErgoTaskQueuerOperator,)
    sensors = (ErgoJobResultSensor,)
    # FAB views and Flask blueprints removed - not supported in Airflow 3.x FastAPI webserver
    operator_extra_links = (ErgoTaskDetailLink(),)

    log = logging.root.getChild(f'{__name__}.{"ErgoPlugin"}')
