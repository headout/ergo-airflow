import logging
from os import path

from airflow.models.dagbag import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from flask import Blueprint

from ergo.migrations.utils import initdb
from ergo.operators.task_producer import ErgoTaskProducerOperator
from ergo.sensors.job_result_sensor import ErgoJobResultSensor
from ergo.www.views import ErgoView

ab_ergo_view = ErgoView()
ab_ergo_package = {
    'name': 'Ergo Tasks',
    'category': 'Ergo',
    'view': ab_ergo_view
}

ergo_bp = Blueprint(
    "ergo_bp",
    __name__,
    template_folder='www/templates',
    static_folder='www/static',
    static_url_path='/static/ergo'
)

class ErgoPlugin(AirflowPlugin, LoggingMixin):
    name = 'ergo'
    operators = (ErgoTaskProducerOperator,)
    sensors = (ErgoJobResultSensor,)
    appbuilder_views = (ab_ergo_package,)
    flask_blueprints = (ergo_bp,)

    log = logging.root.getChild(f'{__name__}.{"ErgoPlugin"}')
