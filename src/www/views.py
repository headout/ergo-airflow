import logging
from functools import wraps

import airflow
import pendulum
from airflow.utils.db import provide_session
from flask import request
from flask_appbuilder import BaseView, expose, has_access
from sqlalchemy.orm import joinedload

from ergo.models import ErgoTask


def login_required(func):
    # when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper


class ErgoView(BaseView):
    log = logging.root.getChild(f'{__name__}.{"ErgoView"}')

    route_base = '/ergo'

    @expose('/')
    def list(self):
        return 'ergo'

    @expose('/task_detail')
    @login_required
    @has_access
    @provide_session
    def task_detail(self, session=None):
        dag_id = request.args.get('ti_dag_id')
        task_id = request.args.get('ti_task_id')
        execution_date = request.args.get('ti_execution_date')
        if execution_date:
            execution_date = pendulum.parse(execution_date)
        task = (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=task_id, ti_dag_id=dag_id, ti_execution_date=execution_date)
        ).one()
        return self.render_template(
            'ergo/task_detail.html',
            task=task,
            job=task.job
        )
