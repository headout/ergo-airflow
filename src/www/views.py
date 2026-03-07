import logging
from functools import wraps

import pendulum
from airflow.exceptions import DagRunNotFound
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
# airflow.www removed in Airflow 3.x
from ergo.models import ErgoTask
from flask import request
from flask_appbuilder import BaseView, expose, has_access
from sqlalchemy.orm import joinedload


def login_required(func):
    # In Airflow 2.x+, authentication is handled by FAB/security manager.
    # This decorator is a no-op passthrough; actual auth is via @has_access.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
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
        run_id = (
            session.query(DagRun.run_id)
            .filter_by(dag_id=dag_id, execution_date=execution_date)
            .scalar()
        )
        if not run_id:
            raise DagRunNotFound(
                f"DagRun for {self.dag_id!r} with date {execution_date} not found"
            ) from None
        task = (
            session.query(ErgoTask)
            .options(joinedload('job'))
            .filter_by(ti_task_id=task_id, ti_dag_id=dag_id, ti_run_id=run_id)
        ).one()
        job = task.job

        req_attrs = {}
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                req_attrs[attr_name] = str(attr)

        res_attrs = {}
        for attr_name in dir(job):
            if not attr_name.startswith('_'):
                attr = getattr(job, attr_name)
                res_attrs[attr_name] = str(attr)

        return self.render_template(
            'ergo/task_detail.html',
            task=task,
            job=job,
            execution_date=execution_date.isoformat(),
            req_attrs=req_attrs,
            res_attrs=res_attrs,
            state_token=task.state
        )
