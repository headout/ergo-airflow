from datetime import datetime

from airflow.models import TaskInstance
from airflow.models.base import ID_LEN
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from sqlalchemy import (Column, ForeignKey, ForeignKeyConstraint, Index,
                        Integer, String, Text, UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload, relationship

from ergo import JobResultStatus

Base = declarative_base()


class ErgoTask(Base):
    __tablename__ = 'ergo_task'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(128), nullable=False)
    request_data = Column(Text, nullable=True)
    # state transitions: SCHEDULED -> QUEUED -> RUNNING -> SUCCESS
    state = Column(String(20), default=State.SCHEDULED,
                   nullable=False)  # enum{State}
    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )
    updated_at = Column(
        UtcDateTime, index=True, nullable=False,
        default=timezone.utcnow, onupdate=timezone.utcnow
    )
    ti_task_id = Column(String(ID_LEN), nullable=False)
    ti_dag_id = Column(String(ID_LEN), nullable=False)
    ti_execution_date = Column(UtcDateTime, nullable=False)

    job = relationship('ErgoJob', back_populates='task', uselist=False)
    # task_instance = relationship(TaskInstance, back_populates='ergo_task')

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_task_id, ti_dag_id, ti_execution_date),
            (TaskInstance.task_id, TaskInstance.dag_id, TaskInstance.execution_date),
            ondelete='CASCADE'
        ),
        UniqueConstraint(
            ti_task_id, ti_dag_id, ti_execution_date, name='ix_unique_task_instance'
        )
    )

    def __str__(self):
        return f'#{self.id}: {self.task_id}'

    def __init__(self, task_id, ti, request_data=''):
        self.task_id = task_id
        self.ti_task_id = ti.task_id
        self.ti_dag_id = ti.dag_id
        self.ti_execution_date = ti.execution_date
        self.request_data = request_data


class ErgoJob(Base):
    __tablename__ = 'ergo_job'

    id = Column(String(128), primary_key=True)
    task_id = Column(
        ForeignKey("ergo_task.id", ondelete='CASCADE'),
        nullable=False,
        index=True,
        unique=True
    )
    result_data = Column(Text, nullable=True)
    result_code = Column(Integer, default=JobResultStatus.NONE,
                         nullable=False)  # enum{JobResultStatus}
    error_msg = Column(Text, nullable=True)

    created_at = Column(
        UtcDateTime, index=True, default=timezone.utcnow, nullable=False
    )
    response_at = Column(UtcDateTime, nullable=True)

    task = relationship('ErgoTask', back_populates='job')

    def __str__(self):
        return f'code={self.result_code}, msg={self.error_msg}, data={self.result_data}'

    def __init__(self, job_id, task_id, result=None):
        self.id = job_id
        self.task_id = task_id
        if result is not None:
            # TODO: Parse result and fill
            pass

#### TaskInstance extensions ####


@provide_session
def _get_ergo_task(ti, session=None):
    return (
        session.query(ErgoTask)
        .options(joinedload('job'))
        .filter_by(ti_task_id=ti.task_id, ti_dag_id=ti.dag_id, ti_execution_date=ti.execution_date)
    ).one_or_none()


def _get_ergo_job(ti):
    return getattr(_get_ergo_task(ti), 'job', None)


TaskInstance.ergo_task = property(lambda self: str(_get_ergo_task(self)))
TaskInstance.ergo_task_status = property(
    lambda self: getattr(_get_ergo_task(self), 'state', None))
TaskInstance.ergo_job_id = property(
    lambda self: str(getattr(_get_ergo_job(self), 'id', None)))
TaskInstance.ergo_job_status = property(lambda self: str(_get_ergo_job(self)))
TaskInstance.ergo_job_response_at = property(
    lambda self: getattr(_get_ergo_job(self), 'response_at', None))
