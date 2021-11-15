import json
import logging
from functools import cached_property

from airflow.models.base import ID_LEN
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from ergo import JobResultStatus
from sqlalchemy import (Column, ForeignKey, ForeignKeyConstraint, Integer,
                        String, Text, UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

logger = logging.getLogger(__name__)


class ErgoTask(Base):
    __tablename__ = 'ergo_task'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(128), nullable=False)
    request_data = Column(Text, nullable=True)
    queue_url = Column(String(256), nullable=False)
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
    ti_run_id = Column(String(ID_LEN), nullable=False)

    job = relationship('ErgoJob', back_populates='task', uselist=False)
    # task_instance = relationship(TaskInstance, back_populates='ergo_task')

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_task_id, ti_dag_id, ti_run_id),
            ('task_instance.task_id', 'task_instance.dag_id', 'task_instance.run_id'),
            ondelete='CASCADE'
        ),
        UniqueConstraint(
            ti_task_id, ti_dag_id, ti_run_id, name='ix_unique_task_instance'
        )
    )

    def __str__(self):
        return f'#{self.id}: {self.task_id}'

    def __init__(self, task_id, ti, queue_url, request_data=''):
        self.task_id = task_id
        self.ti_task_id = ti.task_id
        self.ti_dag_id = ti.dag_id
        self.ti_run_id = ti.run_id
        self.request_data = request_data
        self.queue_url = queue_url


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
    _error_msg = Column('error_msg', Text, nullable=True)

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

    @cached_property
    def _error_metadata(self):
        if not self._error_msg:
            return None
        raw_str = self._error_msg
        try:
            return json.loads(raw_str)
        except json.JSONDecodeError as e:
            if e.msg == 'Expecting property name enclosed in double quotes':
                logger.warning(
                    'DEPRECATED use of single quotes in error metadata.\nFixing the dumped string for backwards compatibility.')
                try:
                    return json.loads(raw_str.replace("\'", "\""))
                except Exception as e:
                    logger.exception(
                        'Failed parsing error message', exc_info=e)
                    return None
            logger.exception('Failed parsing error message', exc_info=e)
            return None
        except Exception as e:
            logger.exception('Failed parsing error message', exc_info=e)
            return None

    @cached_property
    def error_msg(self):
        return self._error_metadata['message'] if self._error_metadata else self._error_msg

    @cached_property
    def error_stacktrace(self):
        if not self._error_metadata:
            return None
        return self._error_metadata['traceback']
