from functools import wraps

from airflow.utils import db
from airflow.utils.state import State

SECTION_NAME = "ergo"


class JobResultStatus(object):
    NONE = 0
    SUCCESS = 200

    @staticmethod
    def task_state(code):
        if code == JobResultStatus.SUCCESS:
            return State.SUCCESS
        elif code == JobResultStatus.NONE:
            return State.QUEUED
        else:
            return State.FAILED


def ergo_initdb(func):
    from ergo.migrations.utils import initdb

    prev_wrappers = getattr(func, '_wrappers', list())
    if SECTION_NAME in prev_wrappers:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        initdb()

    wrapper._wrappers = list(prev_wrappers) + list(SECTION_NAME)

    return wrapper


db.initdb = ergo_initdb(db.initdb)
