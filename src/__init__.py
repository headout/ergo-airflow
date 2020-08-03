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
