from airflow.exceptions import AirflowFailException


class ErgoFailedResultException(AirflowFailException):
    def __init__(self, status_code, error_msg):
        self.status_code = status_code
        self.error_msg = error_msg

    def __str__(self):
        return f'{self.status_code}: {self.error_msg}'
