import os

from airflow import settings
from airflow.hooks.base_hook import CONN_ENV_PREFIX
from airflow.hooks.mysql_hook import MySqlHook

MYSQL_CONN_ID = "ergo_sql_alchemy_conn"


def get_mysql_hook():
    os.environ[CONN_ENV_PREFIX + MYSQL_CONN_ID.upper()] = settings.SQL_ALCHEMY_CONN
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID)


def run_sql(sql, ignore_error=False):
    hook = get_mysql_hook()
    print ("sql:\n%s" % sql)
    try:
        res = hook.get_records(sql)
    except Exception as e:
        if not ignore_error:
            raise e
        res = None
    print (res)
    return res

def run_version_0_0_1():
    print("Running Ergo Migrations - v0.0.1")
    run_sql(
        """
        CREATE TABLE IF NOT EXISTS ergo_task (
            id INTEGER NOT NULL AUTO_INCREMENT, 
            task_id VARCHAR(128) NOT NULL, 
            request_data TEXT, 
            state VARCHAR(20) NOT NULL, 
            created_at DATETIME NOT NULL, 
            updated_at DATETIME NOT NULL, 
            ti_task_id VARCHAR(250) NOT NULL, 
            ti_dag_id VARCHAR(250) NOT NULL, 
            ti_execution_date TIMESTAMP(6) NOT NULL, 
            PRIMARY KEY (id), 
            FOREIGN KEY(ti_task_id, ti_dag_id, ti_execution_date) REFERENCES task_instance (task_id, dag_id, execution_date) ON DELETE CASCADE
        );
        """
    )

    run_sql(
        """
        CREATE INDEX ix_ergo_task_created_at ON ergo_task (created_at);
        CREATE INDEX ix_ergo_task_updated_at ON ergo_task (updated_at);
        """ ,
        ignore_error=True
    )

    run_sql(
        """
        CREATE TABLE IF NOT EXISTS ergo_job (
            id VARCHAR(128) NOT NULL, 
            task_id INTEGER NOT NULL, 
            PRIMARY KEY (id), 
            FOREIGN KEY(task_id) REFERENCES ergo_task (id) ON DELETE CASCADE
        );
        """
    )

    run_sql(
        """
        CREATE UNIQUE INDEX ix_ergo_job_task_id ON ergo_job (task_id);
        """,
        ignore_error=True
    )

def run_version_0_0_2():
    print("Running Ergo Migrations - v0.0.2")
    run_sql("ALTER TABLE ergo_job ADD COLUMN error_msg TEXT;", ignore_error=True)
    run_sql("ALTER TABLE ergo_job ADD COLUMN result_code INTEGER NOT NULL;", ignore_error=True)
    run_sql("ALTER TABLE ergo_job ADD COLUMN result_data TEXT;", ignore_error=True)

def run_version_0_0_3():
    print("Running Ergo Migrations - v0.0.3")
    run_sql("ALTER TABLE ergo_job ADD COLUMN created_at DATETIME NOT NULL;", ignore_error=True)
    run_sql("ALTER TABLE ergo_job ADD COLUMN response_at DATETIME;", ignore_error=True)
    run_sql("CREATE INDEX ix_ergo_job_created_at ON ergo_job (created_at);", ignore_error=True)

def run_version_0_0_4():
    print("Running Ergo Migrations - v0.0.4")
    run_sql("ALTER TABLE ergo_task ADD CONSTRAINT ix_unique_task_instance UNIQUE (ti_task_id, ti_dag_id, ti_execution_date);", ignore_error=True)

def main():
    run_version_0_0_1()
    run_version_0_0_2()
    run_version_0_0_3()
    run_version_0_0_4()

if __name__ == "__main__":
    main()
