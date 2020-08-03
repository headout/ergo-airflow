# Ergo - Task Offloader

## Installation

- Clone the repository.
- Link src/ directory as a new plugin in <AIRFLOW_HOME>/plugins.

## Usage

- **IMPORTANT!** Add a dummy DAG in your DAGs folder (<AIRFLOW_HOME>/dags) to load required Ergo DAGs. You can use [this script](sample/dags/dag_ergo.py).
- Enable two DAGS - `ergo_task_queuer` and `ergo_job_collector` in the Airflow UI.

## Configuration

Sample:

```ini
[ergo]
request_queue_url = $REQUEST_SQS_QUEUE
result_queue_url = $RESULT_SQS_QUEUE
```

Explanation:

- `request_queue_url` - SQS queue url used for ergo task requests, i.e. airflow is producer
- `result_queue_url` - SQS queue url used for ergo task results, i.e. airflow is consumer
- `max_task_requests` - Maximum number of ergo requests to batch before sending to SQS (default 10)
