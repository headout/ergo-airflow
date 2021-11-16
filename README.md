# Ergo - Task Offloader

## Installation

- Clone the repository.
- Link src/ directory as a new plugin in <AIRFLOW_HOME>/plugins.

## Compatibility

Apache Airflow 2.0.1+

## Usage

- **IMPORTANT!** Add a dummy DAG in your DAGs folder (<AIRFLOW_HOME>/dags) to load required Ergo DAGs. You can use [this script](sample/dags/dag_ergo.py).
- Enable two DAGS - `ergo_task_queuer` and `ergo_job_collector` in the Airflow UI.
- An example DAG using Ergo to offload tasks can be [found here](sample/dags/example.py).

## Configuration

Sample:

```ini
[ergo]
# Max number of tasks to queue at one time via SQS. `TaskRequestBatchSensor` will either wait
# for urgent condition (defined by the threshold below) or if any target queue has more number
# of scheduled tasks than the value of `max_task_requests`.
# AWS Batch SQS API supports a maximum of 10 messages at a time.
# Default: 10
max_task_requests = 10

# Fallback SQS queue URL to send task requests to be processed. Messages are sent to this queue
# with `task_id` as the `MessageGroupId` and `request_data` as the `MessageBody`.
# In most production setups, there will be more than 1 queue to manage and these can
# be set per DAG via the `ergo_task_sqs_queue_url` argument of `ErgoTaskProducerOperator`.
# So the value set here in config is a fallback in case queue URL isn't provided to the operator.
# Required.
request_queue_url = $REQUEST_SQS_QUEUE

# SQS queue URL to listen to for task results. Messages are received from this queue assuming
# `Body` contains the needed `metadata` and `jobId` (usually set automatically by the Ergo clients).
# Ergo is configured to listen to only one result queue per Airflow instance.
# This can be later adapted to support multiple queues.
# Required.
result_queue_url = $RESULT_SQS_QUEUE

# Threshold time (in seconds) to wait for all scheduled task requests before they are considered
# "urgent". Task requests are only dispatched in batches if there's atleast one urgent task for any
# queue.
# Default: 180
queue_wait_threshold_secs = 180

# Equivalent to `max_active_runs` of `ergo_task_queuer` DAG. Increases parallelism if there usually
# are more than 10 scheduled tasks at any time.
# Default: 1
max_runs_dag_task_queuer = 3

# Equivalent to `max_active_runs` of `ergo_job_collector` DAG.
# Default: 1
max_runs_dag_job_collector = 3

# Poke interval for the request batcher sensor (in seconds).
# Default: 60
poke_interval_task_collect_secs = 60

# Poke interval for the job result collector sensor (in seconds).
# Default: 60
poke_interval_result_collect_secs = 60
```
