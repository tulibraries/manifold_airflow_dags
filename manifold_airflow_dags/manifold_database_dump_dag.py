"""Airflow DAG to Run the Blogs Sync rake task on a Manifold instance"""
from datetime import datetime, timedelta
import pendulum
import airflow
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_slack_notification(channel="lets-make-a-cms", username="airflow", text=":partygritty: {{ execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ execution_date }} {{ ti.log_url }}")


MANIFOLD_INSTANCE_SSH_CONN = \
        BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_DUMP_DATABASE_INTERVAL = \
        Variable.get("MANIFOLD_DUMP_DATABASE_SCHEDULE_INTERVAL")
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_S3_PATH = "manifold"
AIRFLOW_DATA_BUCKET = \
        Variable.get("AIRFLOW_DATA_BUCKET")

DUMP_PATH = "/tmp"
UPLOAD_FILENAME = "manifold-{{ data_interval_start.strftime('%Y-%m-%dT%H%z')}}.sqlc"
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2019, 5, 28, tz="UTC"),
    "email": ["svc.libdev@temple.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "on_failure_callback": [slackpostonfail],
    "on_success_callback": [slackpostonsuccess],
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "manifold_database_dump",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=MANIFOLD_DUMP_DATABASE_INTERVAL,
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.

SET_DUMP_NAME = BashOperator(
    task_id="set_dump_name",
    bash_command="echo " + "{{ data_interval_start.strftime('%Y%m%d_%H%M%S') }}",
    dag=DAG
)

# Note: single quotes are required in the bash command line.
dump_database_bash = f"""
sudo su - postgres bash -c \
  'pg_dump --format=c --file="{ DUMP_PATH }/dbdump_%s.sqlc" manifold'
""" % "{{ ti.xcom_pull(task_ids='set_dump_name') }}"

DUMP_DATABASE = SSHOperator(
    task_id="dump_database",
    command=dump_database_bash,
    cmd_timeout=None,
    ssh_conn_id="manifold-db",
    dag=DAG
)

#
# Copy the dump file to S3
#
DATABASE_DUMP_TO_S3 = SFTPToS3Operator(
    task_id="database_dump_to_s3",
    sftp_conn_id="manifold-db",
    sftp_path=f"{ DUMP_PATH }/dbdump_%s.sqlc" % "{{ ti.xcom_pull(task_ids='set_dump_name') }}",
    s3_conn_id="AIRFLOW_S3",
    s3_bucket=AIRFLOW_DATA_BUCKET,
    s3_key="%s/dbdump_%s.sqlc" % ("manifold", "{{ ti.xcom_pull(task_ids='set_dump_name') }}"),
    dag=DAG
)


#
# SET UP TASK DEPENDENCIES
#
DUMP_DATABASE.set_upstream(SET_DUMP_NAME)
DATABASE_DUMP_TO_S3.set_upstream(DUMP_DATABASE)
