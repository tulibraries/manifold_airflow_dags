"""Airflow DAG to Run the Events Sync rake task on a Manifold instance"""
from datetime import datetime, timedelta
import pendulum 
import airflow
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_slack_notification(channel="lets-make-a-cms", username="airflow", text=":partygritty: {{ execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ execution_date }} {{ ti.log_url }}")

MANIFOLD_INSTANCE_SSH_CONN = airflow.hooks.base.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_EVENTS_SYNC_INTERVAL = airflow.models.Variable.get("MANIFOLD_EVENTS_SYNC_SCHEDULE_INTERVAL")
#
# CREATE DAG
#
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2019, 5, 28, tz="UTC"),
    "email": ["svc.libdev@temple.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": [slackpostonfail],
    "on_success_callback": [slackpostonsuccess],
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

MANIFOLD_EVENTS_SYNC_DAG = airflow.DAG(
    "manifold_events_sync",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule=MANIFOLD_EVENTS_SYNC_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

sync_events_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rails sync:events"
"""

sync_events = SSHOperator(
    task_id="sync_events",
    command=sync_events_bash,
    cmd_timeout=None,
    ssh_conn_id="AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE",
    dag=MANIFOLD_EVENTS_SYNC_DAG
)
