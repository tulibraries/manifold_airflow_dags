"""Airflow DAG to Run the Finding Aids JSON task on a Manifold instance"""
from datetime import datetime, timedelta
import pendulum
import airflow
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_slack_notification(channel="lets-make-a-cms", username="airflow", text=":partygritty: {{ execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ execution_date }} {{ ti.log_url }}")

MANIFOLD_INSTANCE_SSH_CONN = airflow.hooks.base.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_FINDING_AIDS_INTERVAL = airflow.models.Variable.get("MANIFOLD_FINDING_AIDS_SCHEDULE_INTERVAL", default_var="@never")
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

MANIFOLD_FINDING_AIDS_DAG = airflow.DAG(
    "manifold_finding_aids",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=MANIFOLD_FINDING_AIDS_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

finding_aids_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rails sync:fas"
"""

finding_aids = SSHOperator(
    task_id="finding_aids",
    command=finding_aids_bash,
    cmd_timeout=None,
    ssh_conn_id="AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE",
    dag=MANIFOLD_FINDING_AIDS_DAG
)