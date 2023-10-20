"""Airflow DAG to Run the Blogs Sync rake task on a Manifold instance"""
from datetime import datetime, timedelta
import pendulum
import airflow
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from manifold_airflow_dags.tasks.task_slack_posts import slackpostonfail, slackpostonsuccess

MANIFOLD_INSTANCE_SSH_CONN = airflow.hooks.base.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_BLOGS_SYNC_INTERVAL = airflow.models.Variable.get("MANIFOLD_BLOGS_SYNC_SCHEDULE_INTERVAL")
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
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

MANIFOLD_BLOGS_SYNC_DAG = airflow.DAG(
    "manifold_blogs_sync",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule=MANIFOLD_BLOGS_SYNC_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

sync_blogs_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rails sync:blogs"
"""

sync_blogs = SSHOperator(
    task_id="sync_blogs",
    command=sync_blogs_bash,
    cmd_timeout=None,
    ssh_conn_id="AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE",
    dag=MANIFOLD_BLOGS_SYNC_DAG,
)

post_slack = PythonOperator(
    task_id="slack_post_succ",
    python_callable=slackpostonsuccess,
    dag=MANIFOLD_BLOGS_SYNC_DAG
)

#
# SET UP TASK DEPENDENCIES
post_slack.set_upstream(sync_blogs)
