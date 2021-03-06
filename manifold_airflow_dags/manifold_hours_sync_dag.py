"""Airflow DAG to Run the Hours Sync rake task on a Manifold instance"""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from manifold_airflow_dags.task_slack_posts import slackpostonfail, slackpostonsuccess

MANIFOLD_INSTANCE_SSH_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_HOURS_SYNC_INTERVAL = airflow.models.Variable.get("MANIFOLD_HOURS_SYNC_SCHEDULE_INTERVAL")
#
# CREATE DAG
#
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 28),
    'email': ['chad.nelson@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

MANIFOLD_HOURS_SYNC_DAG = airflow.DAG(
    'manifold_hours_sync',
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=MANIFOLD_HOURS_SYNC_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

sync_hours_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rake sync:hours"
"""

sync_hours = SSHOperator(
    task_id='sync_hours',
    command=sync_hours_bash,
    dag=MANIFOLD_HOURS_SYNC_DAG,
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE'
)

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=MANIFOLD_HOURS_SYNC_DAG
)

#
# SET UP TASK DEPENDENCIES
#
post_slack.set_upstream(sync_hours)
