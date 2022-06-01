"""Airflow DAG to Run the Sitemap Create rake task on a Manifold instance"""
from datetime import datetime, timedelta
import airflow
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from manifold_airflow_dags.tasks.task_slack_posts import slackpostonfail, slackpostonsuccess

MANIFOLD_INSTANCE_SSH_CONN = airflow.hooks.base.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_GENERATE_SITEMAP_INTERVAL = airflow.models.Variable.get("MANIFOLD_GENERATE_SITEMAP_SCHEDULE_INTERVAL", default_var="@weekly")
#
# CREATE DAG
#
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 28),
    'email': ["svc.libdev@temple.edu"],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

MANIFOLD_GENERATE_SITEMAP_DAG = airflow.DAG(
    'manifold_generate_sitemap',
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval=MANIFOLD_GENERATE_SITEMAP_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

generate_sitemap_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rake sitemap:create"
"""

generate_sitemap = SSHOperator(
    task_id='generate_sitemap',
    command=generate_sitemap_bash,
    dag=MANIFOLD_GENERATE_SITEMAP_DAG,
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE'
)

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=MANIFOLD_GENERATE_SITEMAP_DAG
)

#
# SET UP TASK DEPENDENCIES
#
post_slack.set_upstream(generate_sitemap)
