"""Airflow DAG to Run the Blogs Sync rake task on a Manifold instance"""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from cob_datapipeline.task_slackpost import task_generic_slackpostsuccess, task_slackpostonfail

MANIFOLD_INSTANCE_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_BLOGS_SYNC_INTERVAL = airflow.models.Variable.get("MANIFOLD_BLOGS_SYNC_SCHEDULE_INTERVAL")
#
# CREATE DAG
#
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 28),
    'email': ['chad.nelson@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': task_slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

MANIFOLD_BLOGS_SYNC_DAG = airflow.DAG(
    'manifold_blogs_sync', default_args=DEFAULT_ARGS, catchup=False,
    max_active_runs=1, schedule_interval=MANIFOLD_BLOGS_SYNC_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#


def slackpostonsuccess(dag, **context):
    """Task Method to Post Successful Manifold Blogs Sync DAG Completion on Slack."""

    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')

    message = "{} DAG {} success: Sync'd all the blogs {}".format(date, dagid, logurl)

    return task_generic_slackpostsuccess(dag, message).execute(context=context)

sync_blogs_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rake sync:blogs"
"""

sync_blogs = SSHOperator(
        task_id='sync_blogs',
        command=sync_blogs_bash,
        dag=MANIFOLD_BLOGS_SYNC_DAG,
        ssh_conn_id='AIRFLOW_CONN_MANIFOLD_INSTANCE'
    )

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=MANIFOLD_BLOGS_SYNC_DAG
)

#
# SET UP TASK DEPENDENCIES
#
post_slack.set_upstream(sync_blogs)
