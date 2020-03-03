"""Airflow DAG to Run the Sitemap Create rake task on a Manifold instance"""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from manifold_airflow_dags.task_slack_posts import slackpostonfail, slackpostonsuccess


AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE_LIST = [
       'AIRFLOW_CONN_MANIFOLD_1_SSH_INSTANCE',
       'AIRFLOW_CONN_MANIFOLD_2_SSH_INSTANCE',
       'AIRFLOW_CONN_MANIFOLD_3_SSH_INSTANCE',
       'AIRFLOW_CONN_MANIFOLD_4_SSH_INSTANCE'
       ]

MANIFOLD_GENERATE_SITEMAP_INTERVAL = airflow.models.Variable.get("MANIFOLD_GENERATE_SITEMAP_SCHEDULE_INTERVAL", default_var="@weekly")

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

the_date_bash = """
sudo su - manifold bash -c \
 echo $(date)
"""

generate_sitemap_bash = """
sudo su - manifold bash -c \
 "cd /var/www/manifold &&\
 RAILS_ENV=production bundle exec rake sitemap:create"
"""

# Starting node task
start_date = SSHOperator(
    task_id='start_date',
    command=the_date_bash,
    dag=MANIFOLD_GENERATE_SITEMAP_DAG,
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE'
)

# Ending node task
#end_date = SSHOperator(
#    task_id='end_date',
#    command=the_date_bash,
#    dag=MANIFOLD_GENERATE_SITEMAP_DAG,
#    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE'
#)

sitemap_generator_tasks = []
for index, instance in enumerate(AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE_LIST):
    sitemap_generator_tasks.append(SSHOperator(
        task_id = 'generate_sitemap_%d' % (index+1),
        command = generate_sitemap_bash,
        dag = MANIFOLD_GENERATE_SITEMAP_DAG,
        ssh_conn_id = 'AIRFLOW_CONN_MANIFOLD_%d_SSH_INSTANCE' % (index+1)))

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=MANIFOLD_GENERATE_SITEMAP_DAG
)

#
# SET UP TASK DEPENDENCIES
#
for task in sitemap_generator_tasks:
    task.set_upstream(start_date)
    post_slack.set_upstream(task)

