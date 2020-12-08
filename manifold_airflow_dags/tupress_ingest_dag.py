"""Airflow DAG to Run the Title Deltas Sync rake task on a TUPress instance"""
from datetime import datetime, timedelta
import airflow
import re
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import Variable
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.python_operator import PythonOperator

TUPRESS_HARVEST_SCHEDULE_INTERVAL = \
        Variable.get("TUPRESS_HARVEST_SCHEDULE_INTERVAL")
TUPRESS_SFTP_PATH = \
        Variable.get("TUPRESS_SFTP_PATH")
TUPRESS_WEB_PATH = \
        Variable.get("TUPRESS_WEB_PATH")
TUPRESS_USER_NAME = \
        Variable.get("TUP_ACCOUNT_NAME")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 28),
    'email': ['steven.ng@temple.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'tupress_database_delta',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=TUPRESS_HARVEST_SCHEDULE_INTERVAL,
)

def calculate_file_to_move(**context):
    sftp_conn = SFTPHook(ftp_conn_id="tupsftp")
    files_list = sftp_conn.list_directory("./")
    just_dated_files = [f for f in files_list if re.match(r'\d+-\d+-\d+', f)]
    # Ignore a file that does not end with DELTA
    just_deltas_files = [f for f in just_dated_files if f.endswith("Titles_DELTA.xml")]
    if just_deltas_files:
        most_recent_deltas_file = max(just_deltas_files)
        context['task_instance'].xcom_push(key="most_recent_deltas_file", value=most_recent_deltas_file)
        return most_recent_deltas_file
    else:
        raise ValueError('No matching files were found on the alma sftp server')
#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
GET_FILE_TO_TRANSFER = PythonOperator(
    task_id='get_file_to_transfer',
    python_callable=calculate_file_to_move,
    provide_context=True,
    dag=DAG
)

#
# Copy the delta file locally
#

SFTP_GET_DELTA = SFTPOperator(
    task_id='sftp_get_delta',
    ssh_conn_id="tupsftp",
    local_filepath=f"/tmp/%s" % "{{ ti.xcom_pull(task_ids='get_file_to_transfer') }}",
    remote_filepath=f"{ TUPRESS_SFTP_PATH }/%s" % "{{ ti.xcom_pull(task_ids='get_file_to_transfer') }}",
    operation="get",
    dag=DAG
)

#
# Copy the delta file to S3
#
SFTP_PUT_DELTA = SFTPOperator(
    task_id='sftp_put_delta',
    ssh_conn_id="tupress",
    local_filepath=f"/tmp/%s" % "{{ ti.xcom_pull(task_ids='get_file_to_transfer') }}",
    remote_filepath=f"{ TUPRESS_WEB_PATH }/%s" % "{{ ti.xcom_pull(task_ids='get_file_to_transfer') }}",
    operation="put",
    dag=DAG
)

ingest_deltas_bash = f"""
sudo su - { TUPRESS_USER_NAME } bash -c \
 "cd /var/www/tupress &&\
 RAILS_ENV=production bundle exec rake db:seed:run_updates['%s']"
""" % f"{ TUPRESS_WEB_PATH }/%s" % "{{ ti.xcom_pull(task_ids='get_file_to_transfer') }}"

INGEST_DELTAS = SSHOperator(
    task_id='ingest_deltas',
    command=ingest_deltas_bash,
    dag=DAG,
    ssh_conn_id='tupress'
)

#
# Clean up cached file
#
REMOVE_CACHED_DELTAS = BashOperator(
    task_id='remove_cached_deltas',
    bash_command=f"rm %s/%s" % (TUPRESS_WEB_PATH, "{{ ti.xcom_pull(task_ids='get_file_to_transfer').replace(' ', '\ ') }}"),
    dag=DAG
)

#
# Sync TUPress Blog
#
sync_blog = f"""
sudo su - { TUPRESS_USER_NAME } bash -c \
 "cd /var/www/tupress &&\
 RAILS_ENV=production bundle exec rake sync:feeds"
"""

SYNC_BLOG = SSHOperator(
    task_id='sync_blog',
    command=sync_blog,
    dag=DAG,
    ssh_conn_id='tupress'
)

#
# SET UP TASK DEPENDENCIES
#
SFTP_GET_DELTA.set_upstream(GET_FILE_TO_TRANSFER)
SFTP_PUT_DELTA.set_upstream(SFTP_GET_DELTA)
INGEST_DELTAS.set_upstream(SFTP_PUT_DELTA)
REMOVE_CACHED_DELTAS.set_upstream(INGEST_DELTAS)
SYNC_BLOG.set_upstream(REMOVE_CACHED_DELTAS)

