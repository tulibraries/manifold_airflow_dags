"""Airflow DAG to Sync Prod DB to Qa and Stage"""
import airflow
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.operators.python_operator import PythonOperator
from manifold_airflow_dags.tasks.task_slack_posts import slackpostonfail, slackpostonsuccess

MANIFOLD_INSTANCE_SSH_CONN = \
        BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE")
MANIFOLD_DUMP_DATABASE_INTERVAL = \
        Variable.get("MANIFOLD_DUMP_DATABASE_SCHEDULE_INTERVAL", "@never")
AIRFLOW_S3_CONN_ID = "AIRFLOW_S3"
AIRFLOW_S3_PREFIX = "manifold/"
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 28),
    'email': ["svc.libdev@temple.edu"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': slackpostonfail,
    'retry_delay': timedelta(minutes=5),
}

DAG = airflow.DAG(
    'manifold_database_sync',
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

# 1) Find the latest db dump in s3
# 2) Copy the Dump from s3 to the downstream DB Server
# 3) Drop and pg_restore the DB from the dump on the downstream server
# 4) Run a DB migration from the downstream App server to bring the DB back into shape
# 5) restart the rails app on the downstream server
# 6) Do some cleanup of the files on the downstream DB server
#


def newest_key_with_prefix(aws_conn_id, bucket, prefix):
    client =  S3Hook(aws_conn_id=aws_conn_id).get_conn() # get the underlying boto3 client object
    s3_keys = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    in_date_order = sorted(s3_keys, key= lambda k: k['LastModified'])
    path = in_date_order[-1]['Key']
    return {"full_path": path, "filename": path.split('/')[-1]}

DETERMINE_S3_DB_DUMP_FILE = PythonOperator(
    task_id="get_latest_db_dump_in_s3",
    python_callable=newest_key_with_prefix,
    provide_context=False,
    op_kwargs={
        "aws_conn_id": AIRFLOW_S3_CONN_ID,
        "bucket": AIRFLOW_DATA_BUCKET,
        "prefix": AIRFLOW_S3_PREFIX,
    },
    dag=DAG)

POST_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=DAG)

# Create a temporary SQL script to Terminate connections to Manifold DB so we can drop the DB.
disconnect_db_sql = """
  sudo su - postgres bash -c 'echo "select pg_terminate_backend(pid) from pg_stat_activity where datname=\'manifold\' AND application_name!=\'psql\';" > /tmp/disconnect_db.sql'
"""

# Restore the database:
# 1. drop the db connection by calling the SQL script
# 2. drop the db
# 3. recreate the database
# 4. run pg_restore in a way that recreates the db and tables.
# 5. clean up the temporary disconnect_dbs SQL script
#
# [TODO] - pg_restore returns some ignorable warnings that cause the command rc to be 2, which
# then fails in airflow.
# We use the restore user for this because the restore user does not exist in the prod DB.
# This prevents us from accidentally running the restore on prod db.
drop_and_restore_db = f"""
sudo su - postgres bash -c \
  "psql manifold -f \/tmp\/disconnect_db.sql &&\
  dropdb manifold &&\
  createdb manifold &&\
  pg_restore --username=restore --dbname=manifold /tmp/%s &&\
  rm \/tmp\/disconnect_db.sql"
""" % "{{ ti.xcom_pull(task_ids='get_latest_db_dump_in_s3')['filename'] }}"

run_db_migration = f"""
sudo su - manifold bash -c \
  "cd /var/www/manifold &&\
   RAILS_ENV=production bundle exec rails db:migrate"
"""

restart_rails_app = f"""
sudo systemctl restart httpd
"""

cleanup_files = f"""
sudo su root - bash -c \
  'rm /tmp/%s'
""" % "{{ ti.xcom_pull(task_ids='get_latest_db_dump_in_s3')['filename'] }}"


##
## BEFORE RUNNING, MAKE SURE YOU HAVE SET UP THE MANIFOLD_{server}_DB and MANIFOLD_{server}_APP connections
## you'll need to run this locally. When targetting manifold vagrant, they should use conan as user,
## host of host.docker.internal, and the ports as defined in the manifold vagrant host file
## https://github.com/tulibraries/ansible-playbook-manifold/blob/qa/inventory/vagrant/hosts
##
## If running locally on a Vagrant Box, replace the list ["QA","STAGE"] with ["VAGRANT"]
##
for server in ["QA","STAGE"]:
    COPY_DB_DUMP_TO_SERVER = S3ToSFTPOperator(
        task_id=f"copy_db_dump_to_{server}",
        s3_conn_id=AIRFLOW_S3_CONN_ID,
        s3_bucket=AIRFLOW_DATA_BUCKET,
        s3_key="{{ ti.xcom_pull(task_ids='get_latest_db_dump_in_s3')['full_path'] }}",
        sftp_conn_id=f"MANIFOLD_{server}_DB",
        sftp_path="/tmp/" + "{{ ti.xcom_pull(task_ids='get_latest_db_dump_in_s3')['filename'] }}",
        dag=DAG)

    UPLOAD_DISCONNECT_COMMAND = SSHOperator(
        task_id=f"upload_db_disconnect_to_{server}",
        ssh_conn_id=f"MANIFOLD_{server}_DB",
        command=disconnect_db_sql,
        dag=DAG)

    DROP_AND_RESTORE_DB = SSHOperator(
        task_id=f"drop_and_restore_{server}_db",
        ssh_conn_id=f"MANIFOLD_{server}_DB",
        command=drop_and_restore_db,
        dag=DAG)

    RUN_DB_MIGRATION = SSHOperator(
        task_id=f"run_{server}_db_migration",
        ssh_conn_id=f"MANIFOLD_{server}_APP",
        command=run_db_migration,
        dag=DAG)

    RESTART_RAILS_APP = SSHOperator(
         task_id=f"restart_{server}_rails_app",
        ssh_conn_id=f"MANIFOLD_{server}_APP",
        command=restart_rails_app,
        dag=DAG)

    CLEANUP_FILES = SSHOperator(
        task_id=f"clean_up_files_on_{server}_DB",
        ssh_conn_id=f"MANIFOLD_{server}_DB",
        command=cleanup_files,
        dag=DAG)

    COPY_DB_DUMP_TO_SERVER.set_upstream(DETERMINE_S3_DB_DUMP_FILE)
    UPLOAD_DISCONNECT_COMMAND.set_upstream(COPY_DB_DUMP_TO_SERVER)
    DROP_AND_RESTORE_DB.set_upstream(UPLOAD_DISCONNECT_COMMAND)
    RUN_DB_MIGRATION.set_upstream(DROP_AND_RESTORE_DB)
    RESTART_RAILS_APP.set_upstream(RUN_DB_MIGRATION)
    CLEANUP_FILES.set_upstream(RESTART_RAILS_APP)
    CLEANUP_FILES.set_downstream(POST_SLACK)
