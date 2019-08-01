"""Airflow DAG to Sync data between Manifold instance DBs"""
from datetime import datetime, timedelta
import airflow
from airflow.contrib.operators import SSHOperator
from airflow.operators import PythonOperator
from cob_datapipeline.task_slackpost import task_generic_slackpostsuccess, task_slackpostonfail

MANIFOLD_PROD_INSTANCE_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_PROD_DB")
MANIFOLD_STAGE_INSTANCE_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_STAGE_DB")
MANIFOLD_QA_INSTANCE_CONN = airflow.hooks.base_hook.BaseHook.get_connection("AIRFLOW_CONN_MANIFOLD_QA_DB")
MANIFOLD_DB_SYNC_INTERVAL = airflow.models.Variable.get("MANIFOLD_DB_SYNC_SCHEDULE_INTERVAL")
MANIFOLD_DB_DUMP_TIMESTAMP = datetime.now().replace(microsecond=0).isoformat()
MANIFOLD_DB_DUMP_FILENAME = "manifold-prod-db-dump-{}.sqlc".format(MANIFOLD_DB_DUMP_TIMESTAMP)

AF_DATA_DIR = Variable.get("AIRFLOW_DATA_DIR")
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

MANIFOLD_DB_SYNC_DAG = airflow.DAG(
    'manifold_db_sync',
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=MANIFOLD_DB_SYNC_INTERVAL
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#


def slackpostonsuccess(dag, **context):
    """Task Method to Post Successful TUL Cob Index DAG Completion on Slack."""

    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')
    message = "{} DAG {} success: Sync'd all the Manifold DBs {}".format(date, dagid, logurl)

    return task_generic_slackpostsuccess(dag, message).execute(context=context)



dump_prod_db_bash = "sudo su - postgres bash -c \
\"pg_dump --format=c --file=`/tmp/{}\" \
manifold".format(MANIFOLD_DB_DUMP_FILENAME)


create_prod_db_dump = SSHOperator(
    task_id='manifold_create_prod_db_dump',
    command=dump_db_to_local_tmp_bash,
    dag=MANIFOLD_EVENTS_SYNC_DAG,
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_PROD_DB'
)


get_prod_db_dump = SFTPOperator(
    task_id="manifold_get_prod_db_dump",
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_PROD_DB',
    local_filepath="{}/manifold/{}".format(AF_DATA_DIR, MANIFOLD_DB_DUMP_FILENAME),
    remote_filepath="/tmp/{}".format(MANIFOLD_DB_DUMP_FILENAME),
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)


put_prod_dump_on_stage = SFTPOperator(
    task_id="manifold_put_prod_dump_on_stage",
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_STAGE_DB',
    local_filepath="{}/manifold/{}".format(AF_DATA_DIR, MANIFOLD_DB_DUMP_FILENAME),
    remote_filepath="/tmp/{}".format(MANIFOLD_DB_DUMP_FILENAME),
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)


put_prod_dump_on_qa = SFTPOperator(
    task_id="manifold_put_prod_dump_on_qa",
    ssh_conn_id='AIRFLOW_CONN_MANIFOLD_QA_DB',
    local_filepath="{}/manifold/{}".format(AF_DATA_DIR, MANIFOLD_DB_DUMP_FILENAME),
    remote_filepath="/tmp/{}".format(MANIFOLD_DB_DUMP_FILENAME),
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)


restore_prod_dump = "sudo su - postgres bash -c \
\"pg_restore --format=c --file=`/tmp/{}\" \
manifold".format(MANIFOLD_DB_DUMP_FILENAME)




post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=MANIFOLD_EVENTS_SYNC_DAG
)

#
# SET UP TASK DEPENDENCIES
#
post_slack.set_upstream(sync_events)
