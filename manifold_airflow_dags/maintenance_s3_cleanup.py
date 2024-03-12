"""
A maintenance workflow to periodically clean out objects in s3.
The variable AIRFLOW_S3_PREFIX will need to be set in the Airflow UI for the key 
you would like to clean up. The S3 objects have datetime strings as keys, so you can
include the first part of the key to delete all objects from that year.  
For example:  catalog_pre_production_oia_harvest/2020 

We don't want to empty out all of the objects in the Airflow bucket, so it will be 
necessary to update the variable with each key that you would like to clean up
individually and rerun the dag.
"""
from datetime import timedelta
import pendulum
import airflow
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Set this prefix to the directory that you would like to clean up.
# Wildcard characters can be used.
# For example: catalog_pre_production_oai_harvest/2020*
AIRFLOW_S3_PREFIX = Variable.get("AIRFLOW_S3_PREFIX")

slackpostonsuccess = send_slack_notification(channel="infra-alerts", username="airflow", text=":partygritty: {{ logical_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="infra_alerts", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ logical_date }} {{ ti.log_url }}")

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": [slackpostonfail],
    "on_success_callback": [slackpostonsuccess],
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@yearly",
}

DAG = airflow.DAG(
    "maintenance_s3_cleanup",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

DELETE_OBJECTS = S3DeleteObjectsOperator(
    task_id="delete_objects",
    bucket=AIRFLOW_DATA_BUCKET,
    prefix=AIRFLOW_S3_PREFIX,
    aws_conn_id=AIRFLOW_S3.conn_id,
    dag=DAG
)