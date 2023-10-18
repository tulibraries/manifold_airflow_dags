"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""
from tulflow import tasks

def slackpostonsuccess(**context):
    """Task to Post Successful Manifold DAG Completion on Lets Make a CMS Slack."""
    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('data_interval_start')
    msg = "{} DAG {} success: {}".format(
        date,
        dagid,
        logurl
    )
    return tasks.execute_slackpostonsuccess(context, slack_webhook_conn_id="MANIFOLD_SLACK_WEBHOOK", message=msg)

def slackpostonfail(context):
    """Task Method to Post Failed Task on Lets Make a CMS Slack."""
    msg = None
    return tasks.execute_slackpostonfail(context, slack_webhook_conn_id="AIRFLOW_CONN_SLACK_WEBHOOK", message=msg)
