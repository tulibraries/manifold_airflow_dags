"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""
from tulflow import tasks

def slackpostonsuccess(dag, **context):
    """Task to Post Successful Manifold DAG Completion on Lets Make a CMS Slack."""
    ti = context.get('task_instance')
    logurl = ti.log_url
    dagid = ti.dag_id
    date = context.get('execution_date')
    msg = "{} DAG {} success: {}".format(
        date,
        dagid,
        logurl
    ),
    return tasks.execute_slackpostonsuccess(context, conn_id="MANIFOLD_SLACK_WEBHOOK", message=msg)

def slackpostonfail(dag, **context):
    """Task Method to Post Failed Task on Lets Make a CMS Slack."""
    msg = None
    return tasks.execute_slackpostonfail(context, conn_id="MANIFOLD_SLACK_WEBHOOK", message=msg)
