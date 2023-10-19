"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

def slackpostonsuccess(**context):
    """Task to Post Successful Manifold DAG Completion on Lets Make a CMS Slack."""
    task_instance = context.get('task_instance')
    log_url =  task_instance.log_url
    dag_id =  task_instance.dag_id
    date = context.get('data_interval_start')
    text = "{} DAG {} success: {}".format(date, dag_id, log_url)

    return send_slack_webhook_notification(slack_webhook_conn_id="MANIFOLD_SLACK_WEBHOOK", text=":partygritty: " + text)

def slackpostonfail(context):
    """Task Method to Post Failed Task on Lets Make a CMS Slack."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    dag_id = task_instance.dag_id
    task_date = context.get("execution_date")
    if not text:
        text = "Task failed: {} {} {} {}".format(dag_id, task_id, task_date, log_url)

    return send_slack_webhook_notification(slack_webhook_conn_id="AIRFLOW_CONN_SLACK_WEBHOOK", text=":poop: " + text)
