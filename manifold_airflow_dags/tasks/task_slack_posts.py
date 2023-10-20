"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

def slackpostonsuccess():
    """Task to Post Successful Manifold DAG Completion on Lets Make a CMS Slack."""
    text = "{{ dag.execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}"
    print(text)
    send_slack_webhook_notification(slack_webhook_conn_id="MANIFOLD_SLACK_WEBHOOK", text=":partygritty: " + text)

def slackpostonfail():
    """Task Method to Post Failed Task on Lets Make a CMS Slack."""
    text = "Task failed: {{ dag.dag_id}} {{ ti.task_id }} {{ dag.execution_date }} {{ ti.log_url}}"
    print(text)
    send_slack_webhook_notification(slack_webhook_conn_id="AIRFLOW_CONN_SLACK_WEBHOOK", text=":poop: " + text)
