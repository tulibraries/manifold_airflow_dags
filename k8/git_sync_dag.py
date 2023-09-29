from datetime import datetime, timedelta
import pendulum
import airflow
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

GIT_SOURCE_URL = k8s.V1EnvVar(name="GIT_SOURCE_URL", value="https://github.com/tulibraries/manifold.git")
CRONJOB = k8s.V1EnvVar(name="CRONJOB", value="yes")
GIT_SYNC_SECRET_REF = k8s.V1SecretEnvSource(name="git-sync", optional=False)
GIT_SYNC_SECRET = k8s.V1EnvFromSource(secret_ref=GIT_SYNC_SECRET_REF)
SECURITY_CONTEXT = k8s.V1SecurityContext(allow_privilege_escalation=False,
                                          privileged=False,
                                          read_only_root_filesystem=False,
                                          run_as_non_root=False)
# CREATE DAG
DEFAULT_ARGS = {
    "owner": "manifold",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "git-sync",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule="@hourly"
)

GIT_SYNC = KubernetesPodOperator(
    task_id="git-sync",
    name="git-sync",
    namespace= "manifold-airflow-qa",
    image="harbor.k8s.temple.edu/tulibraries/git-sync:latest",
    in_cluster=True,
    env_vars=[ GIT_SOURCE_URL, CRONJOB ],
    env_from=[ GIT_SYNC_SECRET ],
    image_pull_policy="Always",
    dnspolicy="ClusterFirst",
    on_finish_action="delete_pod",
    security_context=SECURITY_CONTEXT,
    dag=DAG)

GIT_SYNC
