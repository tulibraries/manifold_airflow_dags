from datetime import datetime, timedelta
import pendulum
import airflow
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# CREATE DAG
DEFAULT_ARGS = {
    "owner": "manifold",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": pendulum.datetime(2018, 12, 13, tz="UTC"),
    #"retries": 2,
    #"retry_delay": timedelta(minutes=5),
}

DAG = airflow.DAG(
    "hours-sync",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule="@daily"
)

SECURITY_CONTEXT = k8s.V1SecurityContext(allow_privilege_escalation=False,
                                          privileged=False,
                                          read_only_root_filesystem=False,
                                          run_as_non_root=False)

EXECJS_RUNTIME = k8s.V1EnvVar(name="EXECJS_RUNTIME", value="Disabled")
RAILS_ENV = k8s.V1EnvVar(name="RAILS_ENV", value="production")
RAILS_SERVE_STATIC_FILES = k8s.V1EnvVar(name="RAILS_SERVE_STATIC_FILES", value="yes")
db_secret_selector = k8s.V1SecretKeySelector(key="password", name="manifold-db", optional=False)
manifold_db = k8s.V1EnvVarSource(secret_key_ref=db_secret_selector)
MANIFOLD_DB_PASSWORD = k8s.V1EnvVar(name="MANIFOLD_DB_PASSWORD", value_from=manifold_db)
manifold_secrets_ref = k8s.V1SecretEnvSource(name="manifold", optional=False)
MANIFOLD_SECRETS = k8s.V1EnvFromSource(secret_ref=manifold_secrets_ref)
APP_TMP_MOUNT = k8s.V1VolumeMount(mount_path="/app/tmp",
                                  name="manifold-app-tmp-volume-1")
APP_TMP_VOLUME = k8s.V1Volume(name="manifold-app-tmp-volume-1",
                              empty_dir=k8s.V1EmptyDirVolumeSource())


SYNC = KubernetesPodOperator(
    task_id="hours-sync",
    name="hours-sync",
    namespace="manifold-qa",
    image="harbor.k8s.temple.edu/tulibraries/manifold:latest",
    cmds=[ "/bin/sh" ],
    arguments=[ "-c", "rails sync:hours" ],
    in_cluster=True,
    env_vars=[ EXECJS_RUNTIME, RAILS_ENV, MANIFOLD_DB_PASSWORD ],
    env_from=[ MANIFOLD_SECRETS ],
    image_pull_policy="Always",
    dnspolicy="ClusterFirst",
    on_finish_action="delete_pod",
    security_context=SECURITY_CONTEXT,
    volume_mounts=[ APP_TMP_MOUNT ],
    volumes=[ APP_TMP_VOLUME ],
    dag=DAG)
SYNC
