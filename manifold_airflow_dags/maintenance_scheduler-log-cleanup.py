"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the airflow scheduler logs.
"""
from airflow.models import DAG, Variable
from airflow.configuration import conf
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
from manifold_airflow_dags.tasks.task_slack_posts import slackpostonfail, slackpostonsuccess
import os
import logging
import airflow
import jinja2


# airflow-scheduler-log-cleanup
START_DATE = airflow.utils.dates.days_ago(1)
SCHEDULER_LOG_FOLDER = "/var/lib/airflow/airflow-app/logs/scheduler"
SCHEDULE_INTERVAL = "@weekly"
DAG_OWNER_NAME = "maintenance"
ALERT_EMAIL_ADDRESSES = ["svc.libdev@temple.edu"]
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 30
)
# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
ENABLE_DELETE = True
# The number of worker nodes you have in Airflow. Will attempt to run this
# process for however many workers there are so that each worker gets its
# logs cleared.
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [SCHEDULER_LOG_FOLDER]
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_scheduler_log_cleanup_worker.lock"

if not SCHEDULER_LOG_FOLDER or SCHEDULER_LOG_FOLDER.strip() == "":
    raise ValueError(
        "SCHEDULER_LOG_FOLDER not found."
    )

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'on_failure_callback': slackpostonfail,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "maintenance_scheduler-log-cleanup",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    template_undefined=jinja2.Undefined
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

start = EmptyOperator(
    task_id='start',
    dag=dag)

log_cleanup = """

MAX_LOG_AGE_IN_DAYS="{{dag_run.conf.maxLogAgeInDays}}"
if [ "${MAX_LOG_AGE_IN_DAYS}" == "" ]; then
    echo "maxLogAgeInDays conf variable isn't included. Using Default '""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'."
    MAX_LOG_AGE_IN_DAYS='""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'
fi
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "SCHEDULER_LOG_FOLDER: '${SCHEDULER_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=`eval $1`
    echo "Process will be Deleting the following File(s)/Directory(s):"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | \
    grep -v '^$' | wc -l` File(s)/Directory(s)"     \
    # "grep -v '^$'" - removes empty lines.
    # "wc -l" - Counts the number of lines
    echo ""
    if [ "${ENABLE_DELETE}" == "true" ];
    then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
        then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code \
                    '${DELETE_STMT_EXIT_CODE}'"

                echo "Removing lock file..."
                rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
                if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
                    echo "Error removing the lock file. \
                    Check file permissions.\
                    To re-run the DAG, ensure that the lock file has been \
                    deleted (""" + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """)."
                    exit ${REMOVE_LOCK_FILE_EXIT_CODE}
                fi
                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No File(s)/Directory(s) to Delete"
        fi
    else
        echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
    fi
}


if [ ! -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """ ]; then

    echo "Lock file not found on this node! \
    Creating it to prevent collisions..."
    touch """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    CREATE_LOCK_FILE_EXIT_CODE=$?
    if [ "${CREATE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error creating the lock file. \
        Check if the airflow user can create files under tmp directory. \
        Exiting..."
        exit ${CREATE_LOCK_FILE_EXIT_CODE}
    fi

    echo ""
    echo "Running Cleanup Process..."

    FIND_STATEMENT="find ${SCHEDULER_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${SCHEDULER_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${SCHEDULER_LOG_FOLDER}/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    echo "Finished Running Cleanup Process"

    echo "Deleting lock file..."
    rm -f """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """
    REMOVE_LOCK_FILE_EXIT_CODE=$?
    if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted (""" + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """)."
        exit ${REMOVE_LOCK_FILE_EXIT_CODE}
    fi

else
    echo "Another task is already deleting logs on this worker node. \
    Skipping it!"
    echo "If you believe you're receiving this message in error, kindly check \
    if """ + str(LOG_CLEANUP_PROCESS_LOCK_FILE) + """ exists and delete it."
    exit 0
fi

"""

for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for dir_id, directory in enumerate(DIRECTORIES_TO_DELETE):

        log_cleanup_op = BashOperator(
            task_id='log_cleanup_worker_num_' + str(log_cleanup_id) + '_dir_' + str(dir_id),
            bash_command=log_cleanup,
            params={
                "directory": str(directory),
                "sleep_time": int(log_cleanup_id)*3,
                "scheduler_log_folder": str(SCHEDULER_LOG_FOLDER),
                },
            dag=dag)

        log_cleanup_op.set_upstream(start)
