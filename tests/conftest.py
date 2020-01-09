""" PyTest Configuration file. """
import os
import subprocess
import airflow

def pytest_sessionstart():
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    repo_dir = os.getcwd()
    subprocess.run("airflow initdb", shell=True)
    subprocess.run("mkdir -p dags/manifold_airflow_dags", shell=True)
    subprocess.run("mkdir -p data", shell=True)
    subprocess.run("mkdir -p logs", shell=True)
    subprocess.run("cp *.py dags/manifold_airflow_dags", shell=True)
    subprocess.run("cp -r configs dags/manifold_airflow_dags", shell=True)
    subprocess.run("cp -r scripts dags/manifold_airflow_dags", shell=True)
    airflow.models.Variable.set("AIRFLOW_HOME", repo_dir)
    airflow.models.Variable.set("AIRFLOW_DATA_DIR", repo_dir + '/data')
    airflow.models.Variable.set("AIRFLOW_LOG_DIR", repo_dir + '/logs')
    airflow.models.Variable.set("MANIFOLD_EVENTS_SYNC_SCHEDULE_INTERVAL", "@weekly")
    airflow.models.Variable.set("MANIFOLD_BLOGS_SYNC_SCHEDULE_INTERVAL", "@weekly")
    airflow.models.Variable.set("MANIFOLD_DUMP_DATABASE_SCHEDULE_INTERVAL", "@weekly")
    airflow.models.Variable.set("MANIFOLD_HOURS_SYNC_SCHEDULE_INTERVAL", "@weekly")


    manifold = airflow.models.Connection(
                conn_id="AIRFLOW_CONN_MANIFOLD_SSH_INSTANCE",
                conn_type="SSH",
                )
    slack = airflow.models.Connection(
                conn_id="AIRFLOW_CONN_SLACK_WEBHOOK",
                conn_type="http",
                host="127.0.0.1/services",
                port="",
                )
    airflow_session = airflow.settings.Session()
    airflow_session.add(manifold)
    airflow_session.add(slack)
    airflow_session.commit()

def pytest_sessionfinish():
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    subprocess.run("rm -rf dags", shell=True)
    subprocess.run("rm -rf data", shell=True)
    subprocess.run("rm -rf logs", shell=True)
    subprocess.run("yes | airflow resetdb", shell=True)
