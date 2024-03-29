# Manifold Airflow Dags

[![CircleCI](https://circleci.com/gh/tulibraries/manifold_airflow_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/manifold_airflow_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

This is the repository for the Manifold Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) and related jobs. These DAGs are expected to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

## Repository Structure

This repository has 3 main groups of files:
- Airflow DAG definition python files (ending with `_dag.py`);
- Airflow DAG tasks python files used by the above (starting with `task_`);
- and required local development, test, deployment, and CI files (`tests`, `configs`, `.travis`, Pipfile, etc.).

## Airflow Expectations

The following are the Airflow expectations for the DAGs to successfully run:

**Libraries & Packages**

- Python Version and Packages: see the [Pipfile](Pipfile)

**Airflow Variables**

See `variables.json` file

**Airflow Connections**
- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).

## Local Development

### Run with local setup
The following commands are available for local testing and development:

* `make up`: Sets up local airflow with these dags.
* `make down`: Close the local setup.
* `make reload`: Reload configurations for local setup.
* `make tty-webserver`: Enter airflow webserver container instance.
* `make tty-worker`: Enter airflow worker container instance.
* `make tty-schedular`: Enter airflow schedular contain instance.

