# Manifold Airflow Dags

[![CircleCI](https://circleci.com/gh/tulibraries/manifold_airflow_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/manifold_airflow_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

This is the repository for Funnel Cake (PA Digital / DPLA Data QA Interface) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) for data indexing to Solr + related jobs. These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

## Repository Structure

This repository has 3 main groups of files:
- Airflow DAG definition python files (ending with `_dag.py`);
- Airflow DAG tasks python files used by the above (starting with `task_`);
- and required local development, test, deployment, and CI files (`tests`, `configs`, `.travis`, Pipfile, etc.).

## Airflow Expectations

These the Airflow expectations for these Funnel Cake DAGs to successfully run:

**Libraries & Packages**

- Python: Version as specified in `.python-version`, currently 3.6.8.
- Python Packages: see the [Pipfile](Pipfile)

**Airflow Variables**

These variable are initially set in `variables.json`

- `AIRFLOW_DATA_BUCKET`: The AWS S3 Bucket label (label / name, not ARN or URI) the harvested OAI-PMH XML data is put into / indexed from.
- `MANIFOLD_BLOGS_SYNC_SCHEDULE_INTERVAL`: Cron schedule to sync blogs. (Usually `@daily`)
- `MANIFOLD_DUMP_DATABASE_SCHEDULE_INTERVAL`: Chron schedule to dump the Manifold database to AWS (Usually `@weekly`)
- `MANIFOLD_EVENTS_SYNC_SCHEDULE_INTERVAL`: Cron schedule to syncrhonize library events (Usually `@daily`)
- `MANIFOLD_HOURS_SYNC_SCHEDULE_INTERVAL`: Cron schedule to syncronize library hours (Usually `@daily`)
- `TUP_ACCOUNT_NAME`: Account used to access TUPress to retrieve blog RSS feed 
- `TUPRESS_HARVEST_SCHEDULE_INTERVAL`: Cron schedule to syncrhoize TUPress blog feed (Usually 8AM daily - `0 8 * * *`)
- `TUPRESS_SFTP_PATH`: destination server file path for delta file (where you will put the tupress delta file for processing)
- `TUPRESS_WEB_PATH`: source server File path to delta file on the TUPress web server (Where you will get the tupress delta file for processing)

See the rest of the expected variables in `variables.json`.

**Airflow Connections**
- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).

## Local Development

### Run with Airflow Playbook Make Commands

Basically, clone https://github.com/tulibraries/ansible_playbook_airflow locally; in a shell at the top level of that repository, run `make up` then `make update`; then git clone/pull your working funcake_dags code to ansible_playbook_airflow/dags/funcake-dags. This shows & runs your development DAG code in the Airflow docker containers, with a webserver at http://localhost:8080.

Depending on the computer you are running on, you may need to extend sleep times and wait a few minutes before the Docker containers are fully up and running Those key points
are documented in the steps below.

The steps for this, typed out:

```
$ clone https://github.com/tulibraries/manifold-airflow-dags.gits
$ cd manifold-airflow-dags
#
# Edit`variables.json` to modify Airflow variables or additional Airflow variables
#
$ make up
#
# If make fails with several: 
#    Variable import failed: ProgrammingError('(psycopg2.ProgrammingError) relation "variable" does not exist...
# you may need to extend the sleep time to wait for the docker-compose build to complete.  See [Docker-Compose Build Sleep Time](#BuildSleep) below. 
#
# check http://localhost:8010 is running okay.
# If you are unable to connect, you may have to allow time for the containerse to complete startup. Wait another 2 minutes and try again.
```

change into `manifold_airflow_dags` and `git pull origin your-working-branch` to get your working branch locally available. *Symlinks will not work due to how Airflow & Docker handle mounts.* You could manually copy over files if that works better for you.

### Run with local setup

* `make up`: Sets up local airflow with these dags.
* `make down`: Close the local setup.
* `make reload`: Reload configurations for local setup.
* `make tty-webserver`: Enter airflow webserver container instance.
* `make tty-worker`: Enter airflow worker container instance.
* `make tty-schedular`: Enter airflow schedular contain instance.

### <<a id="BuildSleep"></a>Build sleep time

If you encounter the following error from `make up` extend the build target's sleep time in `airflow-docker-dev-setup/Makefile` to 120 seconds.
Edit `airflow-docker-dev-setup/Makefile` based on the code snippet below and re-run `make up`.

```
build:
	@echo "Building airflow containers, networks, volumes"
	docker-compose pull
	docker-compose -p 'infra' up --build -d
	sleep 40 # <== Change from 40 to 120
	@echo "airflow running on http://127.0.0.1:8010"
```
