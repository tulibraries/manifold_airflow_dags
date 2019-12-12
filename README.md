# Manifold (Library Website) DAGs

[![CircleCI](https://circleci.com/gh/tulibraries/manifold_airflow_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/manifold_airflow_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/7.34.svg)

This is the repository for Manifold (Library Website) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) for data syncing + related jobs. These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

## Repository Structure

This repository has 3 main groups of files:
- Airflow DAG definition python files (ending with `_dag.py`);
- Airflow DAG tasks python files used by the above (starting with `task_`);
- and required local development, test, deployment, and CI files (`tests`, `configs`, `.circleci`, Pipfile, etc.).

## Airflow Expectations

These the Airflow expectations for these Manifold DAGs to successfully run:

**Libraries & Packages**

- Python 3.7
- Python Packages: see the [Pipfile](Pipfile)

**Airflow Variables**

See the expected variables in `variables.json`.

**Airflow Connections**
- `MANIFOLD_INSTANCE_SSH_CONN`: An SSH Connection used to connect to the appropriate deployed Manifold webserver VM.

## Local Development

### Run with Airflow Playbook Make Commands

Basically, clone https://github.com/tulibraries/ansible_playbook_airflow locally; in a shell at the top level of that repository, run `make up`; then git clone/pull your working manifold_airflow_dags code to `ansible_playbook_airflow/dags/manifold_airflow_dags`. This shows & runs your development DAG code in the Airflow docker containers, with a webserver at http://localhost:8010.

The steps for this, typed out:

```
$ cd ~/path/to/cloned/ansible-playbook-airflow
$ pipenv shell
(ansible-playbook-airflow) $ make up
# check http://localhost:8010 is running okay
```

Then change into `dags/manifold_airflow_dags` and `git pull origin your-working-branch` to get your working branch locally available. *Symlinks will not work due to how Airflow & Docker handle mounts.* You could manually copy over files if that works better for you.

Continuing from above:

```
(ansible-playbook-airflow) $ cd dags
(ansible-playbook-airflow) $ cd manifold_airflow_dags
(ansible-playbook-airflow) $ git checkout -b my-working-branch
```

Changes to this DAGs folder will be reflected in the local Airflow web UI and related Airflow services within a few seconds. *DAG folders in the ansible-playbook-airflow directory will not be replaced, updated, or touched by any make commands if they already exist.*

When you're done with local development, git commit & push your changes to manifold_airflow_dags up to your working branch, and tear down the docker resources:

```
(ansible-playbook-airflow) $ make down
(ansible-playbook-airflow) $ exit # leaves the pipenv virtual environment
```

## Linting & Testing

How to run the `pylint` linter on this repository:

```
# Ensure you have the correct Python & Pip running:
$ python --version
  Python 3.7.2
$ pip --version
  pip 18.1 from /home/tul08567/.pyenv/versions/3.7.2/lib/python3.7/site-packages/pip (python 3.7)
# Install Pipenv:
$ pip install pipenv
  Collecting pipenv ...
# Install requirements in Pipenv; requires envvar:
$ SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install --dev
  Pipfile.lock not found, creating ...
# Run the linter:
$ pipenv run pylint manifold_airflow_dags
  ...
```

Linting for Errors only (`pipenv run pylint manifold_airflow_dags -E`) is run by CircleCI on every PR.

How to run pytests (unit tests, largely) on this repository (run by CircleCI on every PR):

```
$ pipenv run pytest
```

## Deployment

CircleCI is used for CI and CD.

### CI

We run pylint and pytest to check the basics. These are run within a Pipenv shell to manage expected packages across all possibly environments.

### CD

We deploy via the tulibraries/ansible-playbook-airflow ansible playbook. This runs, pointing at the appropriate tulibraries/manifold_airflow branch (master) to clone & set up (run pip install based on the Pipenv files) within the relevant Airflow deployed environment.

PRs merged to Master cause QA & Stage Environment deploys using the same airflow ansible playbook mentioned above. PRs merged to Master also queue up a Production Environment deploy, that waits for user input before running.

The idea is to eventually touch the airflow ansible playbook as little as possible, and have DAG changes occur here & deploy from here.
