"""Unit Tests for the Manifold Blogs Sync DAG."""
import os
import unittest
import airflow
from manifold_airflow_dags.manifold_hours_sync_dag import MANIFOLD_HOURS_SYNC_DAG

class TestManifoldHoursSyncDag(unittest.TestCase):
    """Primary Class for Testing the Manifold Hours Sync DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, MANIFOLD_HOURS_SYNC_DAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(MANIFOLD_HOURS_SYNC_DAG.dag_id, "manifold_hours_sync")

    def test_dag_interval_is_variable(self):
        """Unit test that the DAG schedule is set by configuration"""
        self.assertEqual(MANIFOLD_HOURS_SYNC_DAG.schedule_interval, "@weekly")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "sync_hours",
            ])