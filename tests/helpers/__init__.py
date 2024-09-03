"""
helper functions for tests
"""

from airflow_providers_wherobots.wherobots.models import Run
from tests.helpers.model_factories import RunFactory

run_factory = RunFactory.create_factory(Run)
