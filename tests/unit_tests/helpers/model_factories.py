"""
helper functions for tests
"""

from polyfactory.factories.pydantic_factory import ModelFactory

from airflow_providers_wherobots.wherobots.models import Run


class RunFactory(ModelFactory[Run]): ...
