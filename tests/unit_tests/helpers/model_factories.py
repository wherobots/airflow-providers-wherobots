"""
helper functions for tests
"""

from polyfactory.factories.dataclass_factory import DataclassFactory

from airflow_providers_wherobots.wherobots.models import Run


class RunFactory(DataclassFactory[Run]): ...
