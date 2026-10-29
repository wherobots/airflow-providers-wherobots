import importlib

import pytest

from airflow_providers_wherobots import _compat
from airflow_providers_wherobots.hooks.rest_api import WherobotsRestAPIHook
from airflow_providers_wherobots.operators.run import WherobotsRunOperator

FALLBACKS = {
    "BaseOperator": "airflow.models",
    "Connection": "airflow.models",
    "BaseHook": "airflow.hooks.base",
}


@pytest.mark.parametrize("name", sorted(FALLBACKS))
def test_compat_prefers_the_task_sdk(name: str) -> None:
    try:
        expected = getattr(importlib.import_module("airflow.sdk"), name)
    except (ImportError, AttributeError):
        expected = getattr(importlib.import_module(FALLBACKS[name]), name)
    assert getattr(_compat, name) is expected


def test_provider_classes_build_on_the_compat_bases() -> None:
    assert issubclass(WherobotsRunOperator, _compat.BaseOperator)
    assert issubclass(WherobotsRestAPIHook, _compat.BaseHook)
