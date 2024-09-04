"""
shared helper functions for tests
"""

import os

import pytest
from airflow.models import Connection
from pytest_mock import MockerFixture


@pytest.fixture(scope="function")
def staging_conn(mocker: MockerFixture):
    staging_host = "api.staging.wherobots.com"
    staging_api_token = os.getenv("STAGING_API_TOKEN")
    if not staging_api_token:
        raise ValueError("STAGING_API_TOKEN is not set")
    staging_conn = Connection(
        conn_id="wherobots_staging_conn",
        conn_type="http",
        host=staging_host,
        password=staging_api_token,
    )
    mocker.patch.dict(
        "os.environ", AIRFLOW_CONN_WHEROBOTS_STAGING_CONN=staging_conn.get_uri()
    )
    return staging_conn
