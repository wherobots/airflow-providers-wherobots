"""
shared helper functions for tests
"""

import os

import pytest
from airflow.models import Connection
from pytest_mock import MockerFixture


@pytest.fixture(scope="function")
def prod_conn(mocker: MockerFixture):
    prod_host = "api.cloud.wherobots.com"
    prod_api_token = os.getenv("PROD_API_TOKEN")
    if not prod_api_token:
        raise ValueError("PROD_API_TOKEN is not set")
    prod_conn = Connection(
        conn_id="wherobots_prod_conn",
        conn_type="http",
        host=prod_host,
        password=prod_api_token,
    )
    mocker.patch.dict(
        "os.environ", AIRFLOW_CONN_WHEROBOTS_PROD_CONN=prod_conn.get_uri()
    )
    return prod_conn
