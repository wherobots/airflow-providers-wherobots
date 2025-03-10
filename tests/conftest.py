"""
shared helper functions for tests
"""

import os

import pytest
from airflow import DAG
from airflow.models import Connection
from pytest_mock import MockerFixture

from tests.unit_tests.operators.test_run import TEST_DAG_ID, DEFAULT_START


@pytest.fixture(scope="function", autouse=True)
def test_default_conn(mocker: MockerFixture):
    default_host = "localhost:3000"
    default_connection = Connection(
        conn_type="http", host=default_host, password="token"
    )
    test_conn_uri = default_connection.get_uri()
    mocker.patch.dict("os.environ", AIRFLOW_CONN_WHEROBOTS_DEFAULT=test_conn_uri)
    return default_connection


@pytest.fixture(scope="function")
def clean_airflow_db():
    os.system("airflow db reset --yes")


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DEFAULT_START,
    ) as dag:
        yield dag
