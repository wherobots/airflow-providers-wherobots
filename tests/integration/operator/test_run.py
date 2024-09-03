"""
Test the operators in run module
"""

import datetime

import pendulum
import pytest
from airflow import DAG
from airflow.models import Connection

from airflow_providers_wherobots.operators.run import WherobotsRunOperator
from airflow_providers_wherobots.wherobots.models import (
    PythonRunPayload,
)
from tests.unit.operator.test_run import dag, execute_dag

DEFAULT_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DEFAULT_END = DEFAULT_START + datetime.timedelta(days=1)

TEST_DAG_ID = "test_run_operator"
TEST_TASK_ID = "run_operator"


@pytest.mark.usefixtures("clean_airflow_db")
def test_staging_run_success(staging_conn: Connection, dag: DAG) -> None:
    operator = WherobotsRunOperator(
        wherobots_conn_id=staging_conn.conn_id,
        task_id="test_run_smoke",
        name="airflow_operator_test_run_{{ ts_nodash }}",
        python=PythonRunPayload(
            uri="s3://wbts-wbc-rcv7vl73oy/hao9o6y8ci/data/customer-z4asgjn7clrcbz/very_simple_job.py"
        ),
        dag=dag,
    )
    execute_dag(dag, task_id=operator.task_id)