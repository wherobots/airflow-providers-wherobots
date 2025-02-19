"""
Test the operators in run module
"""

import datetime

import pendulum
import pytest
from airflow import DAG
from airflow.models import Connection
from airflow.utils.state import TaskInstanceState
from wherobots.db import Region

from airflow_providers_wherobots.operators.run import WherobotsRunOperator
from tests.unit_tests.operator.test_run import build_ti

DEFAULT_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DEFAULT_END = DEFAULT_START + datetime.timedelta(days=1)

TEST_DAG_ID = "test_run_operator"
TEST_TASK_ID = "run_operator"


@pytest.mark.usefixtures("clean_airflow_db")
def test_prod_run_success(prod_conn: Connection, dag: DAG) -> None:
    operator = WherobotsRunOperator(
        region=Region.AWS_US_WEST_2,
        wherobots_conn_id=prod_conn.conn_id,
        task_id="test_run_smoke",
        name="airflow_operator_test_run_{{ ts_nodash }}",
        run_python={
            "uri": "s3://wbts-wbc-m97rcg45xi/42ly7mi0p1/data/shared/very_simple_job.py"
        },
        dag=dag,
        do_xcom_push=True,
    )
    ti = build_ti(dag, task_id=operator.task_id)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
