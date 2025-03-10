"""
Test the operators in SQL module
"""

import datetime

import pendulum
import pytest
from airflow import DAG
from airflow.models import Connection
from wherobots.db import Region

from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator

DEFAULT_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DEFAULT_END = DEFAULT_START + datetime.timedelta(days=1)

TEST_DAG_ID = "test_sql_operator"
TEST_TASK_ID = "sql_operator"


@pytest.mark.usefixtures("clean_airflow_db")
def test_prod_run_success(prod_conn: Connection, dag: DAG) -> None:
    operator = WherobotsSqlOperator(
        region=Region.AWS_US_WEST_2,
        task_id=TEST_TASK_ID,
        sql="SELECT pickup_datetime FROM wherobots_pro_data.nyc_taxi.yellow_2009_2010 LIMIT 10",
        wherobots_conn_id=prod_conn.conn_id,
        dag=dag,
    )
    result = operator.execute(context={})
    assert result.size == 10
