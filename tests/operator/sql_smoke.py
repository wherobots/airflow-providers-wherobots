"""
Test operators
"""

import argparse
import os

from airflow.models import Connection
from wherobots.db import Runtime

from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", help="API key")
    parser.add_argument("sql", help="SQL query to execute")
    args = parser.parse_args()
    test_conn = Connection(
        conn_type="http", host="api.staging.wherobots.services", password=args.api_key
    )
    test_conn_uri = test_conn.get_uri()
    os.environ["AIRFLOW_CONN_TEST_CONN"] = test_conn_uri
    operator = WherobotsSqlOperator(
        task_id="test_task",
        sql=args.sql,
        wherobots_conn_id="test_conn",
        runtime_id=Runtime.SEDONA,
    )
    result = operator.execute(context={})
    print(result)
