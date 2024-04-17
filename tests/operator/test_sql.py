"""
Test operators
"""

from unittest import mock
from unittest.mock import MagicMock

from airflow.models import Connection
from wherobots.db import Runtime

from airflow_providers_wherobots.hooks.sql import WherobotsSqlHook
from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator

test_host = "localhost:3000"
test_conn = Connection(conn_type="http", host=test_host, password="token")
test_conn_uri = test_conn.get_uri()


def mock_wherobots_db_connection():
    mock_connection = MagicMock()
    mock_cursor = MagicMock(rowcount=1)
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("1",)]
    return mock_connection


@mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=test_conn_uri)
class TestWherobotsSqlOperator:

    @mock.patch(
        "airflow_providers_wherobots.hooks.sql.connect",
        return_value=mock_wherobots_db_connection(),
    )
    def test_default_handler(self, mock_connect):
        # Instantiate hook
        operator = WherobotsSqlOperator(
            task_id="test_task",
            sql="select * from table_a",
            wherobots_conn_id="test_conn",
            runtime_id=Runtime.ATLANTIS,
        )
        result = operator.execute(context={})
        print(result)
