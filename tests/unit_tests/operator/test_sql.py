"""
Test operators
"""

from unittest import mock
from unittest.mock import MagicMock

from wherobots.db import Runtime

from airflow_providers_wherobots.operators.sql import WherobotsSqlOperator


def mock_wherobots_db_connection():
    mock_connection = MagicMock()
    mock_cursor = MagicMock(rowcount=1)
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("1",)]
    return mock_connection


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
            runtime=Runtime.LARGE,
        )
        result = operator.execute(context={})
        print(result)
