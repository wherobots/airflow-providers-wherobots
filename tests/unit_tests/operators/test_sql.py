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
        operator.execute(context={})

    @mock.patch(
        "airflow_providers_wherobots.hooks.sql.connect",
        return_value=mock_wherobots_db_connection(),
    )
    def test_runtime_version_passed(self, mock_connect):
        operator = WherobotsSqlOperator(
            task_id="test_task_version",
            sql="select * from table_a",
            runtime=Runtime.LARGE,
            version="2.0.0-preview",
        )
        operator.execute(context={})
        # Check that connect() was called with version="2.0.0-preview"
        found = False
        for call in mock_connect.call_args_list:
            if call.kwargs.get("version") == "2.0.0-preview":
                found = True
        assert found, "connect() was not called with the correct version param"
