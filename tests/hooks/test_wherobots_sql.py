"""
Test hooks
"""

from unittest import mock


from airflow.models import Connection
from wherobots.db import Runtime

from airflow_providers_wherobots.hooks.wherobots_sql import WherobotsSqlHook

test_host = "localhost:3000"
test_conn = Connection(conn_type="http", host=test_host, password="token")
test_conn_uri = test_conn.get_uri()


@mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=test_conn_uri)
class TestWherobotsSqlHook:

    @mock.patch("airflow_providers_wherobots.hooks.wherobots_sql.wherobots.db.connect")
    def test_post(self, mock_connect):
        # Instantiate hook
        hook = WherobotsSqlHook(wherobots_conn_id="test_conn")

        # Sample Hook's run method executes an API call
        hook.create_sql_session(runtime=Runtime.ATLANTIS)
        mock_connect.assert_called_once_with(
            host=test_host,
            api_key="token",
            runtime=Runtime.ATLANTIS,
            wait_timeout=hook.session_wait_timeout,
            read_timeout=hook.read_timeout,
        )
