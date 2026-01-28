"""
Test hooks
"""

from unittest import mock
from unittest.mock import MagicMock

from airflow.models import Connection
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
from wherobots.db.session_type import SessionType

from airflow_providers_wherobots.hooks.sql import WherobotsSqlHook


class TestWherobotsSqlHook:
    @mock.patch("airflow_providers_wherobots.hooks.sql.connect")
    def test_get_conn(self, mock_connect: MagicMock, test_default_conn: Connection):
        # Instantiate hook
        hook = WherobotsSqlHook(
            runtime=Runtime.LARGE,
            region=Region.AWS_US_WEST_2,
            force_new=True,
            session_type=SessionType.SINGLE,
        )

        # Sample Hook's run method executes an API call
        hook.get_conn()
        mock_connect.assert_called_once_with(
            host=test_default_conn.host,
            api_key="token",
            runtime=Runtime.LARGE,
            region=Region.AWS_US_WEST_2,
            version=None,
            wait_timeout=hook.session_wait_timeout,
            read_timeout=hook.read_timeout,
            force_new=True,
            session_type=SessionType.SINGLE,
        )
