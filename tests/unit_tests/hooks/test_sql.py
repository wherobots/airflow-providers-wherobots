"""
Test hooks
"""

from unittest import mock
from unittest.mock import MagicMock

from airflow.models import Connection
from pytest_mock import MockerFixture
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
from wherobots.db.session_type import SessionType

from airflow_providers_wherobots.client_attribution import (
    CLIENT_HOP,
    WHEROBOTS_CLIENT_HEADER,
)
from airflow_providers_wherobots.hooks.sql import WherobotsSqlHook


class TestWherobotsSqlHook:
    @mock.patch("airflow_providers_wherobots.hooks.sql.connect")
    def test_get_conn(
        self,
        mock_connect: MagicMock,
        test_default_conn: Connection,
        mocker: MockerFixture,
    ):
        # The exact-kwargs assertion below omits `extra_headers`, so the probe
        # is pinned rather than inherited from whatever driver dependency
        # resolution happened to install. Attribution has its own two tests;
        # this one is about the connection arguments.
        mocker.patch(
            "airflow_providers_wherobots.hooks.sql._CONNECT_SUPPORTS_EXTRA_HEADERS",
            False,
        )

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

    @mock.patch("airflow_providers_wherobots.hooks.sql.connect")
    def test_get_conn_passes_client_attribution_to_the_driver(
        self,
        mock_connect: MagicMock,
        test_default_conn: Connection,
        mocker: MockerFixture,
    ):
        """Our hop goes to the driver, which appends its own hop to the right."""
        mocker.patch(
            "airflow_providers_wherobots.hooks.sql._CONNECT_SUPPORTS_EXTRA_HEADERS",
            True,
        )
        WherobotsSqlHook().get_conn()
        assert mock_connect.call_args.kwargs["extra_headers"] == {
            WHEROBOTS_CLIENT_HEADER: CLIENT_HOP
        }

    @mock.patch("airflow_providers_wherobots.hooks.sql.connect")
    def test_get_conn_omits_client_attribution_on_older_drivers(
        self,
        mock_connect: MagicMock,
        test_default_conn: Connection,
        mocker: MockerFixture,
    ):
        """A driver without `extra_headers` still connects, just unattributed."""
        mocker.patch(
            "airflow_providers_wherobots.hooks.sql._CONNECT_SUPPORTS_EXTRA_HEADERS",
            False,
        )
        WherobotsSqlHook().get_conn()
        assert "extra_headers" not in mock_connect.call_args.kwargs
