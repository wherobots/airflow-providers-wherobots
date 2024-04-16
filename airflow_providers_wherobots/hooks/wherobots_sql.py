"""
Constants
"""

import logging
from typing import Any
from functools import cached_property

import wherobots.db
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from wherobots.db import Runtime
from wherobots.db.constants import (
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
)

from airflow_providers_wherobots.hooks.wherobots import DEFAULT_CONN_ID


log = logging.getLogger(__name__)


class WherobotsSqlHook(BaseHook):  # type: ignore[misc]
    def __init__(
        self,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        session_wait_timeout: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
        read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
    ):
        super().__init__()
        self.wherobots_conn_id = wherobots_conn_id
        self.session_wait_timeout = session_wait_timeout
        self.read_timeout = read_timeout
        self._conn = self.get_connection(self.wherobots_conn_id)
        self._db_conn = None

    def get_conn(self) -> wherobots.db.Connection:
        return self._db_conn

    def create_sql_session(
        self, runtime: Runtime = Runtime.SEDONA
    ) -> wherobots.db.Connection:
        self._db_conn = wherobots.db.connect(
            host=self._conn.host,
            api_key=self._conn.get_password(),
            runtime=runtime,
            wait_timeout=self.session_wait_timeout,
            read_timeout=self.read_timeout,
        )
        return self._db_conn
