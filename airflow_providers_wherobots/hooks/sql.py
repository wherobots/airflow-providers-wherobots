"""
Constants
"""

from __future__ import annotations

import logging
from typing import Optional

import wherobots.db
from airflow.providers.common.sql.hooks.sql import DbApiHook
from wherobots.db import Connection as WDBConnection
from wherobots.db import Runtime, connect
from wherobots.db.constants import (
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
)

from airflow_providers_wherobots.hooks.base import DEFAULT_CONN_ID

log = logging.getLogger(__name__)


class WherobotsSqlHook(DbApiHook):  # type: ignore[misc]
    conn_name_attr = "wherobots_conn_id"

    def __init__(  # type: ignore[no-untyped-def]
        self,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        runtime_id: Runtime = Runtime.SEDONA,
        session_wait_timeout: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
        read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.wherobots_conn_id = wherobots_conn_id
        self.session_wait_timeout = session_wait_timeout
        self.read_timeout = read_timeout
        self.runtime_id = runtime_id
        self._conn = self.get_connection(self.wherobots_conn_id)
        self._db_conn: Optional[wherobots.db.Connection] = None

    def _create_or_get_sql_session(
        self, runtime: Runtime = Runtime.SEDONA
    ) -> WDBConnection:
        return connect(
            host=self._conn.host,
            api_key=self._conn.get_password(),
            runtime=runtime,
            wait_timeout=self.session_wait_timeout,
            read_timeout=self.read_timeout,
        )

    def get_conn(self) -> WDBConnection:
        if not self._db_conn:
            self._db_conn = self._create_or_get_sql_session(self.runtime_id)
        return self._db_conn

    def get_autocommit(self, conn: WDBConnection) -> bool:
        return True
