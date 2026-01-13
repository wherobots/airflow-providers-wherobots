"""
Hook for Wherobots' Spatial SQL API interface.
"""

from typing import Optional

from airflow.providers.common.sql.hooks.sql import DbApiHook
from wherobots.db import Connection as WDBConnection, connect
from wherobots.db.constants import (
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_RUNTIME,
    DEFAULT_SESSION_TYPE,
)
from wherobots.db.region import Region
from wherobots.db.runtime import Runtime
from wherobots.db.session_type import SessionType

from airflow_providers_wherobots.hooks.base import DEFAULT_CONN_ID


class WherobotsSqlHook(DbApiHook):  # type: ignore[misc]
    conn_name_attr = "wherobots_conn_id"

    def __init__(  # type: ignore[no-untyped-def]
        self,
        region: Optional[Region] = None,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        runtime: Runtime = DEFAULT_RUNTIME,
        version: Optional[str] = None,
        session_wait_timeout: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
        read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
        force_new: bool = False,
        session_type: SessionType = DEFAULT_SESSION_TYPE,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.wherobots_conn_id = wherobots_conn_id
        self.runtime = runtime
        self.version = version
        self.session_wait_timeout = session_wait_timeout
        self.read_timeout = read_timeout
        self.region = region
        self.force_new = force_new
        self.session_type = session_type

        self._conn = self.get_connection(self.wherobots_conn_id)
        self._db_conn: Optional[WDBConnection] = None

    def _create_or_get_sql_session(
        self,
        runtime: Runtime = DEFAULT_RUNTIME,
    ) -> WDBConnection:
        return connect(
            host=self._conn.host,
            api_key=self._conn.password,
            runtime=runtime,
            region=self.region,
            version=self.version,
            wait_timeout=self.session_wait_timeout,
            read_timeout=self.read_timeout,
            force_new=self.force_new,
            session_type=self.session_type,
        )

    def get_conn(self) -> WDBConnection:
        if not self._db_conn:
            self._db_conn = self._create_or_get_sql_session(self.runtime)
        return self._db_conn

    def get_autocommit(self, conn: WDBConnection) -> bool:
        return True
