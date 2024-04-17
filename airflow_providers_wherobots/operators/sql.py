"""
Operator for firing sql queries and collect results to s3
"""

from __future__ import annotations

from typing import Any

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from wherobots.db import Runtime
from wherobots.db.constants import (
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
)
from wherobots.db import Cursor as WDbCursor

from airflow_providers_wherobots.hooks.wherobots import DEFAULT_CONN_ID
from airflow_providers_wherobots.hooks.sql import WherobotsSqlHook


class WherobotsSqlOperator(SQLExecuteQueryOperator):  # type: ignore[misc]

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        runtime_id: Runtime = Runtime.SEDONA,
        session_wait_timeout: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
        read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(conn_id=wherobots_conn_id, **kwargs)
        self.wherobots_conn_id = wherobots_conn_id
        self.runtime_id = runtime_id
        self.session_wait_timeout = session_wait_timeout
        self.read_timeout = read_timeout

    def get_db_hook(self) -> DbApiHook:
        return WherobotsSqlHook(
            wherobots_conn_id=self.wherobots_conn_id,
            runtime_id=self.runtime_id,
            session_wait_timeout=self.session_wait_timeout,
            read_timeout=self.read_timeout,
        )
