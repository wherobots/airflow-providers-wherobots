"""
Operator for firing sql queries and collect results to s3
"""

from __future__ import annotations

from typing import Sequence, Optional

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from wherobots.db import Runtime, Region
from wherobots.db.constants import (
    DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_RUNTIME,
)
from wherobots.db import Cursor as WDbCursor
from pandas.core.frame import DataFrame

from airflow_providers_wherobots.hooks.sql import WherobotsSqlHook
from airflow_providers_wherobots.hooks.base import DEFAULT_CONN_ID
from airflow_providers_wherobots.operators import warn_for_default_region


def wherobots_default_handler(cursor: WDbCursor) -> DataFrame | None:
    """Return results for DbApiHook.run()."""
    if not hasattr(cursor, "description"):
        raise RuntimeError(
            "The database we interact with does not support DBAPI 2.0. Use operator and "
            "handlers that are specifically designed for your database."
        )
    return cursor.fetchall()


class WherobotsSqlOperator(SQLExecuteQueryOperator):  # type: ignore[misc]
    template_fields: Sequence[str] = SQLExecuteQueryOperator.template_fields
    template_fields_renderers = {"sql": "sql"}
    conn_id_field = "wherobots_conn_id"

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *,
        region: Optional[Region] = None,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        runtime: Runtime = DEFAULT_RUNTIME,
        version: Optional[str] = None,
        session_wait_timeout: int = DEFAULT_SESSION_WAIT_TIMEOUT_SECONDS,
        read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
        **kwargs,
    ):
        super().__init__(
            conn_id=wherobots_conn_id, handler=wherobots_default_handler, **kwargs
        )
        self.wherobots_conn_id = wherobots_conn_id
        self.runtime = runtime
        self.version = version
        self.session_wait_timeout = session_wait_timeout
        self.read_timeout = read_timeout
        self.region = region

    def get_db_hook(self) -> DbApiHook:
        self.region = warn_for_default_region(self.region)
        return WherobotsSqlHook(
            region=self.region,
            wherobots_conn_id=self.wherobots_conn_id,
            runtime=self.runtime,
            version=self.version,
            session_wait_timeout=self.session_wait_timeout,
            read_timeout=self.read_timeout,
        )
