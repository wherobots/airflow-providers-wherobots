"""
Hook for Wherobots' HTTP API
"""

import platform
from functools import cached_property
from typing import Any, Optional, Dict

import requests
from importlib import metadata
from airflow.version import version as airflow_version
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter, Retry
from requests.auth import AuthBase

from airflow_providers_wherobots.hooks.base import (
    DEFAULT_CONN_ID,
    PACKAGE_NAME,
)
from airflow_providers_wherobots.wherobots.models import (
    Run,
    LogsResponse,
)


class WherobotsAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, r: PreparedRequest):
        if self.api_key:
            r.headers["X-API-Key"] = self.api_key
        return r


class WherobotsRestAPIHook(BaseHook):
    conn_name_attr = "wherobots_conn_id"
    default_conn_name = DEFAULT_CONN_ID

    def __init__(
        self,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        retry_limit: int = 3,
        retry_min_delay: float = 1.0,
    ):
        super().__init__()
        self.wherobots_conn_id = wherobots_conn_id
        self.retry_limit = retry_limit
        self.retry_min_delay = retry_min_delay
        self.session = requests.Session()
        retries = Retry(
            total=self.retry_limit,
            backoff_factor=self.retry_min_delay,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @cached_property
    def conn(self) -> Connection:
        return self.get_connection(self.wherobots_conn_id)

    @cached_property
    def user_agent_header(self):
        try:
            package_version = metadata.version(PACKAGE_NAME)
        except metadata.PackageNotFoundError:
            package_version = "unknown"
        python_version = platform.python_version()
        system = platform.system().lower()
        header_value = (
            f"{PACKAGE_NAME}/{package_version} os/{system}"
            f" python/{python_version} airflow/{airflow_version}"
        )
        return {"User-Agent": header_value}

    def _api_call(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        auth = WherobotsAuth(self.conn.password)
        url = "https://" + self.conn.host.rstrip("/") + endpoint
        resp = self.session.request(
            url=url,
            method=method,
            json=payload,
            auth=auth,
            params=params,
            headers=self.user_agent_header,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"HTTP error: {e} with response: {resp.text}") from e
        return resp

    def get_run(self, run_id: str) -> Run:
        resp_json = self._api_call("GET", f"/runs/{run_id}").json()
        return Run.model_validate(resp_json)

    def create_run(self, payload: Dict[str, Any]) -> Run:
        resp_json = self._api_call(
            "POST",
            "/runs",
            payload=payload,
        ).json()
        return Run.model_validate(resp_json)

    def cancel_run(self, run_id: str) -> None:
        self._api_call("POST", f"/runs/{run_id}/cancel")

    def get_run_logs(self, run_id: str, start: int, size: int = 500) -> LogsResponse:
        params = {"cursor": start, "size": size}
        resp_json = self._api_call("GET", f"/runs/{run_id}/logs", params=params).json()
        return LogsResponse.model_validate(resp_json)
