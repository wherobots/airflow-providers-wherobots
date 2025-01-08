"""
Define the Operators for triggering and monitoring the execution of Wherobots Run
"""

from enum import auto
from time import sleep
from typing import Optional, Sequence, Any, Dict

from airflow.models import BaseOperator
from strenum import StrEnum

from airflow_providers_wherobots.hooks.base import DEFAULT_CONN_ID
from airflow_providers_wherobots.hooks.rest_api import WherobotsRestAPIHook
from airflow_providers_wherobots.wherobots.models import (
    RUN_NAME_ALPHABET,
    RunStatus,
    Run,
)

from wherobots.db import Runtime
from wherobots.db.constants import DEFAULT_RUNTIME


class XComKey(StrEnum):
    run_id = auto()


class WherobotsRunOperator(BaseOperator):
    """
    Operator for triggering and monitoring the execution of Wherobots Run
    """

    template_fields: Sequence[str] = "run_payload"

    def __init__(
        self,
        name: Optional[str] = None,
        runtime: Runtime = DEFAULT_RUNTIME,
        run_python: Optional[Dict[str, Any]] = None,
        run_jar: Optional[Dict[str, Any]] = None,
        environment: Optional[Dict[str, Any]] = None,
        polling_interval: int = 20,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        poll_logs: bool = False,
        timeout_seconds: int = 3600,
        xcom_push: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # If the user specifies the name, we will use it and rely on the server to validate the name
        self.run_payload: Dict[str, Any] = {
            "runtime": runtime.value,
            "name": name or self.default_run_name,
            "timeoutSeconds": timeout_seconds,
        }
        if run_python:
            self.run_payload["runPython"] = run_python
        if run_jar:
            self.run_payload["runJar"] = run_jar
        if environment:
            self.run_payload["environment"] = environment
        self._polling_interval = polling_interval
        self.wherobots_conn_id = wherobots_conn_id
        self.xcom_push = xcom_push
        self.run_id: Optional[str] = None
        self.poll_logs = poll_logs
        self._logs_available = False

    @property
    def default_run_name(self) -> str:
        return (
            "".join(
                filter(
                    # This is not necessary as airflow's name convention is same as Wherobots Run, just for safety
                    lambda x: x in RUN_NAME_ALPHABET,
                    f"{self.dag_id}.{self.task_id}"[:200],
                )
            )
            + ".{{ ts_nodash }}"
        )

    def poll_and_display_logs(
        self, hook: WherobotsRestAPIHook, run: Run, start: int
    ) -> int:
        """
        Poll the logs and display them
        """
        log_resp = hook.get_run_logs(run.ext_id, start)
        if not self._logs_available:
            if log_resp.items:
                self._logs_available = True
                self.log.info("=== Logs for Run %s Start:", run.ext_id)
            else:
                self.log.info(
                    "Run %s status: %s, logs will start to stream once available",
                    run.ext_id,
                    run.status.value,
                )
        if not log_resp.items:
            return log_resp.current_page
        last_item = log_resp.items[-1]
        first_item = log_resp.items[0]
        if first_item.timestamp == start:
            # We don't repeatedly print a log item
            log_resp.items = log_resp.items[1:]
        for log_item in log_resp.items:
            self.log.info(f"Log: {log_item.raw}")
        return log_resp.next_page or last_item.timestamp

    def _log_run_status(self, run: Run):
        self.log.info(f"Run {run.ext_id} status: {run.status}")

    def _wait_run_poll_logs(self, hook: WherobotsRestAPIHook, run: Run):
        logs_cursor: int = 0
        while run.status == RunStatus.PENDING:
            sleep(self._polling_interval)
            run = hook.get_run(run.ext_id)
            self._log_run_status(run)
        while run.status == RunStatus.RUNNING:
            # Pull the run logs
            logs_cursor = self.poll_and_display_logs(hook, run, logs_cursor)
            sleep(self._polling_interval)
            run = hook.get_run(run.ext_id)
        # If logs_cursor is still not None after run is ended, there are still logs to pull, we will pull them all.
        while True:
            # Sleep 3 sec to avoid too frequent polling
            sleep(3)
            next_cursor = self.poll_and_display_logs(hook, run, logs_cursor)
            if next_cursor == logs_cursor:
                break
            else:
                logs_cursor = next_cursor
        self.log.info("=== Logs for Run %s End", run.ext_id)
        return run

    def _wait_run_simple(self, hook: WherobotsRestAPIHook, run: Run) -> Run:
        while run.status.is_active():
            sleep(self._polling_interval)
            run = hook.get_run(run.ext_id)
            self._log_run_status(run)
        return run

    def execute(self, context) -> Any:
        """
        Trigger the Wherobots Run and keep polling for status until the Run ends
        """
        with WherobotsRestAPIHook(self.wherobots_conn_id) as rest_api_hook:
            self.log.info(f"Creating Run with payload {self.run_payload}")
            run = rest_api_hook.create_run(self.run_payload)
            if self.xcom_push and context:
                context["ti"].xcom_push(key=XComKey.run_id, value=run.ext_id)
            self.run_id = run.ext_id
            self.log.info(f"Run {run.ext_id} created")
            # wait for the run ends
            if self.poll_logs:
                run = self._wait_run_poll_logs(rest_api_hook, run)
            else:
                run = self._wait_run_simple(rest_api_hook, run)
            # loop end, means run is in terminal state
            self._log_run_status(run)
            if run.status in (RunStatus.FAILED, RunStatus.CANCELLED):
                raise RuntimeError(
                    f"Run {run.ext_id} failed or cancelled by another party"
                )

    def on_kill(self) -> None:
        """
        Cancel the run if the task is killed
        """
        if self.run_id:
            with WherobotsRestAPIHook(self.wherobots_conn_id) as rest_api_hook:
                rest_api_hook.cancel_run(self.run_id)
                self.log.info(f"Run {self.run_id} is cancelled")
        else:
            self.log.error("Cancel failed due to missing run_id")
