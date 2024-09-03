"""
Define the Operators for triggering and monitoring the execution of Wherobots Run
"""

from enum import auto, Enum
from time import sleep
from typing import Optional, Sequence, Any

from airflow.models import BaseOperator
from wherobots.db import Runtime

from airflow_providers_wherobots.hooks.base import DEFAULT_CONN_ID
from airflow_providers_wherobots.hooks.rest_api import WherobotsRestAPIHook
from airflow_providers_wherobots.wherobots.models import (
    PythonRunPayload,
    JavaRunPayload,
    CreateRunPayload,
    RUN_NAME_ALPHABET,
)


class XComKey(str, Enum):
    run_id = auto()


class WherobotsRunOperator(BaseOperator):
    """
    Operator for triggering and monitoring the execution of Wherobots Run
    """

    template_fields: Sequence[str] = "run_payload"

    def __init__(
        self,
        name: Optional[str] = None,
        runtime: Optional[Runtime] = Runtime.SEDONA,
        python: Optional[PythonRunPayload] = None,
        java: Optional[JavaRunPayload] = None,
        polling_interval: int = 30,
        wherobots_conn_id: str = DEFAULT_CONN_ID,
        xcom_push: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # If the user specifies the name, we will use it and rely on the server to validate the name
        self.run_payload = CreateRunPayload(
            runtime=runtime,
            name=name or self.default_run_name,
            python=python,
            java=java,
        )
        self.polling_interval = polling_interval
        self.wherobots_conn_id = wherobots_conn_id
        self.xcom_push = xcom_push
        self.run_id: Optional[str] = None

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
            while run.status.is_active():
                sleep(self.polling_interval)
                run = rest_api_hook.get_run(run.ext_id)
                self.log.info(f"Run {run.ext_id} status: {run.status}")
            # loop end, means run is in terminal state
            self.log.info(f"Run {run.ext_id} is {run.status}")

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
