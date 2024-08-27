"""
Test the operators in run module
"""

import datetime
import uuid
from typing import Any, Optional
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow import DAG
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from pytest_mock import MockerFixture, mocker

from airflow_providers_wherobots.operators.run import WherobotsRunOperator
from airflow_providers_wherobots.wherobots.models import (
    PythonRunPayload,
    RunStatus,
    CreateRunPayload,
)
from tests.helpers import run_factory


def execute_run_operator(
    create_operator_kwargs: dict[str, Any],
    start: datetime = datetime.datetime(2021, 9, 13, 0, 0, 0),
    dag_id: Optional[str] = None,
    task_id: Optional[str] = "test_run_operator",
):
    end = start + datetime.timedelta(days=1)
    if not dag_id:
        # generate a random dag name to avoid duplicate in the same test case
        dag_id = f"test_run_operator_{uuid.uuid4()}"
    with DAG(
        dag_id=dag_id,
        schedule="@daily",
        start_date=start,
    ) as dag:
        operator = WherobotsRunOperator(
            task_id=task_id,
            dag=dag,
            **create_operator_kwargs,
        )
        dag_run: DagRun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=start,
            data_interval=(start, end),
            start_date=start,
            run_type=DagRunType.MANUAL,
        )
        ti = dag_run.get_task_instance(task_id=task_id)
        ti.task = dag.get_task(task_id=task_id)
        ti.run(ignore_ti_state=True)
        return operator


class TestWherobotsRunOperator:

    @pytest.mark.usefixtures("clean_airflow_db")
    def test_render_template(self, mocker: MockerFixture):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        execute_run_operator(
            {
                "name": "test_run_{{ ds }}",
                "python": PythonRunPayload(
                    uri="s3://bucket/test-{{ ds }}.py",
                    args=["{{ ds }}"],
                    entrypoint="src.main_{{ ds }}",
                ),
            },
            start=data_interval_start,
            task_id="test_render_template_python",
        )
        assert create_run.call_count == 1
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, CreateRunPayload)
        expected_ds = data_interval_start.format("YYYY-MM-DD")
        assert rendered_payload.name == f"test_run_{expected_ds}"
        assert rendered_payload.python.uri == f"s3://bucket/test-{expected_ds}.py"
        assert rendered_payload.python.args == [expected_ds]
        assert rendered_payload.python.entrypoint == f"src.main_{expected_ds}"

    @pytest.mark.usefixtures("clean_airflow_db")
    def test_default_name(self, mocker: MockerFixture):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = execute_run_operator(
            {
                "python": PythonRunPayload(
                    uri="", args=[]
                ),
            },
            start=data_interval_start,
            task_id="run_operator",
            dag_id="test_default_name",
        )
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, CreateRunPayload)
        assert rendered_payload.name == operator.default_run_name.replace("{{ ts_nodash }}", data_interval_start.strftime("%Y%m%dT%H%M%S"))
