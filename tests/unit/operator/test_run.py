"""
Test the operators in run module
"""

import datetime
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow import DAG
from airflow.models import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from pytest_mock import MockerFixture

from airflow_providers_wherobots.operators.run import WherobotsRunOperator
from airflow_providers_wherobots.wherobots.models import (
    PythonRunPayload,
    RunStatus,
    CreateRunPayload,
)
from tests.unit.helpers import run_factory

DEFAULT_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DEFAULT_END = DEFAULT_START + datetime.timedelta(days=1)

TEST_DAG_ID = "test_run_operator"
TEST_TASK_ID = "run_operator"


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DEFAULT_START,
    ) as dag:
        yield dag


def execute_dag(dag: DAG, task_id: str, start=DEFAULT_START, end=DEFAULT_END):
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
    return ti


class TestWherobotsRunOperator:

    @pytest.mark.usefixtures("clean_airflow_db")
    def test_render_template(self, mocker: MockerFixture, dag: DAG):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = WherobotsRunOperator(
            task_id="test_render_template_python",
            name="test_run_{{ ds }}",
            python=PythonRunPayload(
                uri="s3://bucket/test-{{ ds }}.py",
                args=["{{ ds }}"],
                entrypoint="src.main_{{ ds }}",
            ),
            dag=dag,
        )
        execute_dag(dag, task_id=operator.task_id)
        assert create_run.call_count == 1
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, CreateRunPayload)
        expected_ds = data_interval_start.format("YYYY-MM-DD")
        assert rendered_payload.name == f"test_run_{expected_ds}"
        assert rendered_payload.python.uri == f"s3://bucket/test-{expected_ds}.py"
        assert rendered_payload.python.args == [expected_ds]
        assert rendered_payload.python.entrypoint == f"src.main_{expected_ds}"

    @pytest.mark.usefixtures("clean_airflow_db")
    def test_default_name(self, mocker: MockerFixture, dag: DAG):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = WherobotsRunOperator(
            task_id="test_default_name",
            python=PythonRunPayload(uri=""),
            dag=dag,
        )
        execute_dag(dag, task_id=operator.task_id)
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, CreateRunPayload)
        assert rendered_payload.name == operator.default_run_name.replace("{{ ts_nodash }}", data_interval_start.strftime("%Y%m%dT%H%M%S"))
