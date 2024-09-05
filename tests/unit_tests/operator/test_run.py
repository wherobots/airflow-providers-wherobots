"""
Test the operators in run module
"""

import datetime
import itertools
import uuid
from typing import Tuple
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
from pytest_mock import MockerFixture

from airflow_providers_wherobots.operators.run import WherobotsRunOperator
from airflow_providers_wherobots.wherobots.models import (
    PythonRunPayload,
    RunStatus,
    CreateRunPayload,
    LogsResponse,
    Run,
    LogItem,
)
from tests.unit_tests import helpers
from tests.unit_tests.helpers import run_factory

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


def build_ti(dag: DAG, task_id: str, start=DEFAULT_START, end=DEFAULT_END):
    dag_run: DagRun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=start,
        data_interval=(start, end),
        start_date=start,
        run_type=DagRunType.MANUAL,
    )
    ti: TaskInstance = dag_run.get_task_instance(task_id=task_id)
    ti.task = dag.get_task(task_id=task_id)
    return ti


def execute_dag(dag: DAG, task_id: str, start=DEFAULT_START, end=DEFAULT_END):
    ti = build_ti(dag, task_id, start=start, end=end)
    ti.run(ignore_ti_state=True)


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
        assert rendered_payload.name == operator.default_run_name.replace(
            "{{ ts_nodash }}", data_interval_start.strftime("%Y%m%dT%H%M%S")
        )

    @pytest.mark.usefixtures("clean_airflow_db")
    @pytest.mark.parametrize(
        "poll_logs,test_item",
        itertools.product(
            [False, True],
            [
                (
                    [
                        run_factory.build(status=RunStatus.RUNNING),
                        run_factory.build(status=RunStatus.FAILED),
                    ],
                    TaskInstanceState.FAILED,
                ),
                (
                    [run_factory.build(status=RunStatus.CANCELLED)],
                    TaskInstanceState.FAILED,
                ),
                (
                    [
                        run_factory.build(status=RunStatus.RUNNING),
                        run_factory.build(status=RunStatus.COMPLETED),
                    ],
                    TaskInstanceState.SUCCESS,
                ),
            ],
        ),
    )
    def test_execute_handle_states(
        self,
        mocker: MockerFixture,
        dag: DAG,
        poll_logs: bool,
        test_item: Tuple[list[Run], TaskInstanceState],
    ):
        get_run_results, task_state = test_item
        mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.PENDING),
        )
        mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.get_run",
            side_effect=get_run_results,
        )
        if poll_logs:
            mocker.patch(
                "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.get_run_logs",
                return_value=LogsResponse(items=[], current_page=0, next_page=None),
            )
        operator = WherobotsRunOperator(
            task_id=f"test_execute_{uuid.uuid4()}",
            python=PythonRunPayload(uri=""),
            dag=dag,
            polling_interval=0,
            poll_logs=poll_logs,
        )
        ti = build_ti(dag, task_id=operator.task_id)
        try:
            ti.run(ignore_ti_state=True)
        except Exception as e:
            assert isinstance(e, RuntimeError)
        assert ti.state == task_state

    def test_on_kill(
        self,
        dag: DAG,
        mocker: MockerFixture,
    ):
        mocked_cancel_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.cancel_run"
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
        operator.on_kill()
        assert mocked_cancel_run.call_count == 0
        operator.run_id = "test_run_id"
        operator.on_kill()
        mocked_cancel_run.assert_called_with(operator.run_id)

    def test_poll_and_display_logs(self, mocker: MockerFixture):
        hook = mocker.MagicMock()
        test_run: Run = helpers.run_factory.build()
        hook.get_run_logs.return_value = LogsResponse(
            items=[LogItem(raw="log1", timestamp=1), LogItem(raw="log2", timestamp=2)],
            current_page=1,
            next_page=2,
        )
        operator = WherobotsRunOperator(
            task_id="test_poll_and_display_logs",
            python=PythonRunPayload(uri=""),
            dag=DAG("test_poll_and_display_logs"),
        )
        assert operator.poll_and_display_logs(hook, test_run, 0) == 2
        hook.get_run_logs.assert_called_with(test_run.ext_id, 0)