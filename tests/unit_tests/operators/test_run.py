"""
Test the operators in run module
"""

import datetime
import itertools
import logging
import uuid
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow import DAG
from pytest_mock import MockerFixture
from wherobots.db import Region

from airflow_providers_wherobots.operators import run
from airflow_providers_wherobots.operators.run import WherobotsRunOperator, XComKey
from airflow_providers_wherobots.wherobots.models import (
    RunStatus,
    LogsResponse,
    Run,
    LogItem,
    LogProcessor,
)
from tests.unit_tests import helpers
from tests.unit_tests.helpers import run_factory

DEFAULT_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DEFAULT_END = DEFAULT_START + datetime.timedelta(days=1)

TEST_DAG_ID = "test_run_operator"
TEST_TASK_ID = "run_operator"


def execute_operator(
    operator: WherobotsRunOperator, logical_date=DEFAULT_START
) -> Dict[str, Any]:
    """Render templates and execute the operator in-process, without a metadata DB.

    Uses only operator methods shared by Airflow 2 and the Airflow 3 Task SDK,
    so the same tests run on every supported Airflow version.
    """
    context: Dict[str, Any] = {
        "ds": logical_date.format("YYYY-MM-DD"),
        "ts_nodash": logical_date.strftime("%Y%m%dT%H%M%S"),
        "ti": MagicMock(),
    }
    operator.render_template_fields(context)
    operator.execute(context)
    return context


class TestWherobotsRunOperator:
    def test_render_template(self, mocker: MockerFixture, dag: DAG):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = WherobotsRunOperator(
            region=Region.AWS_US_WEST_2,
            task_id="test_render_template_python",
            name="test_run_{{ ds }}",
            run_python={
                "uri": "s3://bucket/test-{{ ds }}.py",
                "args": ["{{ ds }}"],
            },
            dag=dag,
        )
        assert operator.region == Region.AWS_US_WEST_2
        execute_operator(operator)
        assert create_run.call_count == 1
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, dict)
        expected_ds = data_interval_start.format("YYYY-MM-DD")
        assert rendered_payload["name"] == f"test_run_{expected_ds}"
        assert (
            rendered_payload["runPython"]["uri"] == f"s3://bucket/test-{expected_ds}.py"
        )
        assert rendered_payload["runPython"]["args"] == [expected_ds]

    def test_default_name(self, mocker: MockerFixture, dag: DAG):
        data_interval_start = pendulum.datetime(2021, 9, 13, tz="UTC")
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = WherobotsRunOperator(
            task_id="test_default_name",
            run_python={"uri": ""},
            dag=dag,
        )
        execute_operator(operator)
        rendered_payload = create_run.call_args.args[0]
        assert isinstance(rendered_payload, dict)
        assert rendered_payload["name"] == operator.default_run_name.replace(
            "{{ ts_nodash }}", data_interval_start.strftime("%Y%m%dT%H%M%S")
        )

    @pytest.mark.parametrize(
        "poll_logs,test_item",
        list(
            itertools.product(
                [False, True],
                [
                    (
                        [
                            run_factory.build(status=RunStatus.RUNNING),
                            run_factory.build(status=RunStatus.FAILED),
                        ],
                        False,
                    ),
                    (
                        [run_factory.build(status=RunStatus.CANCELLED)],
                        False,
                    ),
                    (
                        [
                            run_factory.build(status=RunStatus.RUNNING),
                            run_factory.build(status=RunStatus.TIMED_OUT),
                        ],
                        False,
                    ),
                    (
                        [
                            run_factory.build(status=RunStatus.RUNNING),
                            run_factory.build(status=RunStatus.COMPLETED),
                        ],
                        True,
                    ),
                ],
            )
        ),
    )
    def test_execute_handle_states(
        self,
        mocker: MockerFixture,
        dag: DAG,
        poll_logs: bool,
        test_item: Tuple[List[Run], bool],
    ):
        get_run_results, succeeds = test_item
        mocked_create_run = mocker.patch(
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
            run_python={"uri": ""},
            dag=dag,
            polling_interval=0,
            poll_logs=poll_logs,
            do_xcom_push=True,
            wait_post_run_logs_timeout_seconds=1,
        )
        mocker.patch.object(run, "POST_RUN_POLL_LOGS_INTERVAL", 0.1)
        if succeeds:
            context = execute_operator(operator)
            context["ti"].xcom_push.assert_called_once_with(
                key=XComKey.run_id, value=mocked_create_run.return_value.ext_id
            )
        else:
            with pytest.raises(RuntimeError):
                execute_operator(operator)

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
            run_python={
                "uri": "s3://bucket/test-{{ ds }}.py",
                "args": ["{{ ds }}"],
                "entrypoint": "src.main_{{ ds }}",
            },
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
            run_python={"uri": ""},
            dag=DAG("test_poll_and_display_logs"),
        )
        assert operator.poll_and_display_logs(hook, test_run, 0) == 2
        hook.get_run_logs.assert_called_with(test_run.ext_id, 0)

    def test_wait_run_poll_logs(self, mocker: MockerFixture):
        hook = mocker.MagicMock()
        test_run: Run = helpers.run_factory.build()
        operator = WherobotsRunOperator(
            task_id="test_poll_and_display_logs",
            run_python={"uri": ""},
            dag=DAG("test_poll_and_display_logs"),
            wait_post_run_logs_timeout_seconds=1,
        )
        run.POST_RUN_POLL_LOGS_INTERVAL = 0.1
        mocked_poll_method = mocker.patch.object(
            operator, "poll_and_display_logs", side_effect=[2] * 20
        )
        operator._tail_post_run_logs(hook, test_run, 1)
        # The polling should stops before iterate on all the 20 side effects as the next_cursor is not increasing for more than the timeout
        assert mocked_poll_method.call_count < 20
        # The polling should not stop before 30 as the next_cursor increase for 30 rounds
        mocked_poll_method = mocker.patch.object(
            operator,
            "poll_and_display_logs",
            side_effect=list(range(0, 30)) + [30] * 20,
        )
        operator._tail_post_run_logs(hook, test_run, 1)
        assert mocked_poll_method.call_count > 30
        operator._tail_post_run_logs(hook, test_run, 1)

    def test_runtime_version_in_payload(self, mocker: MockerFixture, dag: DAG):
        create_run: MagicMock = mocker.patch(
            "airflow_providers_wherobots.hooks.rest_api.WherobotsRestAPIHook.create_run",
            return_value=run_factory.build(status=RunStatus.COMPLETED),
        )
        operator = WherobotsRunOperator(
            region=Region.AWS_US_WEST_2,
            task_id="test_runtime_version",
            name="test_run",
            run_python={"uri": "s3://bucket/test.py"},
            version="1.2.3",
            dag=dag,
        )
        execute_operator(operator)
        rendered_payload = create_run.call_args.args[0]
        assert rendered_payload["version"] == "1.2.3"


class TestStreamLogProcessor:
    @pytest.fixture(scope="class")
    @staticmethod
    def stream_log_processor():
        """
        Create a single StreamLogProcessor instance for all tests in this class.
        """
        return LogProcessor()

    @pytest.mark.parametrize(
        "line, expected_level",
        [
            ("INFO warn", logging.INFO),
            ("WARN", logging.WARN),
            ("DEBUG", logging.DEBUG),
            ("ERROR", logging.ERROR),
            ("CRITICAL", logging.CRITICAL),
            ("random stuff here without level", logging.INFO),
            (
                "py4j.protocol.Py4JJavaError: An error occurred while calling",
                logging.ERROR,
            ),
            ("shouldn't be error 1", logging.INFO),
            ("shouldn't be error 2", logging.INFO),
            ("shouldn't be error 3", logging.INFO),
            ("shouldn't be error", logging.INFO),
        ],
    )
    def test_process_line_detects_levels(
        self, stream_log_processor, line, expected_level
    ):
        """
        Ensure StreamLogProcessor maps log lines to the expected logging level.
        """
        assert stream_log_processor.process_line(line) == expected_level
