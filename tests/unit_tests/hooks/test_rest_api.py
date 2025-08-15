"""
Test the REST API hooks.
"""

import json
from http import HTTPStatus

import airflow
import requests
import responses
from pydantic import BaseModel
from pytest_mock import MockerFixture
from responses import matchers
from wherobots.db import Runtime, Region

from airflow_providers_wherobots.hooks.rest_api import (
    WherobotsAuth,
    WherobotsRestAPIHook,
)
from airflow_providers_wherobots.wherobots.models import (
    Run,
    LogsResponse,
)
from tests.unit_tests import helpers


@responses.activate
def test_wherobots_auth() -> None:
    """
    Test the WherobotsAuth class inject the Bearer Authorization header properly
    """
    auth = WherobotsAuth("api_key")
    responses.get(
        url="https://example.com",
        status=HTTPStatus.OK,
        match=[matchers.header_matcher({"X-API-Key": "api_key"})],
    )
    with requests.session() as session:
        resp = session.get("https://example.com", auth=auth)
        assert resp.status_code == HTTPStatus.OK


class TestModel(BaseModel):
    key: str


class TestWherobotsRestAPIHook:
    """
    Test the WherobotsRestAPIHook class
    """

    def test_context_manager(self, mocker: MockerFixture) -> None:
        """
        Test the context manager
        """
        with WherobotsRestAPIHook() as hook:
            mock_session_close = mocker.patch.object(hook.session, "close")
        mock_session_close.assert_called_once()

    @responses.activate
    def test_api_call(self, test_default_conn) -> None:
        """
        Test the _api_call method
        """
        url = f"https://{test_default_conn.host}/test"
        responses.add(
            responses.GET,
            url,
            json={"key": "value"},
            status=HTTPStatus.OK,
        )
        with WherobotsRestAPIHook() as hook:
            test_resp_json = hook._api_call("GET", "/test").json()
            test_model = TestModel.model_validate(test_resp_json)

            assert 429 in hook.build_retry().status_forcelist
            assert test_model.key == "value"
            assert len(responses.calls) == 1
            assert responses.calls[0].request.url == url
            assert (
                responses.calls[0].request.headers["X-API-Key"]
                == test_default_conn.password
            )

    @responses.activate
    def test_get_run(self, test_default_conn) -> None:
        """
        Test the get_run method
        """
        test_run: Run = helpers.run_factory.build()
        url = f"https://{test_default_conn.host}/runs/{test_run.ext_id}"
        payload: dict = json.loads(test_run.model_dump_json())
        payload.update({"extra_field": "random"})
        responses.add(
            responses.GET,
            url,
            json=payload,
            status=HTTPStatus.OK,
        )
        with WherobotsRestAPIHook() as hook:
            fetched_run = hook.get_run(test_run.ext_id)
            assert fetched_run.ext_id == test_run.ext_id
            assert fetched_run.status == test_run.status

    @responses.activate
    def test_create_run(self, test_default_conn) -> None:
        """
        Test the get_run method
        """
        test_run: Run = helpers.run_factory.build()
        url = f"https://{test_default_conn.host}/runs"
        create_payload = {
            "name": test_run.name,
            "runtime": Runtime.TINY.value,
            "python": {
                "uri": "s3://bucket/test.py",
                "args": ["arg1", "arg2"],
                "entrypoint": "src.main",
            },
            "timeoutSeconds": 5000,
        }
        responses.add(
            responses.POST,
            url,
            json=test_run.model_dump(mode="json"),
            match=[matchers.json_params_matcher(create_payload)],
            status=HTTPStatus.OK,
        )
        with WherobotsRestAPIHook() as hook:
            hook.create_run(payload=create_payload, region=Region.AWS_US_WEST_2)

    @responses.activate
    def test_get_run_logs(self, test_default_conn) -> None:
        """
        Test the get_run method
        """
        test_run: Run = helpers.run_factory.build()
        url = f"https://{test_default_conn.host}/runs/{test_run.ext_id}/logs"
        responses.add(
            responses.GET,
            url,
            json=LogsResponse(items=[], current_page=12345, next_page=None).model_dump(
                mode="json"
            ),
            match=[matchers.query_param_matcher({"cursor": 12345, "size": 500})],
            status=HTTPStatus.OK,
        )
        with WherobotsRestAPIHook() as hook:
            hook.get_run_logs(run_id=test_run.ext_id, start=12345)

    @responses.activate
    def test_cancel_run(self, test_default_conn) -> None:
        """
        Test the cancel_run method
        """
        test_run: Run = helpers.run_factory.build()
        url = f"https://{test_default_conn.host}/runs/{test_run.ext_id}/cancel"
        responses.add(
            responses.POST,
            url,
            status=HTTPStatus.OK,
        )
        with WherobotsRestAPIHook() as hook:
            hook.cancel_run(run_id=test_run.ext_id)

    def test_user_agent(self, test_default_conn, mocker: MockerFixture) -> None:
        """
        Test the user_agent_header property
        """
        with WherobotsRestAPIHook() as hook:
            mocker.patch(
                "airflow_providers_wherobots.hooks.rest_api.platform.system",
                return_value="Linux",
            )
            mocker.patch(
                "airflow_providers_wherobots.hooks.rest_api.platform.python_version",
                return_value="3.8.10",
            )
            mocker.patch(
                "airflow_providers_wherobots.hooks.rest_api.metadata.version",
                return_value="1.2.3",
            )
            assert hook.user_agent_header == {
                "User-Agent": f"airflow-providers-wherobots/1.2.3 os/linux python/3.8.10 airflow/{airflow.__version__}"
            }
