import email.utils
import datetime
from http import HTTPStatus

import pytest
import requests
import responses
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError

from airflow_providers_wherobots.hooks.base import WherobotsRetry


@responses.activate
def test_retry_after() -> None:
    """
    Test the WherobotsRetry class with a 429 response and Retry-After header
    """

    retry = WherobotsRetry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504, 429],
    )

    responses.get(
        url="https://example.com",
        status=HTTPStatus.TOO_MANY_REQUESTS,
        headers={"Retry-After": "5.2"},
    )

    with requests.session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        with pytest.raises(RetryError) as exception:
            resp = session.get("https://example.com")
            assert resp.status_code == HTTPStatus.TOO_MANY_REQUESTS
            assert "Max retries exceeded with url" in str(exception.value)

def generate_retry_after_date(delay_seconds):
    """
    Generates a future date string in RFC 1123 format for Retry-After header.

    Args:
    delay_seconds: The number of seconds to add to the current time.

    Returns:
    A string representing the future date in RFC 1123 format.
    """

    # Get the current time in UTC
    current_time = datetime.datetime.now(datetime.UTC)

    # Add the specified delay
    future_time = current_time + datetime.timedelta(seconds=delay_seconds)

    # Format the future time as an RFC 1123 string
    return email.utils.format_datetime(future_time, usegmt=True)


def test_parse_retry_after() -> None:
    """
    Test the parse_retry_after method
    """
    retry = WherobotsRetry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504, 429],
    )

    assert retry.parse_retry_after("5.2") == 6.0
    assert retry.parse_retry_after("10") == 10.0
    assert retry.parse_retry_after("0") == 0.0

    timestamp = generate_retry_after_date(5)
    assert 4.0 <= retry.parse_retry_after(timestamp) <= 5.0