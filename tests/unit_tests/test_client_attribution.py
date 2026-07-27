"""
Test the shared ``X-Wherobots-Client`` client-attribution hop.
"""

import importlib
import re
from importlib import metadata

from pytest_mock import MockerFixture

from airflow_providers_wherobots import client_attribution
from airflow_providers_wherobots.client_attribution import (
    CLIENT_HOP,
    UNKNOWN_VERSION,
    WHEROBOTS_CLIENT_HEADER,
    client_attribution_header,
)
from airflow_providers_wherobots.hooks.base import PACKAGE_NAME

# One hop: `client=<token>` plus zero or more `;key=value` params, where no
# value may contain the `,` / `;` delimiters.
HOP_PATTERN = re.compile(r"^client=[^,;]+(?:;[^,;=]+=[^,;]*)*$")


def test_header_name_and_hop_format() -> None:
    """The hook emits exactly one canonical header carrying one well-formed hop."""
    header = client_attribution_header()
    assert list(header) == [WHEROBOTS_CLIENT_HEADER]
    assert WHEROBOTS_CLIENT_HEADER == "X-Wherobots-Client"

    hop = header[WHEROBOTS_CLIENT_HEADER]
    assert HOP_PATTERN.match(hop)
    assert hop == f"client=airflow;ver={metadata.version(PACKAGE_NAME)}"


def test_hop_is_within_the_length_bound() -> None:
    """The chain is bounded to 512 bytes, and tokens to 64 characters."""
    assert len(CLIENT_HOP.encode("utf-8")) <= 512
    assert len("airflow") <= 64


def test_unresolvable_version_falls_back_to_unknown(mocker: MockerFixture) -> None:
    """A provider that isn't installed still emits a well-formed hop."""
    mocker.patch.object(
        client_attribution.metadata,
        "version",
        side_effect=metadata.PackageNotFoundError(PACKAGE_NAME),
    )
    assert client_attribution._resolve_provider_version() == UNKNOWN_VERSION

    # The hop is built once at import, so reload the module under the patch to
    # observe the header a never-installed provider would actually send.
    try:
        reloaded = importlib.reload(client_attribution)
        assert reloaded.client_attribution_header() == {
            WHEROBOTS_CLIENT_HEADER: "client=airflow;ver=unknown"
        }
        assert HOP_PATTERN.match(reloaded.CLIENT_HOP)
    finally:
        mocker.stopall()
        importlib.reload(client_attribution)


def test_delimiters_are_stripped_from_the_version(mocker: MockerFixture) -> None:
    """A version can never smuggle a hop or field separator into the grammar."""
    mocker.patch.object(
        client_attribution.metadata, "version", return_value="1.0;0,dev"
    )
    try:
        reloaded = importlib.reload(client_attribution)
        assert reloaded.CLIENT_HOP == "client=airflow;ver=1.0_0_dev"
        assert HOP_PATTERN.match(reloaded.CLIENT_HOP)
    finally:
        mocker.stopall()
        importlib.reload(client_attribution)
