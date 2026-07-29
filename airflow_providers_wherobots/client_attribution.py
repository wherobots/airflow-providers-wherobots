"""
Client attribution via the shared ``X-Wherobots-Client`` header.

``X-Wherobots-Client`` is an ordered, append-only, comma-separated list of
hops modelled on ``X-Forwarded-For``: the leftmost hop is the ORIGIN client
and every component appends its own hop on the right. It lets Wherobots
services attribute a request to the client it came from.

This provider is an origin client: an Airflow DAG is where the request enters
the Wherobots client ecosystem, so it emits ``client=airflow;ver=<version>``
as the leftmost hop. Where the provider goes through another Wherobots client
(the Python DB-API driver, for the SQL hook), that client appends its own hop
to the right, producing e.g.
``client=airflow;ver=1.7.0, client=dbapi;ver=0.28.1``.

The header is advisory: it is client-asserted and informational only, and must
never influence authentication or authorization.
"""

from importlib import metadata
from typing import Dict, Final

from airflow_providers_wherobots.hooks.base import PACKAGE_NAME

# Canonical name of the shared, cross-service client-chain header.
WHEROBOTS_CLIENT_HEADER: Final[str] = "X-Wherobots-Client"

# Canonical, stable vocabulary token for this client. Renaming it splits its
# history in the platform's attribution analytics, so it must not change.
CLIENT_TOKEN: Final[str] = "airflow"

# Sentinel used when the installed distribution's version can't be resolved
# (e.g. the provider is imported from a source tree that was never installed).
UNKNOWN_VERSION: Final[str] = "unknown"

# Commas separate hops and semicolons separate a hop's fields, so neither may
# appear inside a value.
_DELIMITERS = str.maketrans({",": "_", ";": "_"})


def _resolve_provider_version() -> str:
    """Return the installed provider version, or ``unknown`` if unavailable."""
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        return UNKNOWN_VERSION


# Resolved once at import: `importlib.metadata.version` scans the installed
# package database on each call, and the version can't change within a process.
PROVIDER_VERSION: Final[str] = _resolve_provider_version()

# This provider's single hop, e.g. `client=airflow;ver=1.7.0`.
CLIENT_HOP: Final[str] = (
    f"client={CLIENT_TOKEN};ver={PROVIDER_VERSION.translate(_DELIMITERS)}"
)


def client_attribution_header() -> Dict[str, str]:
    """Return the ``X-Wherobots-Client`` header carrying this provider's hop."""
    return {WHEROBOTS_CLIENT_HEADER: CLIENT_HOP}
