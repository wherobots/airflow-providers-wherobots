import logging
from typing import Optional, Union

from wherobots.db import Region

logger = logging.getLogger(__name__)


def warn_for_default_region(
    region: Optional[Union[str, Region]],
) -> Optional[Union[str, Region]]:
    """Resolve the region argument for a Wherobots operator.

    The value is returned unchanged — a ``Region`` enum or a raw string (e.g. a
    BYOC region). Normalization (enum -> value) happens at the request boundary
    (``WherobotsRestAPIHook.create_run`` / ``wherobots.db.connect``), so the enum
    keeps working with older ``wherobots-python-dbapi`` releases. When no region
    is provided, returns ``None`` so the API applies the organization's
    configured default region — only set ``region`` to override that default.
    """
    if not region:
        logger.info(
            "No region specified; the Wherobots API will use your organization's "
            "configured default region. Pass `region` to override it."
        )
        return None
    return region
