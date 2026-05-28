import logging
from typing import Optional, Union

from wherobots.db import Region

logger = logging.getLogger(__name__)


def warn_for_default_region(region: Optional[Union[str, Region]]) -> Optional[str]:
    """Normalize a region argument to the string sent to the Wherobots API.

    Accepts a ``Region`` enum or a raw string (e.g. a BYOC region), which is
    passed through as-is. When no region is provided, returns ``None`` so the
    API applies the organization's configured default region — only set
    ``region`` to override that default with a specific region.
    """
    if not region:
        logger.info(
            "No region specified; the Wherobots API will use your organization's "
            "configured default region. Pass `region` to override it."
        )
        return None
    return region.value if isinstance(region, Region) else region
