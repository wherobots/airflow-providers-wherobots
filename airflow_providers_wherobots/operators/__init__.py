import logging
from typing import Optional

from wherobots.db import Region
from wherobots.db.constants import DEFAULT_REGION

logger = logging.getLogger(__name__)


def warn_for_default_region(region: Optional[Region]) -> Region:
    if not region:
        logger.warning(""""Parameter region was not specified, it will be required by Wherobots API in the near future, please specify it in the operator.
        If you don't know your Wherobots Compute Region, please contact Wherobots support.""")
        logger.warning(f"Using default region: {DEFAULT_REGION.value}")
        region = DEFAULT_REGION
    return region
