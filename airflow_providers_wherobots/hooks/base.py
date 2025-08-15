"""
Constants
"""
from typing_extensions import override
from urllib3.util import Retry

DEFAULT_CONN_ID = "wherobots_default"
PACKAGE_NAME = "airflow-providers-wherobots"

class WherobotsRetry(Retry):

    @override
    def parse_retry_after(self, retry_after: str) -> float:
        import math
        import re

        if re.match("^[-+]?[0-9]*\.[0-9]+$", retry_after):
            retry_after = str(math.ceil(float(retry_after)))
        return super().parse_retry_after(retry_after)

