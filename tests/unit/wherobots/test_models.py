"""
Test module wherobots.models
"""
from wherobots.db import Runtime

from airflow_providers_wherobots.wherobots.models import CreateRunPayload, PythonRunPayload, JavaRunPayload


def test_create_run_payload():
    payload = CreateRunPayload.create(
        runtime=Runtime.SEDONA,
        name="test",
        python=PythonRunPayload.create(
            uri="s3://path/to/python",
            args=["--arg1", "value1"],
            entrypoint="main",
        ),
        java=JavaRunPayload.create(
            uri="s3://path/to/java",
            args=["--arg1", "value1"],
            main_class="main",
        ),
        timeout_seconds=1200,
    )
    assert payload.runtime == Runtime.SEDONA
    assert payload.timeout_seconds == 1200
