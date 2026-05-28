from wherobots.db import Region

from airflow_providers_wherobots.operators import warn_for_default_region


class TestWarnForDefaultRegion:
    def test_none_returns_none(self) -> None:
        # No region -> None so the API applies the organization's default.
        assert warn_for_default_region(None) is None

    def test_enum_is_normalized_to_value(self) -> None:
        assert warn_for_default_region(Region.AWS_US_WEST_2) == "aws-us-west-2"

    def test_string_is_passed_through(self) -> None:
        assert warn_for_default_region("byoc-acme-us-east-1") == "byoc-acme-us-east-1"
