from wherobots.db import Region

from airflow_providers_wherobots.operators import warn_for_default_region


class TestWarnForDefaultRegion:
    def test_none_returns_none(self) -> None:
        # No region -> None so the API applies the organization's default.
        assert warn_for_default_region(None) is None

    def test_enum_is_passed_through_unchanged(self) -> None:
        # Normalization happens at the request boundary, not here, so the enum
        # still works with older dbapi releases that call region.value.
        assert warn_for_default_region(Region.AWS_US_WEST_2) is Region.AWS_US_WEST_2

    def test_string_is_passed_through(self) -> None:
        assert warn_for_default_region("byoc-acme-us-east-1") == "byoc-acme-us-east-1"

    def test_empty_string_is_passed_through(self) -> None:
        # "" is a caller mistake worth surfacing to the API, not silently
        # dropped as if no region were provided.
        assert warn_for_default_region("") == ""
