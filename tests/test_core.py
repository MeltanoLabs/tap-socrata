"""Tests standard tap features using the built-in SDK tests library."""

from __future__ import annotations

from singer_sdk.testing import get_tap_test_class

from tap_socrata.tap import TapSocrata

SAMPLE_CONFIG = {
    "domains": ["data.cityofnewyork.us"],
    # DOF Parking Violation Codes: a small (~97 row), fully dense public
    # dataset (no sparse/geo-computed columns), kept scoped so the standard
    # test suite doesn't sync all of NYC Open Data.
    "dataset_ids": ["ncbg-6agr"],
}


# Run standard built-in tap tests from the SDK. These exercise discovery and
# a live sync against the public NYC Open Data portal (no credentials required).
TestTapSocrata = get_tap_test_class(
    tap_class=TapSocrata,
    config=SAMPLE_CONFIG,
)
