"""Socrata entry point."""

from __future__ import annotations

from tap_socrata.tap import TapSocrata

TapSocrata.cli()
