"""Socrata tap class."""

from __future__ import annotations

import sys
import typing as t
from datetime import datetime, timezone

import requests
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_socrata.client import SocrataStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapSocrata(Tap):
    """Socrata tap class."""

    name = "tap-socrata"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "domains",
            th.ArrayType(th.StringType),
            required=True,
            description="Domain names to query (e.g., ['data.cityofchicago.org'])",
        ),
        th.Property(
            "dataset_ids",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "Optional list of Socrata dataset (4x4) IDs to restrict discovery to. "
                "If omitted, every dataset published under `domains` is discovered, "
                "which can be a very large number of streams."
            ),
        ),
        th.Property(
            "api_key_id",
            th.StringType,
            secret=True,
            description="The API Key ID for authentication",
        ),
        th.Property(
            "api_key_secret",
            th.StringType,
            secret=True,
            description="The API Key Secret for authentication",
        ),
        th.Property(
            "app_token",
            th.StringType,
            secret=True,
            description="Optional Socrata App Token for higher rate limits",
        ),
        th.Property(
            "secret_token",
            th.StringType,
            secret=True,
            description="Optional Socrata Secret Token paired with App Token",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            required=False,
            description="Optional User-Agent string to use for requests",
        ),
    ).to_dict()

    def _get_discovery_url(self) -> str:
        """Determine which discovery API to use based on provided domains."""
        if self.config.get("domains"):
            # Check the first domain to determine which API to use
            domain = self.config["domains"][0].lower()
            if any(
                domain.endswith(tld)
                for tld in [".eu"]  # Only check for EU TLDs
            ):
                return "https://api.eu.socrata.com/api/catalog/v1"
        # Default to US API if no domains specified or non-EU domain
        return "https://api.us.socrata.com/api/catalog/v1"

    def _get_schema_for_column(self, col_type: str) -> dict[str, t.Any]:
        """Map Socrata datatypes to JSON Schema types."""
        schema: dict[str, t.Any] = {
            "type": ["null", "string"],  # Default to nullable string
        }

        if col_type == "number":
            # Since Socrata might return numbers as strings, accept both
            schema["type"] = ["null", "string", "number"]
        elif col_type == "checkbox":
            schema["type"] = ["null", "boolean"]
        elif col_type in ["fixed_timestamp", "floating_timestamp"]:
            schema["type"] = ["null", "string"]
            schema["format"] = "date-time"
        elif col_type == "location":
            schema["type"] = ["null", "object"]
            schema["properties"] = {
                "latitude": {"type": ["null", "string"]},
                "longitude": {"type": ["null", "string"]},
                "human_address": {"type": ["null", "string"]},  # It's a JSON string
            }
        elif col_type == "url":
            schema["type"] = ["null", "object"]
            schema["properties"] = {
                "url": {"type": ["null", "string"]},
                "description": {"type": ["null", "string"]},
            }
        elif col_type in [
            "line",
            "multiline",
            "point",
            "multipoint",
            "polygon",
            "multipolygon",
        ]:
            schema["type"] = ["null", "object"]
            schema["properties"] = {
                "type": {"type": "string"},
                "coordinates": {"type": "array"},
            }

        return schema

    def _sanitize_stream_name(self, name: str, dataset_id: str) -> str:
        """Create a human-readable stream name from the dataset name and ID."""
        # Convert to lowercase and replace problematic characters
        sanitized = (
            name.lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
        )
        # Remove any other non-alphanumeric characters
        sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")

        # Add the ID as a suffix to ensure uniqueness
        return f"{sanitized}_{dataset_id.replace('-', '_')}"

    @override
    def discover_streams(self) -> list[SocrataStream]:  # noqa: C901, PLR0912, PLR0915
        """Return a list of discovered streams."""
        headers = {}
        if self.config.get("app_token"):
            headers["X-App-Token"] = self.config["app_token"]
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config["user_agent"]

        streams = []
        discovery_url = self._get_discovery_url()

        # Get all datasets with pagination
        offset = 0
        all_datasets = []

        while True:
            params = {
                "domains": self.config.get("domains", []),
                "offset": offset,
                "limit": 1000,
            }
            if self.config.get("dataset_ids"):
                params["ids"] = self.config["dataset_ids"]
            api_key_id = self.config.get("api_key_id")
            api_key_secret = self.config.get("api_key_secret")
            if api_key_id and api_key_secret:
                response = requests.get(
                    discovery_url,
                    params=params,
                    headers=headers,
                    auth=(api_key_id, api_key_secret),
                    timeout=10,
                )
            else:
                response = requests.get(
                    discovery_url,
                    params=params,
                    headers=headers,
                    timeout=10,
                )

            response.raise_for_status()
            datasets = response.json().get("results", [])
            if not datasets:
                break

            all_datasets.extend(datasets)
            offset += len(datasets)
        for dataset in all_datasets:
            resource = dataset.get("resource", {})
            metadata = dataset.get("metadata", {})
            try:
                schema: dict[str, t.Any] = {"type": "object", "properties": {}}

                # Map column types to JSON schema. `columns_field_name` is the
                # actual key Socrata uses in each record's JSON payload, unlike
                # `columns_name` which is just a human-readable display label.
                for field_name, col_type in zip(
                    resource["columns_field_name"],
                    resource["columns_datatype"],
                    strict=True,
                ):
                    schema["properties"][field_name] = self._get_schema_for_column(col_type.lower())

                stream_name = self._sanitize_stream_name(
                    resource.get("name", "unnamed"),
                    resource["id"],
                )

                # Determine primary keys and replication key
                primary_keys = []
                replication_key = None
                data_updated_at = None
                if resource.get("data_updated_at"):
                    data_updated_at = datetime.strptime(
                        resource["data_updated_at"],
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                    ).replace(tzinfo=timezone.utc)
                    replication_key = "_data_updated_at"
                    schema["properties"]["_data_updated_at"] = {
                        "type": ["null", "string"],
                        "format": "date-time",
                    }
                if "id" in schema["properties"]:
                    primary_keys = ["id"]
                elif "case_id" in schema["properties"]:
                    primary_keys = ["case_id"]
                elif "record_id" in schema["properties"]:
                    primary_keys = ["record_id"]

                stream_class_name = f"SocrataStream_{resource['id']}"
                dynamic_stream_class = type(
                    stream_class_name,
                    (SocrataStream,),
                    {"primary_keys": primary_keys, "replication_key": replication_key},
                )

                stream = dynamic_stream_class(
                    tap=self,
                    name=stream_name,
                    schema=schema,
                    domain=metadata["domain"],
                    dataset_id=resource["id"],
                    dataset_type=resource.get("type"),
                    data_updated_at=data_updated_at,
                )

                streams.append(stream)
            except Exception:  # noqa: BLE001
                self.logger.warning(
                    "Failed to create stream for dataset %s",
                    resource.get("id"),
                    exc_info=True,
                )
        return streams


if __name__ == "__main__":
    TapSocrata.cli()
