"""Socrata tap class."""

from __future__ import annotations
from typing import List

import requests
from singer_sdk import Tap
from singer_sdk import typing as th
import typing as t
from tap_socrata.client import SocrataStream
from datetime import datetime


class TapSocrata(Tap):
    """Socrata tap class."""

    name = "tap-socrata"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "domains",
            th.ArrayType(th.StringType),
            description="Domain names to query (e.g., ['data.cityofchicago.org'])",
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

    def _get_schema_for_column(self, col_type: str, col_name: str) -> dict:
        """Map Socrata datatypes to JSON Schema types."""
        schema = {
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

    def _sanitize_stream_name(self, name: str, id: str) -> str:
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
        return f"{sanitized}_{id.replace('-', '_')}"

    def discover_streams(self) -> List[SocrataStream]:
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
            if self.config.get("api_key_id"):
                response = requests.get(
                    discovery_url,
                    params=params,
                    headers=headers,
                    auth=(
                        self.config.get("api_key_id"),
                        self.config.get("api_key_secret"),
                    ),
                )
            else:
                response = requests.get(
                    discovery_url,
                    params=params,
                    headers=headers,
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
                schema = {"type": "object", "properties": {}}

                # Map column types to JSON schema
                for i, col_name in enumerate(resource["columns_name"]):
                    field_name = (
                        col_name.lower()
                        .replace(" ", "_")
                        .replace("(", "")
                        .replace(")", "")
                        .replace("-", "_")
                    )
                    col_type = resource["columns_datatype"][i].lower()
                    schema["properties"][field_name] = self._get_schema_for_column(
                        col_type, field_name
                    )

                stream_name = self._sanitize_stream_name(
                    resource.get("name", "unnamed"), resource["id"]
                )

                # Determine primary keys and replication key
                primary_keys = []
                replication_key = None
                data_updated_at = None
                if resource.get("data_updated_at"):
                    data_updated_at = datetime.strptime(
                        resource["data_updated_at"],
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                    )
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
            except Exception as e:
                self.logger.warning(
                    f"Failed to create stream for dataset {resource.get('id')}: {str(e)}"
                )
        return streams


if __name__ == "__main__":
    TapSocrata.cli()
