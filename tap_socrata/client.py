# tap_socrata/client.py
"""REST client handling, including SocrataStream base class."""

from __future__ import annotations

import decimal
import logging
import sys
import typing as t
from datetime import datetime, timezone

from requests.auth import HTTPBasicAuth
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import OffsetPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk import Tap
    from singer_sdk.helpers.types import Context

logger = logging.getLogger(__name__)


class SocrataPaginator(OffsetPaginator):
    """Paginator for Socrata's `$limit`/`$offset` API."""

    @override
    def has_more(self, response: requests.Response) -> bool:
        """Return True if the response indicates there are more records."""
        return len(response.json()) >= self.page_size


class SocrataStream(RESTStream):
    """Socrata stream class."""

    records_jsonpath = "$[*]"

    @override
    def __init__(
        self,
        tap: Tap,
        name: str,
        schema: dict,
        domain: str,
        dataset_id: str,  # Add dataset_id parameter
        dataset_type: str,
        data_updated_at: datetime | None,
        limit: int = 50000,  # maximum records returned per request
    ) -> None:
        """Initialize the Socrata stream.

        Args:
            tap: Parent tap instance
            name: Stream name (human readable)
            schema: JSON schema for the stream
            domain: Socrata domain for this dataset
            dataset_id: Socrata dataset ID for API calls
            dataset_type: Type of dataset (e.g., "map" or "table")
            data_updated_at: Timestamp of the last update for the dataset
            limit: Maximum number of records to return per request (default: 50000)
        """
        super().__init__(tap=tap, name=name, schema=schema)
        self._domain = domain
        self._dataset_id = dataset_id  # Store the dataset ID
        self.path = f"/resource/{dataset_id}.json"
        self.dataset_type = dataset_type
        if self.dataset_type == "map":
            self.path = f"/resource/{dataset_id}.geojson"
        self._data_updated_at: datetime | None = data_updated_at
        if data_updated_at:
            self._data_updated_at = data_updated_at.replace(tzinfo=timezone.utc)
        self.limit = limit

    @override
    def validate_response(self, response: requests.Response) -> None:
        response.raise_for_status()

    @override
    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Get records.

        Returns None if dataset hasn't been updated since last run.
        """
        starting_ts = self.get_starting_timestamp(context)
        if starting_ts and self._data_updated_at:
            starting_ts = starting_ts.replace(tzinfo=timezone.utc)

            if starting_ts >= self._data_updated_at:
                return
        yield from super().get_records(context)

    @property
    @override
    def url_base(self) -> str:
        """Return the API URL root for this dataset."""
        return f"https://{self._domain}"

    @property
    @override
    def authenticator(self) -> HTTPBasicAuth | APIAuthenticatorBase:
        """Return a new authenticator object."""
        api_key_id = self.config.get("api_key_id")
        api_key_secret = self.config.get("api_key_secret")
        if api_key_id and api_key_secret:
            return HTTPBasicAuth(api_key_id, api_key_secret)
        return APIAuthenticatorBase()

    @property
    @override
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if self.config.get("app_token"):
            headers["X-App-Token"] = self.config["app_token"]
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config["user_agent"]
        return headers

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "$order": ":id",  # Ensure stable ordering
            "$limit": self.limit,
        }

        # Handle pagination
        if next_page_token:
            params["$offset"] = next_page_token
        return params

    @override
    def get_new_paginator(self) -> SocrataPaginator:
        """Return a paginator for offset-based pagination."""
        return SocrataPaginator(start_value=0, page_size=self.limit)

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        for record in extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        ):
            if self._data_updated_at:
                yield {**record, "_data_updated_at": self._data_updated_at}
            else:
                yield record

    @override
    def get_url(self, context: Context | None = None) -> str:
        """Get URL for API request."""
        return f"{self.url_base}/resource/{self._dataset_id}.json"
