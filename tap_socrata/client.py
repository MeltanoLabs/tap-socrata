# tap_socrata/client.py
"""REST client handling, including SocrataStream base class."""

from __future__ import annotations

import decimal
from datetime import datetime, timezone
import typing as t
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import BasicAuthenticator
from requests.auth import HTTPBasicAuth
from singer_sdk.helpers.jsonpath import extract_jsonpath
import logging

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

logger = logging.getLogger(__name__)


class SocrataStream(RESTStream):
    """Socrata stream class."""

    records_jsonpath = "$[*]"

    def __init__(
        self,
        tap: t.Any,
        name: str,
        schema: dict,
        domain: str,
        dataset_id: str,  # Add dataset_id parameter
        dataset_type: str,
        data_updated_at: datetime,
        limit: int = 50000,  # maximum records returned per request
    ) -> None:
        """Initialize the Socrata stream.

        Args:
            tap: Parent tap instance
            name: Stream name (human readable)
            schema: JSON schema for the stream
            domain: Socrata domain for this dataset
            dataset_id: Socrata dataset ID for API calls
        """
        super().__init__(tap=tap, name=name, schema=schema)
        self._domain = domain
        self._dataset_id = dataset_id  # Store the dataset ID
        self.path = f"/resource/{dataset_id}.json"
        self.dataset_type = dataset_type
        if self.dataset_type == "map":
            self.path = f"/resource/{dataset_id}.geojson"
        self._data_updated_at = data_updated_at
        if self._data_updated_at:
            self._data_updated_at = data_updated_at.replace(tzinfo=timezone.utc)
        self.limit = limit

    def validate_response(self, response: requests.Response) -> None:
        response.raise_for_status()

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Get records.

        Returns None if dataset hasn't been updated since last run.
        """
        starting_ts = self.get_starting_timestamp(context)
        if starting_ts and self._data_updated_at:
            starting_ts = starting_ts.replace(tzinfo=timezone.utc)

            if starting_ts >= self._data_updated_at:
                return []
        yield from super().get_records(context)

    @property
    def url_base(self) -> str:
        """Return the API URL root for this dataset."""
        return f"https://{self._domain}"

    @property
    def authenticator(self) -> HTTPBasicAuth | None:
        """Return a new authenticator object."""
        if self.config.get("api_key_id"):
            return HTTPBasicAuth(
                self.config.get("api_key_id"), self.config.get("api_key_secret")
            )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if self.config.get("app_token"):
            headers["X-App-Token"] = self.config["app_token"]
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config["user_agent"]
        return headers

    # tap_socrata/client.py
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

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,
    ) -> t.Any | None:
        """Return token for identifying next page or None if no more pages."""
        records = response.json()

        # If we got no records, we're done
        if not records:
            return None

        # Calculate next offset
        previous_offset = previous_token or 0
        records_returned = len(records)

        # If we got less than the limit,  we're done
        if records_returned < self.limit:
            return None

        # Otherwise, return next offset
        return previous_offset + records_returned

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

    def get_url(self, context: Context | None = None) -> str:
        """Get URL for API request."""
        return f"{self.url_base}/resource/{self._dataset_id}.json"
