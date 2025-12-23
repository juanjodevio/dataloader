"""API connector for reading data from REST APIs."""

import time
from typing import Any, Iterable, Union

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector

try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.exceptions import RequestException, Timeout
    from urllib3.util.retry import Retry
except ImportError:
    requests = None  # type: ignore
    HTTPAdapter = None  # type: ignore
    RequestException = None  # type: ignore
    Timeout = None  # type: ignore
    Retry = None  # type: ignore

try:
    from jsonpath_ng import parse as parse_jsonpath
except ImportError:
    parse_jsonpath = None  # type: ignore

import pyarrow as pa

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import ApiConnectorConfig


class ApiConnector:
    """Unified connector for reading data from REST APIs.

    Supports pagination, authentication, error handling with retries,
    and conversion of JSON responses to ArrowBatch format.
    """

    def __init__(
        self,
        config: Union[ApiConnectorConfig, SourceConfig, DestinationConfig],
    ):
        """Initialize ApiConnector.

        Args:
            config: API connector configuration (ApiConnectorConfig, SourceConfig, or DestinationConfig).
                All configuration, including connection parameters, should be in the config parameter.

        Raises:
            ImportError: If required dependencies are not installed (install with: pip install dataloader[api])
        """
        if requests is None:
            raise ImportError(
                "ApiConnector requires requests. "
                "Install it with: pip install dataloader[api]"
            )
        if parse_jsonpath is None:
            raise ImportError(
                "ApiConnector requires jsonpath-ng. "
                "Install it with: pip install dataloader[api]"
            )

        self._config = config

        # Extract config values
        if isinstance(config, ApiConnectorConfig):
            self._base_url = config.base_url.rstrip("/")
            self._endpoint = config.endpoint
            self._params = config.params or {}
            self._headers = config.headers or {}
            self._auth_type = config.auth_type
            self._auth_token = (
                config.auth_token.get_secret_value() if config.auth_token else None
            )
            self._auth_username = config.auth_username
            self._auth_password = (
                config.auth_password.get_secret_value()
                if config.auth_password
                else None
            )
            self._pagination_type = config.pagination_type
            self._page_param = config.page_param
            self._limit_param = config.limit_param
            self._page_size = config.page_size
            self._data_path = config.data_path
            self._total_path = config.total_path
            self._timeout = config.timeout
            self._max_retries = config.max_retries
            self._retry_delay = config.retry_delay
            self._backoff_rate = config.backoff_rate
            self._incremental = config.incremental
        elif isinstance(config, SourceConfig):
            # Extract from SourceConfig (will need to add API fields to SourceConfig)
            self._base_url = getattr(config, "base_url", "").rstrip("/")
            self._endpoint = getattr(config, "endpoint", "")
            self._params = getattr(config, "params", {}) or {}
            self._headers = getattr(config, "headers", {}) or {}
            self._auth_type = getattr(config, "auth_type", "none")
            auth_token = getattr(config, "auth_token", None)
            self._auth_token = (
                auth_token.get_secret_value()
                if auth_token and hasattr(auth_token, "get_secret_value")
                else auth_token
            )
            self._auth_username = getattr(config, "auth_username", None)
            auth_password = getattr(config, "auth_password", None)
            self._auth_password = (
                auth_password.get_secret_value()
                if auth_password and hasattr(auth_password, "get_secret_value")
                else auth_password
            )
            self._pagination_type = getattr(config, "pagination_type", "page")
            self._page_param = getattr(config, "page_param", "page")
            self._limit_param = getattr(config, "limit_param", None)
            self._page_size = getattr(config, "page_size", 100)
            self._data_path = getattr(config, "data_path", None)
            self._total_path = getattr(config, "total_path", None)
            self._timeout = getattr(config, "timeout", None) or 30
            self._max_retries = getattr(config, "max_retries", None) or 3
            self._retry_delay = getattr(config, "retry_delay", None) or 1.0
            self._backoff_rate = getattr(config, "backoff_rate", None) or 2.0
            self._incremental = config.incremental
        else:  # DestinationConfig - API connector is read-only
            raise ValueError("ApiConnector does not support write operations")

        # Validate required fields
        if not self._base_url:
            raise ValueError("base_url is required for API connector")
        if not self._endpoint:
            raise ValueError("endpoint is required for API connector")

        # Build requests session with authentication
        self._session = requests.Session()

        # Set up authentication
        if self._auth_type == "bearer":
            if not self._auth_token:
                raise ValueError("auth_token is required when auth_type is 'bearer'")
            self._session.headers.update(
                {"Authorization": f"Bearer {self._auth_token}"}
            )
        elif self._auth_type == "basic":
            if not self._auth_username or not self._auth_password:
                raise ValueError(
                    "auth_username and auth_password are required when auth_type is 'basic'"
                )
            from requests.auth import HTTPBasicAuth

            self._session.auth = HTTPBasicAuth(self._auth_username, self._auth_password)

        # Set default headers
        if self._headers:
            self._session.headers.update(self._headers)

        # Set up retry strategy for urllib3 (handles 5xx errors automatically)
        # We'll handle timeouts and other errors manually in _make_request
        max_retries_int = int(self._max_retries) if self._max_retries else 0
        if max_retries_int > 0:
            retry_strategy = Retry(
                total=max_retries_int,
                backoff_factor=self._retry_delay,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)

        # Schema cache for consistent types across batches
        self._schema_cache: pa.Schema | None = None

    def _build_url(self, page: int | None = None, offset: int | None = None) -> str:
        """Build complete URL with parameters and pagination.

        Args:
            page: Page number (for page-based pagination).
            offset: Offset value (for offset-based pagination).

        Returns:
            Complete URL string.
        """
        url = f"{self._base_url}{self._endpoint}"

        # Start with base params
        params = dict(self._params)

        # Add pagination parameters
        if self._pagination_type == "page" and page is not None:
            params[self._page_param] = page
        elif self._pagination_type == "offset" and offset is not None:
            params[self._page_param] = offset  # Reuse page_param for offset
        # cursor pagination handled separately via params

        # Add limit if specified
        if self._limit_param:
            params[self._limit_param] = self._page_size

        # Build query string
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"

        return url

    def _extract_data(self, response_json: dict | list) -> list[dict]:
        """Extract data array from JSON response using data_path.

        Args:
            response_json: Parsed JSON response (dict or list).

        Returns:
            List of dictionaries representing the data rows.

        Raises:
            ConnectorError: If data_path is invalid or data cannot be extracted.
        """
        # If response is already a list, return it
        if isinstance(response_json, list):
            return response_json

        # If no data_path specified, try common patterns
        if self._data_path is None:
            if isinstance(response_json, dict):
                # Try common keys
                for key in ["data", "results", "items", "records"]:
                    if key in response_json and isinstance(response_json[key], list):
                        return response_json[key]
                # If no common key found, raise error
                raise ConnectorError(
                    "Response is a dict but no data_path specified and no common key found",
                    context={"response_keys": list(response_json.keys())},
                )
            raise ConnectorError(
                "Response is not a list or dict with extractable data",
                context={"response_type": type(response_json).__name__},
            )

        # Use JSONPath to extract data
        try:
            jsonpath_expr = parse_jsonpath(self._data_path)
            matches = jsonpath_expr.find(response_json)
            if not matches:
                return []
            # Get the first match (JSONPath can return multiple)
            result = matches[0].value
            if isinstance(result, list):
                return result
            elif isinstance(result, dict):
                # If result is a dict, try to extract array from it
                for key in ["data", "results", "items", "records"]:
                    if key in result and isinstance(result[key], list):
                        return result[key]
                # If no array is found, return the dict wrapped in a list so downstream
                # transforms can handle dict-of-arrays cases.
                return [result]
            else:
                raise ConnectorError(
                    "JSONPath result is not a list or dict",
                    context={
                        "data_path": self._data_path,
                        "result_type": type(result).__name__,
                    },
                )
        except Exception as e:
            raise ConnectorError(
                f"Failed to extract data using JSONPath: {e}",
                context={"data_path": self._data_path},
            ) from e

    def _get_total(self, response_json: dict) -> int | None:
        """Extract total count from JSON response using total_path.

        Args:
            response_json: Parsed JSON response (must be dict).

        Returns:
            Total count if available, None otherwise.
        """
        if not isinstance(response_json, dict):
            return None

        if self._total_path is None:
            # Try common keys
            for key in ["total", "count", "total_count", "pagination.total"]:
                if key in response_json:
                    value = response_json[key]
                    if isinstance(value, int):
                        return value
            return None

        # Use JSONPath to extract total
        try:
            jsonpath_expr = parse_jsonpath(self._total_path)
            matches = jsonpath_expr.find(response_json)
            if matches:
                value = matches[0].value
                if isinstance(value, int):
                    return value
        except Exception:
            # If JSONPath fails, return None (total is optional)
            pass

        return None

    def _make_request(self, url: str, attempt: int = 0) -> requests.Response:
        """Make HTTP GET request with retry logic and exponential backoff.

        Args:
            url: Complete URL to request.
            attempt: Current retry attempt number.

        Returns:
            requests.Response object.

        Raises:
            ConnectorError: If request fails after all retries.
        """
        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
            return response
        except Timeout as e:
            # Timeout errors - retry with exponential backoff
            if attempt < self._max_retries:
                delay = self._retry_delay * (self._backoff_rate**attempt)
                time.sleep(delay)
                return self._make_request(url, attempt + 1)
            raise ConnectorError(
                f"Request timeout after {self._max_retries + 1} attempts",
                context={"url": url, "timeout": self._timeout},
            ) from e
        except RequestException as e:
            # Check if it's a client error (4xx) - don't retry
            if hasattr(e, "response") and e.response is not None:
                status_code = e.response.status_code
                if 400 <= status_code < 500:
                    raise ConnectorError(
                        f"Client error {status_code}: {e}",
                        context={"url": url, "status_code": status_code},
                    ) from e

            # Connection errors or other request exceptions - retry with backoff
            # Note: 5xx errors are handled by urllib3 Retry automatically
            if attempt < self._max_retries:
                delay = self._retry_delay * (self._backoff_rate**attempt)
                time.sleep(delay)
                return self._make_request(url, attempt + 1)

            raise ConnectorError(
                f"Request failed after {self._max_retries + 1} attempts: {e}",
                context={"url": url},
            ) from e

    def read_batches(self, state: State) -> Iterable[ArrowBatch]:
        """Read data from API as batches.

        Supports pagination and incremental loading via state cursor values.

        Args:
            state: Current state containing cursor values for pagination.

        Yields:
            ArrowBatch instances containing the data.

        Raises:
            ConnectorError: If connection or request fails.
            NotImplementedError: API connector is read-only.
        """
        # Determine starting pagination value from state
        if self._pagination_type == "page":
            current_page = state.cursor_values.get("page", 1)
            current_offset = None
        elif self._pagination_type == "offset":
            current_page = None
            current_offset = state.cursor_values.get("offset", 0)
        else:  # cursor
            current_page = None
            current_offset = None
            # Cursor value should be in params or state

        total_records: int | None = None
        batch_number = 0

        while True:
            # Build URL with pagination
            url = self._build_url(page=current_page, offset=current_offset)

            # Make request with retry logic
            try:
                response = self._make_request(url)
                response_json = response.json()
            except Exception as e:
                raise ConnectorError(
                    f"Failed to fetch data from API: {e}",
                    context={"url": url},
                ) from e

            # Extract data array
            try:
                data = self._extract_data(response_json)
            except ConnectorError as e:
                raise ConnectorError(
                    f"Failed to extract data from response: {e}",
                    context={"url": url},
                ) from e

            # If no data, we're done
            if not data:
                break

            # Get total if available (for validation)
            if total_records is None and isinstance(response_json, dict):
                total_records = self._get_total(response_json)

            # Convert to Arrow Table
            try:
                # Convert list of dicts to Arrow Table
                arrow_table = pa.Table.from_pylist(data)

                # Cache schema from first batch for consistency
                if self._schema_cache is None:
                    self._schema_cache = arrow_table.schema
                else:
                    # Ensure schema consistency (cast if needed)
                    # For now, we'll use the schema from first batch
                    # In future, we could implement schema evolution here
                    pass

                batch_number += 1
                yield ArrowBatch(
                    arrow_table,
                    metadata={
                        "batch_number": batch_number,
                        "row_count": len(data),
                        "source_type": "api",
                        "url": url,
                        "page": current_page,
                        "offset": current_offset,
                        "total_records": total_records,
                    },
                )
            except Exception as e:
                raise ConnectorError(
                    f"Failed to convert data to Arrow format: {e}",
                    context={"url": url, "data_count": len(data)},
                ) from e

            # Update pagination for next iteration
            if self._pagination_type == "page":
                current_page = (current_page or 1) + 1
            elif self._pagination_type == "offset":
                current_offset = (current_offset or 0) + len(data)

            # Check if we've reached the total (if available)
            if total_records is not None:
                # Calculate how many records we've processed so far
                # This is approximate since we don't track across batches
                # For exact tracking, we'd need to accumulate row counts
                if current_page and current_page * self._page_size >= total_records:
                    break
                if current_offset is not None and current_offset >= total_records:
                    break

            # Safety check: if we got fewer records than page_size, we're likely done
            if len(data) < self._page_size:
                break

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write batch to destination.

        API connector is read-only and does not support writing.

        Args:
            batch: The batch of data to write.
            state: Current state.

        Raises:
            NotImplementedError: API connector is read-only.
        """
        raise NotImplementedError("ApiConnector does not support write operations")


@register_connector("api")
def create_api_connector(config: ConnectorConfigUnion) -> ApiConnector:
    """Factory function to create an ApiConnector instance.

    Args:
        config: Connector configuration.

    Returns:
        ApiConnector instance.
    """
    return ApiConnector(config)
