"""Unit tests for ApiConnector."""

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from dataloader.connectors.api.config import ApiConnectorConfig
from dataloader.connectors.api.connector import (
    ApiConnector,
    create_api_connector,
)
from dataloader.core.batch import ArrowBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import IncrementalConfig, SourceConfig


class TestApiConnector:
    """Tests for ApiConnector."""

    @pytest.fixture
    def api_config(self) -> ApiConnectorConfig:
        """Create a sample API connector config."""
        return ApiConnectorConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            page_size=10,
        )

    @pytest.fixture
    def api_source_config(self) -> SourceConfig:
        """Create a sample API source config."""
        return SourceConfig(
            type="api",
            base_url="https://api.example.com",
            endpoint="/data",
            page_size=10,
        )

    def test_api_connector_initialization(self, api_config: ApiConnectorConfig):
        """Test that ApiConnector initializes correctly."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                assert connector._base_url == "https://api.example.com"
                assert connector._endpoint == "/data"
                assert connector._page_size == 10
                assert connector._pagination_type == "page"
                assert connector._auth_type == "none"

    def test_api_connector_initialization_from_source_config(
        self, api_source_config: SourceConfig
    ):
        """Test that ApiConnector initializes from SourceConfig."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_source_config)

                assert connector._base_url == "https://api.example.com"
                assert connector._endpoint == "/data"
                assert connector._page_size == 10

    def test_api_connector_missing_dependencies(self, api_config: ApiConnectorConfig):
        """Test that ApiConnector raises ImportError when dependencies are missing."""
        with patch("dataloader.connectors.api.connector.requests", None):
            with pytest.raises(ImportError, match="requests"):
                ApiConnector(api_config)

        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath", None):
                with pytest.raises(ImportError, match="jsonpath-ng"):
                    ApiConnector(api_config)

    def test_api_connector_missing_required_fields(self):
        """Test that ApiConnector validates required fields."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                # Missing base_url
                with pytest.raises(ValueError, match="base_url"):
                    ApiConnector(ApiConnectorConfig(endpoint="/data"))

                # Missing endpoint
                with pytest.raises(ValueError, match="endpoint"):
                    ApiConnector(ApiConnectorConfig(base_url="https://api.example.com"))

    def test_api_connector_auth_bearer(self):
        """Test bearer token authentication."""
        with patch("dataloader.connectors.api.connector.requests.Session") as mock_session_class:
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                # Create a real dict for headers to test actual behavior
                mock_session = Mock()
                mock_session.headers = {}
                mock_session_class.return_value = mock_session

                config = ApiConnectorConfig(
                    base_url="https://api.example.com",
                    endpoint="/data",
                    auth_type="bearer",
                    auth_token="test-token",
                )
                connector = ApiConnector(config)

                # Check that Authorization header was set
                assert "Authorization" in connector._session.headers
                assert connector._session.headers["Authorization"] == "Bearer test-token"

    def test_api_connector_auth_bearer_missing_token(self):
        """Test that bearer auth requires token."""
        with pytest.raises(Exception) as exc_info:
            ApiConnectorConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                auth_type="bearer",
            )
        # Pydantic validation error
        assert "auth_token" in str(exc_info.value)

    def test_api_connector_auth_basic(self):
        """Test basic authentication."""
        with patch("dataloader.connectors.api.connector.requests.Session") as mock_session_class:
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                with patch("dataloader.connectors.api.connector.requests.auth.HTTPBasicAuth") as mock_auth_class:
                    # Create a real dict for headers to test actual behavior
                    mock_session = Mock()
                    mock_session.headers = {}
                    mock_session.auth = None
                    mock_session_class.return_value = mock_session

                    # Mock HTTPBasicAuth to return a mock object
                    mock_auth_instance = Mock()
                    mock_auth_class.return_value = mock_auth_instance

                    config = ApiConnectorConfig(
                        base_url="https://api.example.com",
                        endpoint="/data",
                        auth_type="basic",
                        auth_username="user",
                        auth_password="pass",
                    )
                    connector = ApiConnector(config)

                    assert connector._session.auth is not None
                    # Verify HTTPBasicAuth was called with credentials
                    mock_auth_class.assert_called_once_with("user", "pass")

    def test_api_connector_auth_basic_missing_credentials(self):
        """Test that basic auth requires username and password."""
        # Missing username - Pydantic validation error
        with pytest.raises(Exception) as exc_info:
            ApiConnectorConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                auth_type="basic",
                auth_password="pass",
            )
        assert "auth_username" in str(exc_info.value)

        # Missing password - Pydantic validation error
        with pytest.raises(Exception) as exc_info:
            ApiConnectorConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                auth_type="basic",
                auth_username="user",
            )
        assert "auth_password" in str(exc_info.value)

    def test_build_url(self, api_config: ApiConnectorConfig):
        """Test URL building with parameters."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                # Basic URL
                url = connector._build_url()
                assert url == "https://api.example.com/data"

                # With page
                url = connector._build_url(page=2)
                assert "page=2" in url

                # With params
                connector._params = {"filter": "active"}
                url = connector._build_url(page=1)
                assert "filter=active" in url
                assert "page=1" in url

    def test_build_url_with_limit_param(self):
        """Test URL building with limit parameter."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                config = ApiConnectorConfig(
                    base_url="https://api.example.com",
                    endpoint="/data",
                    limit_param="limit",
                    page_size=20,
                )
                connector = ApiConnector(config)

                url = connector._build_url(page=1)
                assert "limit=20" in url

    def test_extract_data_from_list(self, api_config: ApiConnectorConfig):
        """Test extracting data from list response."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                response = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
                data = connector._extract_data(response)

                assert len(data) == 2
                assert data[0]["id"] == 1

    def test_extract_data_from_dict_with_common_key(self, api_config: ApiConnectorConfig):
        """Test extracting data from dict with common key."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                response = {"data": [{"id": 1}, {"id": 2}]}
                data = connector._extract_data(response)

                assert len(data) == 2

    def test_extract_data_with_jsonpath(self):
        """Test extracting data using JSONPath."""
        with patch("dataloader.connectors.api.connector.requests"):
            mock_parse = MagicMock()
            mock_expr = MagicMock()
            mock_match = MagicMock()
            mock_match.value = [{"id": 1}, {"id": 2}]
            mock_expr.find.return_value = [mock_match]
            mock_parse.return_value = mock_expr

            with patch("dataloader.connectors.api.connector.parse_jsonpath", mock_parse):
                config = ApiConnectorConfig(
                    base_url="https://api.example.com",
                    endpoint="/data",
                    data_path="results.items",
                )
                connector = ApiConnector(config)

                response = {"results": {"items": [{"id": 1}, {"id": 2}]}}
                data = connector._extract_data(response)

                assert len(data) == 2
                mock_parse.assert_called_once_with("results.items")

    def test_extract_data_error_no_path(self, api_config: ApiConnectorConfig):
        """Test that extract_data raises error when no data_path and no common key."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                response = {"other": "value"}
                with pytest.raises(ConnectorError, match="data_path"):
                    connector._extract_data(response)

    def test_get_total_from_dict(self, api_config: ApiConnectorConfig):
        """Test extracting total count from response."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)

                response = {"total": 100, "data": []}
                total = connector._get_total(response)

                assert total == 100

    def test_get_total_with_jsonpath(self):
        """Test extracting total using JSONPath."""
        with patch("dataloader.connectors.api.connector.requests"):
            mock_parse = MagicMock()
            mock_expr = MagicMock()
            mock_match = MagicMock()
            mock_match.value = 200
            mock_expr.find.return_value = [mock_match]
            mock_parse.return_value = mock_expr

            with patch("dataloader.connectors.api.connector.parse_jsonpath", mock_parse):
                config = ApiConnectorConfig(
                    base_url="https://api.example.com",
                    endpoint="/data",
                    total_path="pagination.total",
                )
                connector = ApiConnector(config)

                response = {"pagination": {"total": 200}}
                total = connector._get_total(response)

                assert total == 200

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_single_page(
        self, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test reading a single page of data."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        }
        mock_response.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(api_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        assert batches[0].row_count == 2
        assert batches[0].columns == ["id", "name"]

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_pagination_page(
        self, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test reading multiple pages with page-based pagination."""
        # Mock responses for two pages
        mock_response1 = Mock()
        mock_response1.json.return_value = {
            "data": [{"id": i} for i in range(1, 11)]  # 10 items
        }
        mock_response1.raise_for_status = Mock()

        mock_response2 = Mock()
        mock_response2.json.return_value = {
            "data": [{"id": i} for i in range(11, 16)]  # 5 items (less than page_size)
        }
        mock_response2.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.side_effect = [mock_response1, mock_response2]
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(api_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 2
        assert batches[0].row_count == 10
        assert batches[1].row_count == 5
        # Verify pagination parameters were used
        assert mock_session.get.call_count == 2
        assert "page=1" in mock_session.get.call_args_list[0][0][0]
        assert "page=2" in mock_session.get.call_args_list[1][0][0]

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_pagination_offset(
        self, mock_parse, mock_session_class
    ):
        """Test reading with offset-based pagination."""
        config = ApiConnectorConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            pagination_type="offset",
            page_param="offset",
            page_size=10,
        )

        # Mock responses
        mock_response1 = Mock()
        mock_response1.json.return_value = {
            "data": [{"id": i} for i in range(1, 11)]
        }
        mock_response1.raise_for_status = Mock()

        mock_response2 = Mock()
        mock_response2.json.return_value = {"data": []}  # Empty - stop
        mock_response2.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.side_effect = [mock_response1, mock_response2]
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        # Verify offset was used
        assert "offset=0" in mock_session.get.call_args_list[0][0][0]

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_incremental_state(
        self, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test reading with incremental state (resume from page)."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"data": [{"id": 1}]}
        mock_response.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(api_config)
        # State with page cursor
        state = State(cursor_values={"page": 3})

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        # Verify it started from page 3
        assert "page=3" in mock_session.get.call_args_list[0][0][0]

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_empty_response(
        self, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test reading when response is empty."""
        # Mock empty response
        mock_response = Mock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(api_config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 0

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    def test_read_batches_http_error(
        self, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test handling of HTTP errors."""
        from requests.exceptions import HTTPError

        # Mock error response
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = HTTPError("404 Not Found")
        http_error.response = mock_response
        mock_response.raise_for_status.side_effect = http_error

        # Mock session
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(api_config)
        state = State()

        with pytest.raises(ConnectorError) as exc_info:
            list(connector.read_batches(state))
        # Should contain either "Client error" or "Failed to fetch"
        assert "Client error" in str(exc_info.value) or "Failed to fetch" in str(exc_info.value)

    @patch("dataloader.connectors.api.connector.requests.Session")
    @patch("dataloader.connectors.api.connector.parse_jsonpath")
    @patch("dataloader.connectors.api.connector.time.sleep")
    def test_read_batches_retry_with_backoff(
        self, mock_sleep, mock_parse, mock_session_class, api_config: ApiConnectorConfig
    ):
        """Test retry logic with exponential backoff."""
        from requests.exceptions import Timeout

        config = ApiConnectorConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            max_retries=2,
            retry_delay=1.0,
            backoff_rate=2.0,
        )

        # Mock responses: first fails with timeout, second succeeds
        mock_response_error = Mock()
        mock_response_error.raise_for_status.side_effect = Timeout("Timeout")

        mock_response_success = Mock()
        mock_response_success.json.return_value = {"data": [{"id": 1}]}
        mock_response_success.raise_for_status = Mock()

        # Mock session
        mock_session = Mock()
        mock_session.get.side_effect = [
            Timeout("Timeout"),
            mock_response_success,
        ]
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        connector = ApiConnector(config)
        state = State()

        batches = list(connector.read_batches(state))

        assert len(batches) == 1
        # Verify retry was attempted (2 calls: initial + retry)
        assert mock_session.get.call_count == 2
        # Verify exponential backoff was used
        assert mock_sleep.call_count == 1
        # First retry delay should be retry_delay * (backoff_rate ^ 0) = 1.0
        mock_sleep.assert_called_with(1.0)

    def test_write_batch_not_implemented(self, api_config: ApiConnectorConfig):
        """Test that write_batch raises NotImplementedError."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = ApiConnector(api_config)
                batch = Mock(spec=ArrowBatch)
                state = State()

                with pytest.raises(NotImplementedError):
                    connector.write_batch(batch, state)

    def test_create_api_connector(self, api_config: ApiConnectorConfig):
        """Test factory function."""
        with patch("dataloader.connectors.api.connector.requests"):
            with patch("dataloader.connectors.api.connector.parse_jsonpath"):
                connector = create_api_connector(api_config)

                assert isinstance(connector, ApiConnector)

