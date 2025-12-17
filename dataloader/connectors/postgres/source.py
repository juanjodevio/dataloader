"""PostgreSQL source connector using SQLAlchemy."""

from typing import Any, Iterable

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import SourceConfig


class PostgresSource:
    """Source connector for PostgreSQL databases using SQLAlchemy.

    Uses SQLAlchemy for database abstraction, enabling support for
    multiple database dialects (Postgres, MySQL, Redshift, etc.)
    with the same interface.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_PORT = 5432
    DIALECT = "postgresql+psycopg2"

    def __init__(self, config: SourceConfig, connection: dict[str, Any]):
        """Initialize PostgresSource.

        Args:
            config: Source configuration containing table and incremental settings.
            connection: Connection parameters (host, port, database, user, password).
        """
        self._config = config
        self._connection = connection
        self._batch_size = connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._engine: Engine | None = None

    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL from config and connection dict."""
        host = self._connection.get("host") or self._config.host
        port = self._connection.get("port") or self._config.port or self.DEFAULT_PORT
        database = self._connection.get("database") or self._config.database
        user = self._connection.get("user") or self._config.user
        password = self._connection.get("password") or self._config.password

        # Use custom dialect if specified (e.g., for Redshift, MySQL)
        dialect = self._connection.get("dialect", self.DIALECT)

        # Build URL: dialect://user:password@host:port/database
        if password:
            return f"{dialect}://{user}:{password}@{host}:{port}/{database}"
        return f"{dialect}://{user}@{host}:{port}/{database}"

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            try:
                url = self._build_connection_url()
                # Pool settings for batch reading
                self._engine = create_engine(
                    url,
                    pool_pre_ping=True,
                    pool_size=1,
                    max_overflow=0,
                )
            except SQLAlchemyError as e:
                raise ConnectorError(
                    f"Failed to create database engine: {e}",
                    context={
                        "host": self._connection.get("host") or self._config.host,
                        "database": self._connection.get("database") or self._config.database,
                    },
                ) from e
        return self._engine

    def _close(self) -> None:
        """Dispose of the engine and close connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def _get_schema(self) -> list[tuple[str, str]]:
        """Fetch column names and types using SQLAlchemy inspector.

        Returns:
            List of (column_name, data_type) tuples.
        """
        engine = self._get_engine()
        inspector = inspect(engine)

        schema = self._config.db_schema or "public"
        table = self._config.table

        columns = inspector.get_columns(table, schema=schema)
        return [(col["name"], str(col["type"])) for col in columns]

    def _build_query(self, state: State) -> tuple[str, dict[str, Any]]:
        """Build SELECT query with optional cursor-based filtering.

        Args:
            state: Current state containing cursor values.

        Returns:
            Tuple of (query_string, parameters).
        """
        schema = self._config.db_schema or "public"
        table = self._config.table
        qualified_table = f'"{schema}"."{table}"'

        query_parts = [f"SELECT * FROM {qualified_table}"]
        params: dict[str, Any] = {}

        # Apply cursor-based filtering for incremental loads
        incremental = self._config.incremental
        if incremental and incremental.cursor_column:
            cursor_column = incremental.cursor_column
            cursor_value = state.cursor_values.get(cursor_column)

            if cursor_value is not None:
                query_parts.append(f'WHERE "{cursor_column}" > :cursor_value')
                params["cursor_value"] = cursor_value

            # Always order by cursor column for consistent pagination
            query_parts.append(f'ORDER BY "{cursor_column}"')

        return " ".join(query_parts), params

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read data from PostgreSQL table as batches.

        Uses SQLAlchemy's streaming result for memory-efficient batch reading.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            ConnectorError: If connection or query fails.
        """
        try:
            engine = self._get_engine()
            schema_info = self._get_schema()
            columns = [col[0] for col in schema_info]
            column_types = {col[0]: col[1] for col in schema_info}

            query, params = self._build_query(state)

            with engine.connect() as conn:
                # Use stream_results for memory-efficient reading
                result = conn.execution_options(stream_results=True).execute(
                    text(query), params
                )

                batch_number = 0
                while True:
                    rows = result.fetchmany(self._batch_size)
                    if not rows:
                        break

                    # Convert Row objects to lists
                    row_data = [list(row) for row in rows]

                    batch_number += 1
                    yield DictBatch(
                        columns=columns,
                        rows=row_data,
                        metadata={
                            "batch_number": batch_number,
                            "row_count": len(row_data),
                            "source_type": "postgres",
                            "table": self._config.table,
                            "schema": self._config.db_schema or "public",
                            "column_types": column_types,
                        },
                    )

        except SQLAlchemyError as e:
            raise ConnectorError(
                f"Failed to read from database: {e}",
                context={
                    "table": self._config.table,
                    "schema": self._config.db_schema or "public",
                },
            ) from e
        finally:
            self._close()


def create_postgres_source(config: SourceConfig, connection: dict[str, Any]) -> PostgresSource:
    """Factory function for creating PostgresSource instances."""
    return PostgresSource(config, connection)
