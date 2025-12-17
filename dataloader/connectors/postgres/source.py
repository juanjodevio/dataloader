"""PostgreSQL source connector for reading data in batches."""

from typing import Any, Iterable

import psycopg2
from psycopg2.extras import RealDictCursor

from dataloader.core.batch import DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.source_config import SourceConfig


class PostgresSource:
    """Source connector for PostgreSQL databases.

    Reads data from a PostgreSQL table in batches, supporting cursor-based
    incremental reads via the state's cursor_values.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_PORT = 5432

    def __init__(self, config: SourceConfig, connection: dict[str, Any]):
        """Initialize PostgresSource.

        Args:
            config: Source configuration containing table and incremental settings.
            connection: Connection parameters (host, port, database, user, password).
        """
        self._config = config
        self._connection_params = self._build_connection_params(config, connection)
        self._batch_size = connection.get("batch_size", self.DEFAULT_BATCH_SIZE)
        self._conn: Any = None

    def _build_connection_params(
        self, config: SourceConfig, connection: dict[str, Any]
    ) -> dict[str, Any]:
        """Build psycopg2 connection parameters from config and connection dict."""
        # Merge config fields with connection dict (connection takes precedence)
        return {
            "host": connection.get("host") or config.host,
            "port": connection.get("port") or config.port or self.DEFAULT_PORT,
            "dbname": connection.get("database") or config.database,
            "user": connection.get("user") or config.user,
            "password": connection.get("password") or config.password,
        }

    def _connect(self) -> None:
        """Establish database connection."""
        if self._conn is not None:
            return

        try:
            self._conn = psycopg2.connect(**self._connection_params)
        except psycopg2.Error as e:
            raise ConnectorError(
                f"Failed to connect to PostgreSQL: {e}",
                context={
                    "host": self._connection_params.get("host"),
                    "port": self._connection_params.get("port"),
                    "database": self._connection_params.get("dbname"),
                },
            ) from e

    def _close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _get_schema(self) -> list[tuple[str, str]]:
        """Fetch column names and types from information_schema.

        Returns:
            List of (column_name, data_type) tuples.
        """
        schema = self._config.db_schema or "public"
        table = self._config.table

        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        with self._conn.cursor() as cursor:
            cursor.execute(query, (schema, table))
            return cursor.fetchall()

    def _build_query(self, state: State) -> tuple[str, list[Any]]:
        """Build SELECT query with optional cursor-based filtering.

        Args:
            state: Current state containing cursor values.

        Returns:
            Tuple of (query_string, parameters).
        """
        schema = self._config.db_schema or "public"
        table = self._config.table
        qualified_table = f'"{schema}"."{table}"'

        # Start building the query
        query_parts = [f"SELECT * FROM {qualified_table}"]
        params: list[Any] = []

        # Apply cursor-based filtering for incremental loads
        incremental = self._config.incremental
        if incremental and incremental.cursor_column:
            cursor_column = incremental.cursor_column
            cursor_value = state.cursor_values.get(cursor_column)

            if cursor_value is not None:
                query_parts.append(f'WHERE "{cursor_column}" > %s')
                params.append(cursor_value)

            # Always order by cursor column for consistent pagination
            query_parts.append(f'ORDER BY "{cursor_column}"')

        return " ".join(query_parts), params

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read data from PostgreSQL table as batches.

        Uses server-side cursors for memory-efficient batch reading.
        For incremental loads, filters by cursor_column > last cursor value.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            ConnectorError: If connection or query fails.
        """
        try:
            self._connect()
            schema_info = self._get_schema()
            columns = [col[0] for col in schema_info]
            column_types = {col[0]: col[1] for col in schema_info}

            query, params = self._build_query(state)

            # Use server-side cursor for efficient batch reading
            cursor_name = f"dataloader_cursor_{id(self)}"
            with self._conn.cursor(name=cursor_name) as cursor:
                cursor.itersize = self._batch_size
                cursor.execute(query, params)

                batch_number = 0
                while True:
                    rows = cursor.fetchmany(self._batch_size)
                    if not rows:
                        break

                    # Convert rows to list format
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

        except psycopg2.Error as e:
            raise ConnectorError(
                f"Failed to read from PostgreSQL: {e}",
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

