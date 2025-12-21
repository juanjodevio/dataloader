"""DuckDB connector for reading and writing data."""

from typing import Any, Iterable, Union

import duckdb
from duckdb import DuckDBPyConnection

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector
from dataloader.core.batch import Batch, DictBatch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import DuckDBConnectorConfig

# Type mapping from batch column types to DuckDB types
DUCKDB_TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "float": "DOUBLE",
    "datetime": "TIMESTAMP",
    "bool": "BOOLEAN",
    "date": "DATE",
}

# Default batch size for reading
DEFAULT_BATCH_SIZE = 1000


class DuckDBConnector:
    """Unified connector for DuckDB database.

    Supports both reading and writing operations. Uses file-based or
    in-memory databases with automatic schema creation and evolution.
    """

    def __init__(
        self,
        config: Union[DuckDBConnectorConfig, SourceConfig, DestinationConfig],
        connection: dict[str, Any] | None = None,
    ):
        """Initialize DuckDBConnector.

        Args:
            config: DuckDB connector configuration (DuckDBConnectorConfig, SourceConfig, or DestinationConfig).
            connection: Optional connection parameters (legacy, defaults to empty dict).
                All configuration should be in the config parameter. This parameter is kept
                for backward compatibility with the factory signature.
        """
        self._config = config
        self._connection = connection or {}

        # Extract config values (support both new and legacy configs)
        if isinstance(config, DuckDBConnectorConfig):
            self._database = config.database
            self._table = config.table
            self._schema = config.db_schema
            self._write_mode = config.write_mode
            self._merge_keys = config.merge_keys
        elif isinstance(config, SourceConfig):
            self._database = config.database or ":memory:"
            self._table = config.table or ""
            self._schema = config.db_schema
            self._write_mode = "append"  # Default for source configs
            self._merge_keys = None
        else:  # DestinationConfig
            self._database = connection.get("database") or config.database or ":memory:"
            self._table = config.table or ""
            self._schema = config.db_schema
            self._write_mode = config.write_mode
            self._merge_keys = config.merge_keys

        self._batch_size = self._connection.get("batch_size", DEFAULT_BATCH_SIZE)
        self._conn: DuckDBPyConnection | None = None
        self._table_created = False

    @property
    def _qualified_table(self) -> str:
        """Return fully qualified table name."""
        if self._schema:
            return f'"{self._schema}"."{self._table}"'
        return f'"{self._table}"'

    def _get_connection(self) -> DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self._conn is None:
            try:
                self._conn = duckdb.connect(self._database)
            except duckdb.Error as e:
                raise ConnectorError(
                    f"Failed to connect to DuckDB: {e}",
                    context={"database": self._database},
                ) from e
        return self._conn

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self) -> None:
        """Ensure connection is closed on garbage collection."""
        self.close()

    # ========== Reading methods ==========

    def _get_schema(self) -> list[tuple[str, str]]:
        """Fetch column names and types from DuckDB table.

        Returns:
            List of (column_name, data_type) tuples.
        """
        conn = self._get_connection()
        try:
            result = conn.execute(f"DESCRIBE {self._qualified_table}")
            return [(row[0], row[1]) for row in result.fetchall()]
        except duckdb.CatalogException as e:
            raise ConnectorError(
                f"Table does not exist: {e}",
                context={"table": self._table, "schema": self._schema},
            ) from e

    def _build_query(self, state: State) -> tuple[str, list[Any]]:
        """Build SELECT query with optional cursor-based filtering.

        Args:
            state: Current state containing cursor values.

        Returns:
            Tuple of (query_string, parameters) for parameterized query.
        """
        query_parts = [f"SELECT * FROM {self._qualified_table}"]
        params: list[Any] = []

        # Apply cursor-based filtering for incremental loads
        incremental = getattr(self._config, "incremental", None)
        if incremental and incremental.cursor_column:
            cursor_column = incremental.cursor_column
            cursor_value = state.cursor_values.get(cursor_column)

            if cursor_value is not None:
                query_parts.append(f'WHERE "{cursor_column}" > ?')
                params.append(cursor_value)

            # Always order by cursor column for consistent pagination
            query_parts.append(f'ORDER BY "{cursor_column}"')

        return " ".join(query_parts), params

    def read_batches(self, state: State) -> Iterable[DictBatch]:
        """Read data from DuckDB table as batches.

        Uses DuckDB's fetchmany for memory-efficient batch reading.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            DictBatch instances containing the data.

        Raises:
            NotImplementedError: If reading is not supported (should not happen for DuckDB).
            ConnectorError: If connection or query fails.
        """
        try:
            conn = self._get_connection()
            schema_info = self._get_schema()
            columns = [col[0] for col in schema_info]
            column_types = {col[0]: col[1] for col in schema_info}

            query, params = self._build_query(state)

            # Use parameterized query for safety
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)

            batch_number = 0
            while True:
                rows = result.fetchmany(self._batch_size)
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
                        "source_type": "duckdb",
                        "table": self._table,
                        "schema": self._schema,
                        "column_types": column_types,
                    },
                )

        except ConnectorError:
            raise
        except duckdb.Error as e:
            raise ConnectorError(
                f"Failed to read from DuckDB: {e}",
                context={
                    "table": self._table,
                    "schema": self._schema,
                    "database": self._database,
                },
            ) from e

    # ========== Writing methods ==========

    def _map_type(self, batch_type: str) -> str:
        """Map batch column type to DuckDB type."""
        return DUCKDB_TYPE_MAP.get(batch_type, "VARCHAR")

    def _infer_column_type(self, value: Any) -> str:
        """Infer DuckDB type from a Python value."""
        if value is None:
            return "VARCHAR"
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "DOUBLE"
        return "VARCHAR"

    def _get_existing_columns(self, conn: DuckDBPyConnection) -> set[str]:
        """Get existing columns for the table."""
        try:
            result = conn.execute(f"DESCRIBE {self._qualified_table}")
            return {row[0] for row in result.fetchall()}
        except duckdb.CatalogException:
            return set()

    def _create_table(
        self, conn: DuckDBPyConnection, batch: Batch
    ) -> None:
        """Create table from batch schema if it doesn't exist."""
        # Try to get column types from batch metadata
        column_types = batch.metadata.get("column_types", {})

        # Build column definitions
        column_defs = []
        for i, col in enumerate(batch.columns):
            if col in column_types:
                duckdb_type = self._map_type(column_types[col])
            elif batch.rows:
                # Infer from first row value
                duckdb_type = self._infer_column_type(batch.rows[0][i])
            else:
                duckdb_type = "VARCHAR"
            column_defs.append(f'"{col}" {duckdb_type}')

        columns_sql = ", ".join(column_defs)

        try:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {self._qualified_table} ({columns_sql})"
            )
        except duckdb.Error as e:
            raise ConnectorError(
                f"Failed to create table: {e}",
                context={"table": self._table, "columns": batch.columns},
            ) from e

    def _add_missing_columns(
        self, conn: DuckDBPyConnection, batch: Batch
    ) -> None:
        """Add columns that exist in batch but not in table (schema evolution)."""
        existing = self._get_existing_columns(conn)
        column_types = batch.metadata.get("column_types", {})

        for i, col in enumerate(batch.columns):
            if col not in existing:
                if col in column_types:
                    duckdb_type = self._map_type(column_types[col])
                elif batch.rows:
                    duckdb_type = self._infer_column_type(batch.rows[0][i])
                else:
                    duckdb_type = "VARCHAR"

                try:
                    conn.execute(
                        f'ALTER TABLE {self._qualified_table} ADD COLUMN "{col}" {duckdb_type}'
                    )
                except duckdb.Error as e:
                    raise ConnectorError(
                        f"Failed to add column '{col}': {e}",
                        context={"table": self._table, "column": col},
                    ) from e

    def _handle_write_mode(self, conn: DuckDBPyConnection, batch: Batch) -> None:
        """Handle write mode logic before inserting."""
        if self._write_mode == "overwrite" and not self._table_created:
            # Drop and recreate table
            try:
                conn.execute(f"DROP TABLE IF EXISTS {self._qualified_table}")
            except duckdb.Error as e:
                raise ConnectorError(
                    f"Failed to drop table for overwrite: {e}",
                    context={"table": self._table},
                ) from e
            self._create_table(conn, batch)
            self._table_created = True

        elif self._write_mode == "merge":
            # Merge not supported in v0.1
            raise ConnectorError(
                "Merge write mode is not supported in v0.1. Use 'append' or 'overwrite'.",
                context={"table": self._table, "write_mode": self._write_mode},
            )

        elif self._write_mode == "append":
            # Ensure table exists and handle schema evolution
            existing = self._get_existing_columns(conn)
            if not existing:
                self._create_table(conn, batch)
            else:
                self._add_missing_columns(conn, batch)
            self._table_created = True

    def _insert_batch(self, conn: DuckDBPyConnection, batch: Batch) -> None:
        """Insert batch rows using parameterized query."""
        if not batch.rows:
            return

        columns = ", ".join(f'"{col}"' for col in batch.columns)
        placeholders = ", ".join("?" for _ in batch.columns)
        insert_sql = f"INSERT INTO {self._qualified_table} ({columns}) VALUES ({placeholders})"

        try:
            conn.executemany(insert_sql, batch.rows)
        except duckdb.Error as e:
            raise ConnectorError(
                f"Failed to insert batch: {e}",
                context={
                    "table": self._table,
                    "row_count": batch.row_count,
                    "columns": batch.columns,
                },
            ) from e

    def write_batch(self, batch: Batch, state: State) -> None:
        """Write a batch to DuckDB table.

        Creates the table if it doesn't exist, handles schema evolution
        for new columns, and inserts rows using parameterized queries.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state (unused for DuckDB but kept for protocol).

        Raises:
            NotImplementedError: If writing is not supported (should not happen for DuckDB).
            ConnectorError: If connection, table creation, or insert fails.
        """
        conn = self._get_connection()

        self._handle_write_mode(conn, batch)
        self._insert_batch(conn, batch)


@register_connector("duckdb")
def create_duckdb_connector(
    config: ConnectorConfigUnion, connection: dict[str, Any] | None = None
) -> DuckDBConnector:
    """Factory function for creating DuckDBConnector instances."""
    return DuckDBConnector(config, connection)

