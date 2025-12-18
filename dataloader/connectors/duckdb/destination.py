"""DuckDB destination connector for batch writes."""

from typing import Any

import duckdb
from duckdb import DuckDBPyConnection

from dataloader.connectors.registry import register_destination
from dataloader.core.batch import Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig

# Type mapping from batch column types to DuckDB types
DUCKDB_TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "float": "DOUBLE",
    "datetime": "TIMESTAMP",
    "bool": "BOOLEAN",
    "date": "DATE",
}


class DuckDBDestination:
    """Destination connector for DuckDB database.

    Supports writing batches to DuckDB tables with automatic schema creation
    and evolution. Uses file-based or in-memory databases.
    """

    def __init__(self, config: DestinationConfig, connection: dict[str, Any]):
        """Initialize DuckDB destination.

        Args:
            config: Destination configuration with database path and table name.
            connection: Connection parameters. Supports:
                - database: Override database path (file path or ':memory:')
        """
        self._config = config
        self._connection = connection
        self._database = connection.get("database") or config.database
        self._table = config.table
        self._schema = config.db_schema
        self._write_mode = config.write_mode

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
            ConnectorError: If connection, table creation, or insert fails.
        """
        conn = self._get_connection()

        self._handle_write_mode(conn, batch)
        self._insert_batch(conn, batch)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self) -> None:
        """Ensure connection is closed on garbage collection."""
        self.close()


@register_destination("duckdb")
def create_duckdb_destination(
    config: DestinationConfig, connection: dict[str, Any]
) -> DuckDBDestination:
    """Factory function for creating DuckDBDestination instances."""
    return DuckDBDestination(config, connection)

