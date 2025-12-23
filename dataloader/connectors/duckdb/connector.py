"""DuckDB connector for reading and writing data."""

from typing import Any, Iterable, Union

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector

try:
    import duckdb
    from duckdb import DuckDBPyConnection
except ImportError:
    duckdb = None  # type: ignore
    DuckDBPyConnection = None  # type: ignore

import pyarrow as pa

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import DuckDBConnectorConfig
from .type_mapper import DuckDBTypeMapper

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
    ):
        """Initialize DuckDBConnector.

        Args:
            config: DuckDB connector configuration (DuckDBConnectorConfig, SourceConfig, or DestinationConfig).
                All configuration, including connection parameters, should be in the config parameter.

        Raises:
            ImportError: If required dependencies are not installed (install with: pip install dataloader[duckdb])
        """
        if duckdb is None:
            raise ImportError(
                "DuckDBConnector requires duckdb. "
                "Install it with: pip install dataloader[duckdb]"
            )
        self._config = config

        # Extract config values
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
            self._database = config.database or ":memory:"
            self._table = config.table or ""
            self._schema = config.db_schema
            self._write_mode = config.write_mode
            self._merge_keys = config.merge_keys

        self._batch_size = DEFAULT_BATCH_SIZE
        self._conn: DuckDBPyConnection | None = None
        self._table_created = False
        self._type_mapper = DuckDBTypeMapper()

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

    def _get_schema(self) -> list[tuple[str, pa.DataType]]:
        """Fetch column names and Arrow types from DuckDB table.

        Returns:
            List of (column_name, arrow_type) tuples.
        """
        conn = self._get_connection()
        try:
            result = conn.execute(f"DESCRIBE {self._qualified_table}")
            schema_info = []
            for row in result.fetchall():
                col_name = row[0]
                duckdb_type_str = row[1]
                arrow_type = self._type_mapper.connector_type_to_arrow(duckdb_type_str)
                schema_info.append((col_name, arrow_type))
            return schema_info
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

    def read_batches(self, state: State) -> Iterable[ArrowBatch]:
        """Read data from DuckDB table as batches.

        Uses DuckDB's native Arrow support for efficient batch reading.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            ArrowBatch instances containing the data.

        Raises:
            NotImplementedError: If reading is not supported (should not happen for DuckDB).
            ConnectorError: If connection or query fails.
        """
        try:
            conn = self._get_connection()
            schema_info = self._get_schema()
            columns = [col[0] for col in schema_info]
            # Convert Arrow types to string for metadata
            column_types = {col[0]: str(col[1]) for col in schema_info}

            query, params = self._build_query(state)

            # Use DuckDB's native Arrow support
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)

            batch_number = 0
            while True:
                # Fetch batch using fetchmany, then convert to Arrow
                # (DuckDB's fetch_arrow_table fetches all remaining rows, so we use fetchmany)
                rows = result.fetchmany(self._batch_size)
                if not rows:
                    break

                # Convert rows to list format
                row_data = [list(row) for row in rows]

                # Create ArrowBatch from rows
                batch_number += 1
                batch = ArrowBatch.from_rows(
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
                yield batch

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

    def _map_arrow_type_to_duckdb(self, arrow_type: pa.DataType) -> str:
        """Map Arrow type to DuckDB type (delegates to TypeMapper)."""
        return self._type_mapper.arrow_to_connector_type(arrow_type)

    def _get_existing_columns(self, conn: DuckDBPyConnection) -> set[str]:
        """Get existing columns for the table."""
        try:
            result = conn.execute(f"DESCRIBE {self._qualified_table}")
            return {row[0] for row in result.fetchall()}
        except duckdb.CatalogException:
            return set()

    def _create_table(self, conn: DuckDBPyConnection, batch: ArrowBatch) -> None:
        """Create table from batch schema if it doesn't exist."""
        # Get Arrow schema to determine column types
        arrow_table = batch.to_arrow()
        arrow_schema = arrow_table.schema

        # Build column definitions from Arrow schema
        column_defs = []
        for col_name in batch.columns:
            arrow_field = arrow_schema.field(col_name)
            duckdb_type = self._map_arrow_type_to_duckdb(arrow_field.type)
            column_defs.append(f'"{col_name}" {duckdb_type}')

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

    def _add_missing_columns(self, conn: DuckDBPyConnection, batch: ArrowBatch) -> None:
        """Add columns that exist in batch but not in table (schema evolution)."""
        existing = self._get_existing_columns(conn)
        arrow_table = batch.to_arrow()
        arrow_schema = arrow_table.schema

        for col_name in batch.columns:
            if col_name not in existing:
                arrow_field = arrow_schema.field(col_name)
                duckdb_type = self._map_arrow_type_to_duckdb(arrow_field.type)

                try:
                    conn.execute(
                        f'ALTER TABLE {self._qualified_table} ADD COLUMN "{col_name}" {duckdb_type}'
                    )
                except duckdb.Error as e:
                    raise ConnectorError(
                        f"Failed to add column '{col_name}': {e}",
                        context={"table": self._table, "column": col_name},
                    ) from e

    def _handle_write_mode(self, conn: DuckDBPyConnection, batch: ArrowBatch, full_refresh: bool = False) -> None:
        """Handle write mode logic before inserting.
        
        Args:
            conn: Database connection
            batch: Batch to write
            full_refresh: If True, use destructive DROP operations. If False, use DELETE/TRUNCATE for overwrite.
        """
        if self._write_mode == "overwrite" and not self._table_created:
            if full_refresh:
                # Full refresh: drop and recreate table (destructive)
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {self._qualified_table}")
                except duckdb.Error as e:
                    raise ConnectorError(
                        f"Failed to drop table for full refresh: {e}",
                        context={"table": self._table},
                    ) from e
                # Always create after drop in full_refresh mode
                self._create_table(conn, batch)
                self._table_created = True
            else:
                # Default overwrite: delete all rows (preserves structure)
                try:
                    existing = self._get_existing_columns(conn)
                    if existing:
                        conn.execute(f"DELETE FROM {self._qualified_table}")
                    self._table_created = True
                except duckdb.Error as e:
                    # If table doesn't exist, create it
                    existing = self._get_existing_columns(conn)
                    if not existing:
                        self._create_table(conn, batch)
                        self._table_created = True
                    else:
                        raise ConnectorError(
                            f"Failed to delete from table for overwrite: {e}",
                            context={"table": self._table},
                        ) from e

        elif self._write_mode == "merge":
            # Merge not supported in v0.1
            raise ConnectorError(
                "Merge write mode is not supported in v0.1. Use 'append' or 'overwrite'.",
                context={"table": self._table, "write_mode": self._write_mode},
            )

        elif self._write_mode == "append":
            if full_refresh and not self._table_created:
                # Full refresh with append: drop and recreate (destructive)
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {self._qualified_table}")
                except duckdb.Error as e:
                    raise ConnectorError(
                        f"Failed to drop table for full refresh: {e}",
                        context={"table": self._table},
                    ) from e
                self._create_table(conn, batch)
            else:
                # Default append: ensure table exists and handle schema evolution
                existing = self._get_existing_columns(conn)
                if not existing:
                    self._create_table(conn, batch)
                else:
                    self._add_missing_columns(conn, batch)
            self._table_created = True

    def _insert_batch(self, conn: DuckDBPyConnection, batch: ArrowBatch) -> None:
        """Insert batch rows using DuckDB's native Arrow support."""
        if batch.row_count == 0:
            return

        try:
            # Convert Arrow table to rows and use executemany
            # (DuckDB's native Arrow insertion can be added as optimization later)
            arrow_table = batch.to_arrow()
            rows = batch.rows  # This converts Arrow to list of lists

            columns = ", ".join(f'"{col}"' for col in batch.columns)
            placeholders = ", ".join("?" for _ in batch.columns)
            insert_sql = f"INSERT INTO {self._qualified_table} ({columns}) VALUES ({placeholders})"

            conn.executemany(insert_sql, rows)
        except duckdb.Error as e:
            raise ConnectorError(
                f"Failed to insert batch: {e}",
                context={
                    "table": self._table,
                    "row_count": batch.row_count,
                    "columns": batch.columns,
                },
            ) from e

    def write_batch(self, batch: ArrowBatch, state: State) -> None:
        """Write a batch to DuckDB table.

        Creates the table if it doesn't exist, handles schema evolution
        for new columns, and inserts rows using parameterized queries.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state. Reads full_refresh flag from state.metadata.

        Raises:
            NotImplementedError: If writing is not supported (should not happen for DuckDB).
            ConnectorError: If connection, table creation, or insert fails.
                Also raises if full_refresh is requested but not supported (currently supported).
        """
        full_refresh = state.metadata.get("full_refresh", False)
        # DuckDB supports full_refresh (DROP TABLE operations)
        # If full_refresh were not supported, raise ConnectorError here with a clear message
        
        conn = self._get_connection()

        self._handle_write_mode(conn, batch, full_refresh=full_refresh)
        self._insert_batch(conn, batch)


@register_connector("duckdb")
def create_duckdb_connector(
    config: ConnectorConfigUnion,
) -> DuckDBConnector:
    """Factory function for creating DuckDBConnector instances."""
    return DuckDBConnector(config)
