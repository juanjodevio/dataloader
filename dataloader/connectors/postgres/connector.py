"""PostgreSQL connector for reading and writing data using SQLAlchemy."""

from typing import Any, Iterable, Union

from dataloader.connectors.registry import ConnectorConfigUnion, register_connector

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore

import pyarrow as pa

try:
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    create_engine = None  # type: ignore
    inspect = None  # type: ignore
    text = None  # type: ignore
    Engine = None  # type: ignore
    SQLAlchemyError = None  # type: ignore

from dataloader.core.batch import ArrowBatch, Batch
from dataloader.core.exceptions import ConnectorError
from dataloader.core.state import State
from dataloader.models.destination_config import DestinationConfig
from dataloader.models.source_config import SourceConfig

from .config import PostgresConnectorConfig
from .type_mapper import PostgresTypeMapper


class PostgresConnector:
    """Unified connector for PostgreSQL databases using SQLAlchemy.

    Supports both reading and writing operations. Uses SQLAlchemy for database
    abstraction, enabling support for multiple database dialects (Postgres, MySQL,
    Redshift, etc.) with the same interface.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_PORT = 5432
    DIALECT = "postgresql+psycopg2"

    def __init__(
        self,
        config: Union[PostgresConnectorConfig, SourceConfig, DestinationConfig],
    ):
        """Initialize PostgresConnector.

        Args:
            config: PostgreSQL connector configuration (PostgresConnectorConfig, SourceConfig, or DestinationConfig).
                All configuration, including connection parameters, should be in the config parameter.

        Raises:
            ImportError: If required dependencies are not installed (install with: pip install dataloader[postgres])
        """
        if create_engine is None or pd is None:
            raise ImportError(
                "PostgresConnector requires sqlalchemy and pandas. "
                "Install them with: pip install dataloader[postgres]"
            )
        self._config = config

        # Extract config values
        if isinstance(config, PostgresConnectorConfig):
            self._host = config.host
            self._port = config.port or self.DEFAULT_PORT
            self._database = config.database
            self._user = config.user
            self._password = config.password.get_secret_value() if config.password else None
            self._db_schema = config.db_schema or "public"
            self._table = config.table
            self._write_mode = config.write_mode
            self._merge_keys = config.merge_keys
        elif isinstance(config, SourceConfig):
            self._host = config.host or ""
            self._port = config.port or self.DEFAULT_PORT
            self._database = config.database or ""
            self._user = config.user or ""
            self._password = config.password.get_secret_value() if config.password else None.get_secret_value() if config.password else None
            self._db_schema = config.db_schema or "public"
            self._table = config.table or ""
            self._write_mode = "append"  # Default for source configs
            self._merge_keys = None
        else:  # DestinationConfig
            self._host = config.host or ""
            self._port = config.port or self.DEFAULT_PORT
            self._database = config.database or ""
            self._user = config.user or ""
            self._password = config.password.get_secret_value() if config.password else None.get_secret_value() if config.password else None
            self._db_schema = config.db_schema or "public"
            self._table = config.table or ""
            self._write_mode = config.write_mode
            self._merge_keys = config.merge_keys

        self._batch_size = self.DEFAULT_BATCH_SIZE
        self._engine: Engine | None = None
        self._table_created = False
        self._type_mapper = PostgresTypeMapper()

    def _build_connection_url(self) -> str:
        """Build SQLAlchemy connection URL from config."""
        # Use default dialect (can be extended in future to support other databases)
        dialect = self.DIALECT

        # Build URL: dialect://user:password@host:port/database
        if self._password:
            return f"{dialect}://{self._user}:{self._password}@{self._host}:{self._port}/{self._database}"
        return f"{dialect}://{self._user}@{self._host}:{self._port}/{self._database}"

    def _get_engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            try:
                url = self._build_connection_url()
                # Pool settings for batch operations
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
                        "host": self._host,
                        "database": self._database,
                    },
                ) from e
        return self._engine

    def _close(self) -> None:
        """Dispose of the engine and close connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    @property
    def _qualified_table(self) -> str:
        """Return fully qualified table name."""
        return f'"{self._db_schema}"."{self._table}"'

    # ========== Reading methods ==========

    def _get_schema(self) -> list[tuple[str, pa.DataType]]:
        """Fetch column names and Arrow types using SQLAlchemy inspector.

        Returns:
            List of (column_name, arrow_type) tuples.
        """
        engine = self._get_engine()
        inspector = inspect(engine)

        columns = inspector.get_columns(self._table, schema=self._db_schema)
        result = []
        for col in columns:
            col_name = col["name"]
            pg_type_str = str(col["type"])
            arrow_type = self._type_mapper.connector_type_to_arrow(pg_type_str)
            result.append((col_name, arrow_type))
        return result

    def _build_query(self, state: State) -> tuple[str, dict[str, Any]]:
        """Build SELECT query with optional cursor-based filtering.

        Args:
            state: Current state containing cursor values.

        Returns:
            Tuple of (query_string, parameters).
        """
        query_parts = [f"SELECT * FROM {self._qualified_table}"]
        params: dict[str, Any] = {}

        # Apply cursor-based filtering for incremental loads
        incremental = getattr(self._config, "incremental", None)
        if incremental and incremental.cursor_column:
            cursor_column = incremental.cursor_column
            cursor_value = state.cursor_values.get(cursor_column)

            if cursor_value is not None:
                query_parts.append(f'WHERE "{cursor_column}" > :cursor_value')
                params["cursor_value"] = cursor_value

            # Always order by cursor column for consistent pagination
            query_parts.append(f'ORDER BY "{cursor_column}"')

        return " ".join(query_parts), params

    def read_batches(self, state: State) -> Iterable[ArrowBatch]:
        """Read data from PostgreSQL table as batches.

        Uses SQLAlchemy with pandas to read data efficiently, then converts to Arrow.

        Args:
            state: Current state containing cursor values for incremental reads.

        Yields:
            ArrowBatch instances containing the data.

        Raises:
            ConnectorError: If connection or query fails.
        """
        try:
            engine = self._get_engine()
            schema_info = self._get_schema()
            columns = [col[0] for col in schema_info]
            # Convert Arrow types to string for metadata
            column_types = {col[0]: str(col[1]) for col in schema_info}

            query, params = self._build_query(state)

            # Use pandas read_sql with chunksize for memory-efficient reading
            # Use engine directly (not connection) for better pandas compatibility
            batch_number = 0
            for chunk_df in pd.read_sql(
                query, engine, params=params, chunksize=self._batch_size
            ):
                if chunk_df.empty:
                    break

                # Convert pandas DataFrame to Arrow Table
                arrow_table = pa.Table.from_pandas(chunk_df)

                batch_number += 1
                yield ArrowBatch(
                    arrow_table,
                    metadata={
                        "batch_number": batch_number,
                        "row_count": len(chunk_df),
                        "source_type": "postgres",
                        "table": self._table,
                        "schema": self._db_schema,
                        "column_types": column_types,
                    },
                )

        except SQLAlchemyError as e:
            raise ConnectorError(
                f"Failed to read from database: {e}",
                context={
                    "table": self._table,
                    "schema": self._db_schema,
                },
            ) from e
        finally:
            self._close()

    # ========== Writing methods ==========

    def _get_existing_columns(self, conn: Any) -> set[str]:
        """Get existing columns for the table."""
        try:
            inspector = inspect(self._get_engine())
            columns = inspector.get_columns(self._table, schema=self._db_schema)
            return {col["name"] for col in columns}
        except Exception:
            return set()

    def _map_arrow_type_to_postgres(self, arrow_type: pa.DataType) -> str:
        """Map Arrow type to PostgreSQL type (delegates to TypeMapper)."""
        return self._type_mapper.arrow_to_connector_type(arrow_type)

    def _create_table(self, conn: Any, batch: ArrowBatch) -> None:
        """Create table from batch schema if it doesn't exist."""
        # Get Arrow schema to determine column types
        arrow_table = batch.to_arrow()
        arrow_schema = arrow_table.schema

        # Build column definitions from Arrow schema
        column_defs = []
        for col_name in batch.columns:
            arrow_field = arrow_schema.field(col_name)
            pg_type = self._map_arrow_type_to_postgres(arrow_field.type)
            column_defs.append(f'"{col_name}" {pg_type}')

        columns_sql = ", ".join(column_defs)

        try:
            conn.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {self._qualified_table} ({columns_sql})"
                )
            )
        except SQLAlchemyError as e:
            raise ConnectorError(
                f"Failed to create table: {e}",
                context={"table": self._table, "columns": batch.columns},
            ) from e

    def _add_missing_columns(self, conn: Any, batch: ArrowBatch) -> None:
        """Add columns that exist in batch but not in table (schema evolution)."""
        existing = self._get_existing_columns(conn)
        arrow_table = batch.to_arrow()
        arrow_schema = arrow_table.schema

        for col_name in batch.columns:
            if col_name not in existing:
                arrow_field = arrow_schema.field(col_name)
                pg_type = self._map_arrow_type_to_postgres(arrow_field.type)

                try:
                    conn.execute(
                        text(
                            f'ALTER TABLE {self._qualified_table} ADD COLUMN "{col_name}" {pg_type}'
                        )
                    )
                except SQLAlchemyError as e:
                    raise ConnectorError(
                        f"Failed to add column '{col_name}': {e}",
                        context={"table": self._table, "column": col_name},
                    ) from e

    def _handle_write_mode(self, conn: Any, batch: ArrowBatch) -> None:
        """Handle write mode logic before inserting."""
        if self._write_mode == "overwrite" and not self._table_created:
            # Drop and recreate table
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {self._qualified_table}"))
            except SQLAlchemyError as e:
                raise ConnectorError(
                    f"Failed to drop table for overwrite: {e}",
                    context={"table": self._table},
                ) from e
            self._create_table(conn, batch)
            self._table_created = True

        elif self._write_mode == "merge":
            # Merge not supported in v0.1
            raise ConnectorError(
                "Merge write mode is not supported for PostgreSQL in v0.1. Use 'append' or 'overwrite'.",
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

    def _insert_batch(self, conn: Any, batch: ArrowBatch) -> None:
        """Insert batch rows using pandas to_sql for efficient bulk inserts."""
        if batch.row_count == 0:
            return

        try:
            # Convert Arrow table to pandas DataFrame
            arrow_table = batch.to_arrow()
            df = arrow_table.to_pandas()

            # Use pandas to_sql with engine for efficient bulk insert
            # pandas.to_sql works with SQLAlchemy engine or connection
            engine = self._get_engine()
            df.to_sql(
                self._table,
                engine,
                schema=self._db_schema,
                if_exists="append",
                index=False,
                method="multi",  # Use multi-row insert for better performance
            )
        except SQLAlchemyError as e:
            raise ConnectorError(
                f"Failed to insert batch: {e}",
                context={
                    "table": self._table,
                    "row_count": batch.row_count,
                    "columns": batch.columns,
                },
            ) from e

    def write_batch(self, batch: ArrowBatch, state: State) -> None:
        """Write a batch to PostgreSQL table.

        Creates the table if it doesn't exist, handles schema evolution
        for new columns, and inserts rows using parameterized queries.

        Args:
            batch: Batch of data to write.
            state: Current pipeline state (unused for PostgreSQL but kept for protocol).

        Raises:
            ConnectorError: If connection, table creation, or insert fails.
        """
        engine = self._get_engine()
        with engine.connect() as conn:
            self._handle_write_mode(conn, batch)
            self._insert_batch(conn, batch)

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        self._close()


@register_connector("postgres")
def create_postgres_connector(
    config: ConnectorConfigUnion,
) -> PostgresConnector:
    """Factory function for creating PostgresConnector instances."""
    return PostgresConnector(config)
