"""Database extractor using SQLAlchemy.

Reads data from any SQLAlchemy-supported database via raw SQL or table
reflection. Supports parameterised queries, connection pooling, and
chunked reads for large result sets.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.extractors.base import BaseExtractor, ExtractionError

logger = structlog.get_logger(__name__)


class DatabaseExtractor(BaseExtractor):
    """Extract records from a relational database.

    Args:
        connection_string: SQLAlchemy connection URL, e.g.
            ``"postgresql://user:pass@host:5432/dbname"``.
        query: SQL query to execute. Mutually exclusive with
            ``table_name``.
        table_name: Read the entire table. Mutually exclusive with
            ``query``.
        params: Bind parameters for the query.
        schema: Database schema (used with ``table_name``).
        chunk_size: If set, fetches rows in chunks to limit memory.
        pool_size: Connection pool size.
        max_overflow: Max connections beyond pool_size.
    """

    def __init__(
        self,
        connection_string: str,
        query: str | None = None,
        table_name: str | None = None,
        params: dict[str, Any] | None = None,
        schema: str | None = None,
        chunk_size: int | None = None,
        pool_size: int = 5,
        max_overflow: int = 10,
    ) -> None:
        if not query and not table_name:
            raise ValueError("Either 'query' or 'table_name' must be provided")
        if query and table_name:
            raise ValueError("Provide 'query' or 'table_name', not both")

        self.connection_string = connection_string
        self.query = query
        self.table_name = table_name
        self.params = params or {}
        self.schema = schema
        self.chunk_size = chunk_size
        self._engine: Engine | None = None
        self._pool_size = pool_size
        self._max_overflow = max_overflow

    def _get_engine(self) -> Engine:
        """Lazily create and cache the SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                pool_pre_ping=True,
            )
        return self._engine

    def validate_source(self) -> bool:
        """Verify database connectivity with a lightweight query."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            logger.warning(
                "db_connection_failed",
                connection=self._safe_connection_string(),
                error=str(exc),
            )
            return False

    def extract(self) -> pd.DataFrame:
        """Execute the query and return results as a DataFrame.

        Returns:
            DataFrame containing query results.

        Raises:
            ExtractionError: If the query fails.
        """
        source_desc = self.query or f"table:{self.table_name}"
        logger.info(
            "db_extraction_started",
            source=source_desc[:120],
            connection=self._safe_connection_string(),
        )

        try:
            engine = self._get_engine()

            if self.table_name:
                df = self._read_table(engine)
            else:
                df = self._read_query(engine)

        except ExtractionError:
            raise
        except Exception as exc:
            raise ExtractionError(
                source=self._safe_connection_string(),
                reason=str(exc),
            ) from exc

        logger.info(
            "db_extraction_completed",
            rows=len(df),
            columns=len(df.columns),
        )
        return df

    def _read_query(self, engine: Engine) -> pd.DataFrame:
        """Execute a raw SQL query."""
        if self.chunk_size:
            return self._read_query_chunked(engine)

        with engine.connect() as conn:
            return pd.read_sql(
                text(self.query),  # type: ignore[arg-type]
                conn,
                params=self.params,
            )

    def _read_query_chunked(self, engine: Engine) -> pd.DataFrame:
        """Read query results in chunks to control memory."""
        chunks: list[pd.DataFrame] = []
        with engine.connect() as conn:
            for i, chunk in enumerate(
                pd.read_sql(
                    text(self.query),  # type: ignore[arg-type]
                    conn,
                    params=self.params,
                    chunksize=self.chunk_size,
                )
            ):
                chunks.append(chunk)
                logger.debug("db_chunk_read", chunk_index=i, rows=len(chunk))

        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def _read_table(self, engine: Engine) -> pd.DataFrame:
        """Read an entire table."""
        with engine.connect() as conn:
            return pd.read_sql_table(
                self.table_name,  # type: ignore[arg-type]
                conn,
                schema=self.schema,
            )

    def _safe_connection_string(self) -> str:
        """Return connection string with password masked."""
        parts = self.connection_string.split("@")
        if len(parts) > 1:
            credentials = parts[0]
            if ":" in credentials:
                user_part = credentials.rsplit(":", 1)[0]
                return f"{user_part}:***@{parts[1]}"
        return self.connection_string

    def dispose(self) -> None:
        """Dispose the connection pool. Call when done with this extractor."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __repr__(self) -> str:
        source = self.table_name or (self.query[:50] + "..." if self.query else "?")
        return f"<DatabaseExtractor source={source}>"
