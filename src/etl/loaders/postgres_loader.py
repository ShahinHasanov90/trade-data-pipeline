"""PostgreSQL loader with upsert (INSERT ... ON CONFLICT) support.

Uses SQLAlchemy Core for high-performance bulk inserts. Supports:
- Plain INSERT (append mode)
- UPSERT via ON CONFLICT DO UPDATE
- Configurable conflict resolution columns
- Automatic table creation from DataFrame schema (optional)
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog
from sqlalchemy import (
    MetaData,
    Table,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from etl.loaders.base import BaseLoader, LoadError

logger = structlog.get_logger(__name__)


class PostgresLoader(BaseLoader):
    """Load DataFrames into a PostgreSQL table.

    Args:
        connection_string: PostgreSQL connection URL.
        table_name: Target table name.
        schema: Database schema. Defaults to ``"public"``.
        conflict_columns: Columns that form the unique constraint for
            upsert. Required if ``upsert=True``.
        upsert: If True, use ``INSERT ... ON CONFLICT DO UPDATE``.
            If False, use plain ``INSERT``.
        update_columns: Columns to update on conflict. If None and
            upsert is True, all non-conflict columns are updated.
        create_table: If True, auto-create the table from the DataFrame
            schema if it does not exist.
        if_exists: Behaviour when table exists and create_table is True.
            One of "append", "replace", "fail".
        pool_size: Connection pool size.
        max_overflow: Max connections beyond pool_size.
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str,
        schema: str = "public",
        conflict_columns: list[str] | None = None,
        upsert: bool = False,
        update_columns: list[str] | None = None,
        create_table: bool = False,
        if_exists: str = "append",
        pool_size: int = 5,
        max_overflow: int = 10,
    ) -> None:
        if upsert and not conflict_columns:
            raise ValueError("conflict_columns required when upsert=True")

        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema
        self.conflict_columns = conflict_columns or []
        self.upsert = upsert
        self.update_columns = update_columns
        self.create_table = create_table
        self.if_exists = if_exists
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

    def validate_target(self) -> bool:
        """Check database connectivity and table existence."""
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            if not self.create_table:
                insp = inspect(engine)
                if not insp.has_table(self.table_name, schema=self.schema):
                    logger.warning(
                        "target_table_missing",
                        table=self.table_name,
                        schema=self.schema,
                    )
                    return False

            return True
        except Exception as exc:
            logger.warning("target_validation_failed", error=str(exc))
            return False

    def load(self, df: pd.DataFrame) -> int:
        """Write a DataFrame batch to PostgreSQL.

        Args:
            df: Records to write.

        Returns:
            Number of records written.

        Raises:
            LoadError: If the insert/upsert fails.
        """
        if df.empty:
            return 0

        engine = self._get_engine()

        try:
            if self.create_table:
                self._ensure_table(df, engine)

            if self.upsert:
                return self._upsert(df, engine)
            else:
                return self._insert(df, engine)

        except LoadError:
            raise
        except Exception as exc:
            raise LoadError(
                target=f"{self.schema}.{self.table_name}",
                reason=str(exc),
                records_affected=len(df),
            ) from exc

    def _insert(self, df: pd.DataFrame, engine: Engine) -> int:
        """Plain INSERT using pandas to_sql for simplicity and speed."""
        df.to_sql(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.debug(
            "postgres_insert",
            table=self.table_name,
            rows=len(df),
        )
        return len(df)

    def _upsert(self, df: pd.DataFrame, engine: Engine) -> int:
        """INSERT ... ON CONFLICT DO UPDATE using PostgreSQL dialect."""
        metadata = MetaData(schema=self.schema)
        table = Table(
            self.table_name,
            metadata,
            autoload_with=engine,
        )

        records = df.to_dict(orient="records")
        stmt = pg_insert(table).values(records)

        # Determine which columns to update on conflict
        update_cols = self.update_columns
        if update_cols is None:
            # Update all columns except the conflict key columns
            update_cols = [
                c.name for c in table.columns if c.name not in self.conflict_columns
            ]

        if update_cols:
            update_dict = {col: stmt.excluded[col] for col in update_cols}
            stmt = stmt.on_conflict_do_update(
                index_elements=self.conflict_columns,
                set_=update_dict,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=self.conflict_columns,
            )

        with engine.begin() as conn:
            conn.execute(stmt)

        logger.debug(
            "postgres_upsert",
            table=self.table_name,
            rows=len(records),
            conflict_columns=self.conflict_columns,
        )
        return len(records)

    def _ensure_table(self, df: pd.DataFrame, engine: Engine) -> None:
        """Create the target table if it does not exist."""
        insp = inspect(engine)
        if not insp.has_table(self.table_name, schema=self.schema):
            logger.info(
                "creating_table",
                table=self.table_name,
                schema=self.schema,
            )
            # Use a small sample to create the table structure
            df.head(0).to_sql(
                name=self.table_name,
                con=engine,
                schema=self.schema,
                if_exists=self.if_exists,
                index=False,
            )

    def finalize(self) -> None:
        """Run ANALYZE on the target table to update statistics."""
        engine = self._get_engine()
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f"ANALYZE {self.schema}.{self.table_name}")
                )
            logger.info("postgres_analyze_completed", table=self.table_name)
        except Exception as exc:
            logger.warning("postgres_analyze_failed", error=str(exc))

    def dispose(self) -> None:
        """Dispose the connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def __repr__(self) -> str:
        mode = "upsert" if self.upsert else "insert"
        return f"<PostgresLoader table={self.schema}.{self.table_name} mode={mode}>"
