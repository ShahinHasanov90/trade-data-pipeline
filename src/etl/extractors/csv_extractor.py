"""CSV file extractor.

Reads flat files (CSV, TSV, pipe-delimited) into DataFrames with
configurable parsing: encoding, delimiter, column selection, dtype
overrides, and chunk-based reading for large files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from etl.extractors.base import BaseExtractor, ExtractionError

logger = structlog.get_logger(__name__)


class CsvExtractor(BaseExtractor):
    """Extract records from a CSV (or delimited) file.

    Args:
        file_path: Path to the source file.
        delimiter: Column separator. Defaults to ``","``
        encoding: File encoding. Defaults to ``"utf-8"``.
        columns: Optional list of column names to select. ``None``
            means read all columns.
        dtype_overrides: Dict mapping column names to pandas dtypes.
            Useful for forcing string types on numeric-looking codes
            (e.g. HS codes with leading zeros).
        skip_rows: Number of header rows to skip before the actual
            header row.
        chunk_size: If set, reads the file in chunks and concatenates.
            Useful for memory-constrained environments.
        na_values: Additional strings to recognise as NA/NaN.
    """

    def __init__(
        self,
        file_path: str | Path,
        delimiter: str = ",",
        encoding: str = "utf-8",
        columns: list[str] | None = None,
        dtype_overrides: dict[str, Any] | None = None,
        skip_rows: int = 0,
        chunk_size: int | None = None,
        na_values: list[str] | None = None,
    ) -> None:
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.columns = columns
        self.dtype_overrides = dtype_overrides or {}
        self.skip_rows = skip_rows
        self.chunk_size = chunk_size
        self.na_values = na_values

    def validate_source(self) -> bool:
        """Check that the file exists and is readable."""
        if not self.file_path.exists():
            logger.warning("csv_file_not_found", path=str(self.file_path))
            return False
        if not self.file_path.is_file():
            logger.warning("csv_path_not_file", path=str(self.file_path))
            return False
        return True

    def extract(self) -> pd.DataFrame:
        """Read the CSV file into a DataFrame.

        Returns:
            DataFrame with the file contents. Columns are optionally
            filtered to ``self.columns``.

        Raises:
            ExtractionError: If the file cannot be read.
        """
        if not self.validate_source():
            raise ExtractionError(
                source=str(self.file_path),
                reason="File does not exist or is not readable",
            )

        logger.info(
            "csv_extraction_started",
            path=str(self.file_path),
            encoding=self.encoding,
        )

        try:
            read_kwargs: dict[str, Any] = {
                "filepath_or_buffer": self.file_path,
                "sep": self.delimiter,
                "encoding": self.encoding,
                "dtype": self.dtype_overrides or None,
                "skiprows": self.skip_rows if self.skip_rows else None,
                "na_values": self.na_values,
                "low_memory": False,
            }

            if self.columns:
                read_kwargs["usecols"] = self.columns

            if self.chunk_size:
                df = self._read_chunked(read_kwargs)
            else:
                df = pd.read_csv(**read_kwargs)

        except ExtractionError:
            raise
        except Exception as exc:
            raise ExtractionError(
                source=str(self.file_path),
                reason=str(exc),
            ) from exc

        logger.info(
            "csv_extraction_completed",
            path=str(self.file_path),
            rows=len(df),
            columns=len(df.columns),
        )
        return df

    def _read_chunked(self, read_kwargs: dict[str, Any]) -> pd.DataFrame:
        """Read file in chunks to control memory usage."""
        read_kwargs["chunksize"] = self.chunk_size
        chunks: list[pd.DataFrame] = []

        reader = pd.read_csv(**read_kwargs)
        for i, chunk in enumerate(reader):
            chunks.append(chunk)
            logger.debug(
                "csv_chunk_read",
                chunk_index=i,
                rows=len(chunk),
            )

        if not chunks:
            return pd.DataFrame()

        return pd.concat(chunks, ignore_index=True)

    def __repr__(self) -> str:
        return (
            f"<CsvExtractor path={self.file_path} "
            f"delimiter={self.delimiter!r} encoding={self.encoding!r}>"
        )
