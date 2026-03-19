"""Abstract base class for all transformers.

A transformer receives a DataFrame, applies some logic, and returns
a (potentially modified) DataFrame along with a count of rejected
records. The two-value return allows the pipeline to track data
quality without losing the rejected rows silently.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    """Interface that all transformers must satisfy.

    The ``transform`` method returns a tuple of:
        - The output DataFrame (valid records only, or enriched records)
        - An integer count of records that were rejected / could not
          be transformed

    This contract lets the pipeline accumulate rejection counts across
    multiple transformer stages and compare against the configured
    error threshold.
    """

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Apply the transformation to the input DataFrame.

        Args:
            df: Input data.

        Returns:
            A tuple of (transformed_dataframe, rejected_record_count).
        """
        ...

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class TransformationError(Exception):
    """Raised when a transformer encounters an unrecoverable error."""

    def __init__(self, transformer: str, reason: str) -> None:
        self.transformer = transformer
        self.reason = reason
        super().__init__(f"Transformation failed in '{transformer}': {reason}")
