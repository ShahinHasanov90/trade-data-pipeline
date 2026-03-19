"""Abstract base class for all extractors.

Every extractor must implement ``extract()`` which returns a pandas
DataFrame. Extractors are responsible for source-level concerns:
connection management, pagination, encoding, etc.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseExtractor(ABC):
    """Interface that all extractors must satisfy.

    Subclasses should:
        1. Accept source-specific configuration in ``__init__``.
        2. Implement ``extract()`` to read data and return a DataFrame.
        3. Optionally implement ``validate_source()`` to pre-flight
           check connectivity or file existence.
    """

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Read data from the source and return it as a DataFrame.

        Raises:
            ExtractionError: If the source cannot be read.

        Returns:
            A DataFrame containing the extracted records.
        """
        ...

    def validate_source(self) -> bool:
        """Check that the data source is accessible.

        The default implementation always returns True. Override this
        in subclasses that need to verify file existence, database
        connectivity, API availability, etc.
        """
        return True

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class ExtractionError(Exception):
    """Raised when an extractor fails to read from its source."""

    def __init__(self, source: str, reason: str) -> None:
        self.source = source
        self.reason = reason
        super().__init__(f"Extraction failed for '{source}': {reason}")
