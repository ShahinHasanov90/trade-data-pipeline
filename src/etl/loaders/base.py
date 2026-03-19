"""Abstract base class for all loaders.

Loaders are responsible for writing processed DataFrames to a target
system (database, file, API, message queue, etc.). They handle
connection management, batching, and error recovery.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseLoader(ABC):
    """Interface that all loaders must satisfy.

    Subclasses should:
        1. Accept target-specific configuration in ``__init__``.
        2. Implement ``load()`` to write a DataFrame batch.
        3. Optionally implement ``validate_target()`` for pre-flight checks.
        4. Optionally implement ``finalize()`` for post-load cleanup
           (e.g. refreshing materialised views, updating watermarks).
    """

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Write a batch of records to the target.

        Args:
            df: DataFrame batch to write.

        Returns:
            Number of records successfully written.

        Raises:
            LoadError: If the write fails.
        """
        ...

    def validate_target(self) -> bool:
        """Check that the target system is accessible.

        Default implementation returns True. Override for targets that
        need connectivity checks.
        """
        return True

    def finalize(self) -> None:
        """Perform post-load operations.

        Called once after all batches have been loaded. Use this for
        tasks like refreshing materialised views, updating metadata
        tables, or sending completion notifications.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"


class LoadError(Exception):
    """Raised when a loader fails to write to its target."""

    def __init__(self, target: str, reason: str, records_affected: int = 0) -> None:
        self.target = target
        self.reason = reason
        self.records_affected = records_affected
        super().__init__(
            f"Load failed for '{target}' ({records_affected} records affected): {reason}"
        )
