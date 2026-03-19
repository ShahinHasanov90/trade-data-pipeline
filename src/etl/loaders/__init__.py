"""Loader modules for writing data to target systems."""

from etl.loaders.base import BaseLoader
from etl.loaders.postgres_loader import PostgresLoader

__all__ = ["BaseLoader", "PostgresLoader"]
