"""Pipeline configuration using Pydantic settings.

Supports loading from YAML files, environment variables, and direct
instantiation. Environment variables follow the ETL_<SECTION>__<KEY> pattern.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(str, Enum):
    """Supported log levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseSettings(BaseModel):
    """Database connection settings."""

    connection_string: str = Field(
        default="postgresql://localhost:5432/trade_db",
        description="SQLAlchemy-compatible connection string",
    )
    pool_size: int = Field(default=5, ge=1, le=50)
    max_overflow: int = Field(default=10, ge=0, le=100)
    pool_timeout: int = Field(default=30, ge=1)
    echo: bool = Field(default=False, description="Echo SQL statements to log")


class PipelineSettings(BaseModel):
    """Core pipeline execution settings."""

    name: str = Field(default="default-pipeline")
    batch_size: int = Field(default=10_000, ge=1, le=1_000_000)
    max_retries: int = Field(default=3, ge=0, le=10)
    retry_delay_seconds: float = Field(default=1.0, ge=0.1)
    parallel_extraction: bool = Field(default=False)
    max_workers: int = Field(default=4, ge=1, le=32)
    fail_fast: bool = Field(
        default=False,
        description="Stop pipeline on first transformer error",
    )
    error_threshold: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Maximum fraction of records that can fail validation before aborting",
    )


class MonitoringSettings(BaseModel):
    """Monitoring and logging settings."""

    log_level: LogLevel = Field(default=LogLevel.INFO)
    log_format: str = Field(default="json")
    enable_timing: bool = Field(default=True)
    enable_record_counts: bool = Field(default=True)
    metrics_export_path: Path | None = Field(default=None)


class ExtractorConfig(BaseModel):
    """Configuration for a single extractor stage."""

    type: str
    file_path: str | None = None
    connection_string: str | None = None
    query: str | None = None
    delimiter: str = ","
    encoding: str = "utf-8"
    extra: dict[str, Any] = Field(default_factory=dict)


class TransformerConfig(BaseModel):
    """Configuration for a single transformer stage."""

    type: str
    rules: dict[str, Any] = Field(default_factory=dict)
    extra: dict[str, Any] = Field(default_factory=dict)


class LoaderConfig(BaseModel):
    """Configuration for a single loader stage."""

    type: str
    table_name: str | None = None
    conflict_columns: list[str] = Field(default_factory=list)
    upsert: bool = False
    extra: dict[str, Any] = Field(default_factory=dict)


class ETLSettings(BaseSettings):
    """Root configuration that aggregates all sub-settings.

    Load order (later overrides earlier):
        1. Defaults defined here
        2. YAML config file (if provided)
        3. Environment variables prefixed with ETL_
    """

    model_config = SettingsConfigDict(
        env_prefix="ETL_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    extractors: list[ExtractorConfig] = Field(default_factory=list)
    transformers: list[TransformerConfig] = Field(default_factory=list)
    loaders: list[LoaderConfig] = Field(default_factory=list)

    @field_validator("extractors", mode="before")
    @classmethod
    def _parse_extractors(cls, v: Any) -> Any:
        if isinstance(v, list):
            return [
                ExtractorConfig(**item) if isinstance(item, dict) else item
                for item in v
            ]
        return v

    @classmethod
    def from_yaml(cls, path: str | Path) -> ETLSettings:
        """Load configuration from a YAML file.

        Values from the file are used as defaults; environment variables
        still take precedence.
        """
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

        return cls(**raw)
