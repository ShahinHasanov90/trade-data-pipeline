"""Pipeline monitoring with structured logging.

Tracks execution timing, record counts, error rates, and emits
structured events via structlog for easy integration with log
aggregation systems (ELK, Datadog, etc.).
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Generator

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class StageMetrics:
    """Metrics collected for a single pipeline stage."""

    stage_name: str
    stage_type: str  # "extractor", "transformer", "loader"
    records_in: int = 0
    records_out: int = 0
    records_error: int = 0
    duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def error_rate(self) -> float:
        """Fraction of input records that produced errors."""
        if self.records_in == 0:
            return 0.0
        return self.records_error / self.records_in

    @property
    def throughput(self) -> float:
        """Records processed per second."""
        if self.duration_seconds == 0.0:
            return 0.0
        return self.records_out / self.duration_seconds

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "stage_type": self.stage_type,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "records_error": self.records_error,
            "error_rate": round(self.error_rate, 6),
            "duration_seconds": round(self.duration_seconds, 4),
            "throughput_rps": round(self.throughput, 2),
            **self.metadata,
        }


@dataclass
class PipelineMetrics:
    """Aggregated metrics for an entire pipeline run."""

    pipeline_name: str
    run_id: str
    stages: list[StageMetrics] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    success: bool = False
    error_message: str | None = None

    @property
    def total_records_in(self) -> int:
        extractors = [s for s in self.stages if s.stage_type == "extractor"]
        return sum(s.records_out for s in extractors)

    @property
    def total_records_out(self) -> int:
        loaders = [s for s in self.stages if s.stage_type == "loader"]
        return sum(s.records_out for s in loaders)

    @property
    def total_errors(self) -> int:
        return sum(s.records_error for s in self.stages)

    def summary(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "success": self.success,
            "total_duration_seconds": round(self.total_duration_seconds, 4),
            "total_records_in": self.total_records_in,
            "total_records_out": self.total_records_out,
            "total_errors": self.total_errors,
            "stages": [s.to_dict() for s in self.stages],
            "error_message": self.error_message,
        }


class PipelineMonitor:
    """Collects and emits structured metrics during pipeline execution.

    Usage::

        monitor = PipelineMonitor("my-pipeline", run_id="abc123")
        with monitor.track_stage("csv-extract", "extractor") as metrics:
            df = extractor.extract()
            metrics.records_out = len(df)
        monitor.finalize(success=True)
        print(monitor.metrics.summary())
    """

    def __init__(self, pipeline_name: str, run_id: str) -> None:
        self._pipeline_name = pipeline_name
        self._run_id = run_id
        self._metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
            run_id=run_id,
        )
        self._pipeline_start: float | None = None
        self._log = logger.bind(pipeline=pipeline_name, run_id=run_id)

    @property
    def metrics(self) -> PipelineMetrics:
        return self._metrics

    def start(self) -> None:
        """Record the pipeline start time and emit a start event."""
        self._pipeline_start = time.monotonic()
        self._log.info("pipeline_started")

    @contextmanager
    def track_stage(
        self,
        stage_name: str,
        stage_type: str,
    ) -> Generator[StageMetrics, None, None]:
        """Context manager that times a stage and appends its metrics.

        Args:
            stage_name: Human-readable name for the stage.
            stage_type: One of "extractor", "transformer", "loader".

        Yields:
            A StageMetrics object. The caller should populate
            ``records_in``, ``records_out``, and ``records_error``
            within the block.
        """
        stage_metrics = StageMetrics(stage_name=stage_name, stage_type=stage_type)
        stage_log = self._log.bind(stage=stage_name, stage_type=stage_type)
        stage_log.info("stage_started")

        start = time.monotonic()
        try:
            yield stage_metrics
        except Exception as exc:
            stage_metrics.duration_seconds = time.monotonic() - start
            stage_metrics.metadata["error"] = str(exc)
            self._metrics.stages.append(stage_metrics)
            stage_log.error(
                "stage_failed",
                duration=stage_metrics.duration_seconds,
                error=str(exc),
            )
            raise
        else:
            stage_metrics.duration_seconds = time.monotonic() - start
            self._metrics.stages.append(stage_metrics)
            stage_log.info(
                "stage_completed",
                duration=round(stage_metrics.duration_seconds, 4),
                records_in=stage_metrics.records_in,
                records_out=stage_metrics.records_out,
                records_error=stage_metrics.records_error,
                error_rate=round(stage_metrics.error_rate, 6),
                throughput_rps=round(stage_metrics.throughput, 2),
            )

    def finalize(self, success: bool, error_message: str | None = None) -> None:
        """Mark the pipeline run as complete and emit a summary event."""
        if self._pipeline_start is not None:
            self._metrics.total_duration_seconds = (
                time.monotonic() - self._pipeline_start
            )
        self._metrics.success = success
        self._metrics.error_message = error_message

        summary = self._metrics.summary()
        if success:
            self._log.info("pipeline_completed", **summary)
        else:
            self._log.error("pipeline_failed", **summary)


def configure_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """Configure structlog with sensible defaults for pipeline use.

    Args:
        log_level: Standard Python log level name.
        log_format: Either "json" for machine-readable output or
            "console" for human-readable colored output.
    """
    import logging

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
