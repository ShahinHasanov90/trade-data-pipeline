"""Core pipeline orchestrator.

The ``Pipeline`` class wires together extractors, transformers, and loaders
into a single executable unit. It supports:

- Sequential and parallel extraction
- Ordered transformer chains
- Batch-aware loading
- Comprehensive monitoring via ``PipelineMonitor``
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import structlog

from etl.extractors.base import BaseExtractor
from etl.loaders.base import BaseLoader
from etl.monitoring import PipelineMonitor
from etl.transformers.base import BaseTransformer

logger = structlog.get_logger(__name__)


@dataclass
class PipelineResult:
    """Summarises the outcome of a pipeline run."""

    run_id: str
    success: bool
    records_extracted: int = 0
    records_loaded: int = 0
    records_rejected: int = 0
    errors: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        lines = [
            f"Pipeline run {self.run_id}: {status}",
            f"  Extracted : {self.records_extracted:,}",
            f"  Loaded    : {self.records_loaded:,}",
            f"  Rejected  : {self.records_rejected:,}",
        ]
        if self.errors:
            lines.append(f"  Errors    : {len(self.errors)}")
            for err in self.errors[:5]:
                lines.append(f"    - {err}")
            if len(self.errors) > 5:
                lines.append(f"    ... and {len(self.errors) - 5} more")
        return "\n".join(lines)


class Pipeline:
    """Orchestrates extract-transform-load execution.

    Example::

        pipeline = Pipeline(name="customs-daily")
        pipeline.add_extractor(csv_ext)
        pipeline.add_transformer(validator)
        pipeline.add_loader(pg_loader)
        result = pipeline.run()

    Args:
        name: Human-readable pipeline name (used in logs and metrics).
        batch_size: Number of records per processing batch.
        parallel_extraction: Fan out extractors across threads.
        max_workers: Thread pool size for parallel extraction.
        fail_fast: Abort on first transformer error instead of
            collecting rejections.
        error_threshold: Maximum error fraction (0-1) before aborting.
    """

    def __init__(
        self,
        name: str = "default-pipeline",
        batch_size: int = 10_000,
        parallel_extraction: bool = False,
        max_workers: int = 4,
        fail_fast: bool = False,
        error_threshold: float = 0.05,
    ) -> None:
        self.name = name
        self.batch_size = batch_size
        self.parallel_extraction = parallel_extraction
        self.max_workers = max_workers
        self.fail_fast = fail_fast
        self.error_threshold = error_threshold

        self._extractors: list[BaseExtractor] = []
        self._transformers: list[BaseTransformer] = []
        self._loaders: list[BaseLoader] = []

    # -- Builder API --------------------------------------------------------

    def add_extractor(self, extractor: BaseExtractor) -> Pipeline:
        """Register an extractor. Returns self for chaining."""
        self._extractors.append(extractor)
        return self

    def add_transformer(self, transformer: BaseTransformer) -> Pipeline:
        """Register a transformer. Transformers execute in insertion order."""
        self._transformers.append(transformer)
        return self

    def add_loader(self, loader: BaseLoader) -> Pipeline:
        """Register a loader. Returns self for chaining."""
        self._loaders.append(loader)
        return self

    # -- Execution ----------------------------------------------------------

    def run(self, run_id: str | None = None) -> PipelineResult:
        """Execute the full extract-transform-load cycle.

        Args:
            run_id: Optional unique identifier for this run. Generated
                automatically if not provided.

        Returns:
            A ``PipelineResult`` with counts and diagnostics.
        """
        run_id = run_id or uuid.uuid4().hex[:12]
        monitor = PipelineMonitor(self.name, run_id)
        monitor.start()

        result = PipelineResult(run_id=run_id, success=False)

        try:
            # --- Extract ---
            raw_frames = self._run_extraction(monitor)
            if not raw_frames:
                raise RuntimeError("All extractors returned empty results")

            combined = pd.concat(raw_frames, ignore_index=True)
            result.records_extracted = len(combined)

            # --- Transform ---
            transformed, rejected = self._run_transformation(combined, monitor)
            result.records_rejected = rejected

            # Check error threshold
            if result.records_extracted > 0:
                error_rate = rejected / result.records_extracted
                if error_rate > self.error_threshold:
                    raise RuntimeError(
                        f"Error rate {error_rate:.2%} exceeds threshold "
                        f"{self.error_threshold:.2%}"
                    )

            # --- Load ---
            loaded_count = self._run_loading(transformed, monitor)
            result.records_loaded = loaded_count
            result.success = True

        except Exception as exc:
            result.errors.append(str(exc))
            monitor.finalize(success=False, error_message=str(exc))
            logger.error("pipeline_run_failed", run_id=run_id, error=str(exc))
            return result

        monitor.finalize(success=True)
        result.metrics = monitor.metrics.summary()
        return result

    # -- Internal stage runners ---------------------------------------------

    def _run_extraction(self, monitor: PipelineMonitor) -> list[pd.DataFrame]:
        """Run all extractors, optionally in parallel."""
        if self.parallel_extraction and len(self._extractors) > 1:
            return self._extract_parallel(monitor)
        return self._extract_sequential(monitor)

    def _extract_sequential(self, monitor: PipelineMonitor) -> list[pd.DataFrame]:
        frames: list[pd.DataFrame] = []
        for extractor in self._extractors:
            stage_name = extractor.__class__.__name__
            with monitor.track_stage(stage_name, "extractor") as metrics:
                df = extractor.extract()
                metrics.records_out = len(df)
                frames.append(df)
        return frames

    def _extract_parallel(self, monitor: PipelineMonitor) -> list[pd.DataFrame]:
        frames: list[pd.DataFrame] = []

        def _do_extract(ext: BaseExtractor) -> tuple[str, pd.DataFrame]:
            name = ext.__class__.__name__
            df = ext.extract()
            return name, df

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(_do_extract, ext): ext for ext in self._extractors
            }
            for future in as_completed(futures):
                name, df = future.result()
                # Track after completion (timing is approximate for parallel)
                with monitor.track_stage(name, "extractor") as metrics:
                    metrics.records_out = len(df)
                frames.append(df)

        return frames

    def _run_transformation(
        self,
        df: pd.DataFrame,
        monitor: PipelineMonitor,
    ) -> tuple[pd.DataFrame, int]:
        """Apply all transformers in order. Returns (result_df, rejected_count)."""
        total_rejected = 0
        current = df

        for transformer in self._transformers:
            stage_name = transformer.__class__.__name__
            with monitor.track_stage(stage_name, "transformer") as metrics:
                metrics.records_in = len(current)
                current, rejected_count = transformer.transform(current)
                metrics.records_out = len(current)
                metrics.records_error = rejected_count
                total_rejected += rejected_count

                if self.fail_fast and rejected_count > 0:
                    raise RuntimeError(
                        f"Fail-fast: {stage_name} rejected {rejected_count} records"
                    )

        return current, total_rejected

    def _run_loading(self, df: pd.DataFrame, monitor: PipelineMonitor) -> int:
        """Load data in batches through all registered loaders."""
        total_loaded = 0

        for loader in self._loaders:
            stage_name = loader.__class__.__name__
            with monitor.track_stage(stage_name, "loader") as metrics:
                metrics.records_in = len(df)
                loaded = 0

                # Batch the DataFrame
                for start in range(0, len(df), self.batch_size):
                    batch = df.iloc[start : start + self.batch_size]
                    loader.load(batch)
                    loaded += len(batch)

                metrics.records_out = loaded
                total_loaded += loaded

        return total_loaded
