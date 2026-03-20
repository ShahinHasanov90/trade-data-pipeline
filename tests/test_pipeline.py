"""Unit tests for the Pipeline orchestrator.

Tests cover:
    - Builder API (add_extractor, add_transformer, add_loader)
    - Sequential extraction and transformation
    - Parallel extraction
    - Error threshold enforcement
    - Fail-fast behaviour
    - PipelineResult summary
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from etl.extractors.base import BaseExtractor
from etl.loaders.base import BaseLoader
from etl.pipeline import Pipeline, PipelineResult
from etl.transformers.base import BaseTransformer


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class FakeExtractor(BaseExtractor):
    """Extractor that returns a pre-built DataFrame."""

    def __init__(self, data: pd.DataFrame) -> None:
        self._data = data

    def extract(self) -> pd.DataFrame:
        return self._data.copy()


class PassThroughTransformer(BaseTransformer):
    """Transformer that returns the input unchanged."""

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        return df, 0


class RejectingTransformer(BaseTransformer):
    """Transformer that rejects a fixed number of rows from the top."""

    def __init__(self, reject_count: int = 1) -> None:
        self._reject = reject_count

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        keep = df.iloc[self._reject :]
        return keep.reset_index(drop=True), self._reject


class InMemoryLoader(BaseLoader):
    """Loader that accumulates records in a list for assertions."""

    def __init__(self) -> None:
        self.loaded_batches: list[pd.DataFrame] = []

    def load(self, df: pd.DataFrame) -> int:
        self.loaded_batches.append(df.copy())
        return len(df)

    @property
    def total_loaded(self) -> int:
        return sum(len(b) for b in self.loaded_batches)


class FailingExtractor(BaseExtractor):
    """Extractor that always raises."""

    def extract(self) -> pd.DataFrame:
        raise RuntimeError("Source unavailable")


class FailingLoader(BaseLoader):
    """Loader that always raises."""

    def load(self, df: pd.DataFrame) -> int:
        raise RuntimeError("Target unavailable")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "declaration_id": ["D001", "D002", "D003", "D004", "D005"],
            "hs_code": ["847130", "300490", "271019", "620342", "090111"],
            "declared_value": [1500.0, 200.0, 87000.0, 450.0, 3200.0],
            "country_origin": ["CN", "DE", "RU", "TR", "BR"],
        }
    )


@pytest.fixture
def pipeline_with_data(sample_df: pd.DataFrame) -> tuple[Pipeline, InMemoryLoader]:
    loader = InMemoryLoader()
    p = Pipeline(name="test-pipeline", batch_size=2)
    p.add_extractor(FakeExtractor(sample_df))
    p.add_transformer(PassThroughTransformer())
    p.add_loader(loader)
    return p, loader


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineBuilder:
    """Tests for the fluent builder API."""

    def test_add_extractor_returns_self(self, sample_df: pd.DataFrame) -> None:
        p = Pipeline()
        result = p.add_extractor(FakeExtractor(sample_df))
        assert result is p

    def test_add_transformer_returns_self(self) -> None:
        p = Pipeline()
        result = p.add_transformer(PassThroughTransformer())
        assert result is p

    def test_add_loader_returns_self(self) -> None:
        p = Pipeline()
        result = p.add_loader(InMemoryLoader())
        assert result is p

    def test_chaining(self, sample_df: pd.DataFrame) -> None:
        loader = InMemoryLoader()
        p = (
            Pipeline(name="chained")
            .add_extractor(FakeExtractor(sample_df))
            .add_transformer(PassThroughTransformer())
            .add_loader(loader)
        )
        assert len(p._extractors) == 1
        assert len(p._transformers) == 1
        assert len(p._loaders) == 1


class TestPipelineExecution:
    """Tests for the full extract-transform-load cycle."""

    def test_basic_run(
        self,
        pipeline_with_data: tuple[Pipeline, InMemoryLoader],
    ) -> None:
        p, loader = pipeline_with_data
        result = p.run()

        assert result.success is True
        assert result.records_extracted == 5
        assert result.records_loaded == 5
        assert result.records_rejected == 0
        assert loader.total_loaded == 5

    def test_batching(
        self,
        pipeline_with_data: tuple[Pipeline, InMemoryLoader],
    ) -> None:
        """With batch_size=2 and 5 records, expect 3 batches."""
        p, loader = pipeline_with_data
        p.run()
        # 5 records / batch_size 2 = 3 batches (2 + 2 + 1)
        assert len(loader.loaded_batches) == 3
        assert len(loader.loaded_batches[0]) == 2
        assert len(loader.loaded_batches[1]) == 2
        assert len(loader.loaded_batches[2]) == 1

    def test_custom_run_id(self, sample_df: pd.DataFrame) -> None:
        p = Pipeline()
        p.add_extractor(FakeExtractor(sample_df))
        p.add_loader(InMemoryLoader())
        result = p.run(run_id="custom-123")
        assert result.run_id == "custom-123"

    def test_generated_run_id(self, sample_df: pd.DataFrame) -> None:
        p = Pipeline()
        p.add_extractor(FakeExtractor(sample_df))
        p.add_loader(InMemoryLoader())
        result = p.run()
        assert len(result.run_id) == 12  # uuid hex[:12]

    def test_multiple_extractors_sequential(self, sample_df: pd.DataFrame) -> None:
        loader = InMemoryLoader()
        p = Pipeline(name="multi-extract", parallel_extraction=False)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_extractor(FakeExtractor(sample_df))
        p.add_loader(loader)

        result = p.run()
        assert result.records_extracted == 10
        assert result.records_loaded == 10

    def test_multiple_extractors_parallel(self, sample_df: pd.DataFrame) -> None:
        loader = InMemoryLoader()
        p = Pipeline(name="parallel", parallel_extraction=True, max_workers=2)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_extractor(FakeExtractor(sample_df))
        p.add_loader(loader)

        result = p.run()
        assert result.records_extracted == 10
        assert result.records_loaded == 10


class TestPipelineRejection:
    """Tests for rejection handling and error thresholds."""

    def test_rejected_records_counted(self, sample_df: pd.DataFrame) -> None:
        loader = InMemoryLoader()
        p = Pipeline(name="reject-test", error_threshold=0.5)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_transformer(RejectingTransformer(reject_count=2))
        p.add_loader(loader)

        result = p.run()
        assert result.records_rejected == 2
        assert result.records_loaded == 3

    def test_error_threshold_exceeded(self, sample_df: pd.DataFrame) -> None:
        """Rejecting 4/5 = 80% should exceed 5% threshold."""
        p = Pipeline(name="threshold-test", error_threshold=0.05)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_transformer(RejectingTransformer(reject_count=4))
        p.add_loader(InMemoryLoader())

        result = p.run()
        assert result.success is False
        assert any("threshold" in e.lower() for e in result.errors)

    def test_error_threshold_not_exceeded(self, sample_df: pd.DataFrame) -> None:
        """Rejecting 1/5 = 20% with 50% threshold should pass."""
        loader = InMemoryLoader()
        p = Pipeline(name="ok-threshold", error_threshold=0.5)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_transformer(RejectingTransformer(reject_count=1))
        p.add_loader(loader)

        result = p.run()
        assert result.success is True

    def test_fail_fast(self, sample_df: pd.DataFrame) -> None:
        p = Pipeline(name="fail-fast", fail_fast=True)
        p.add_extractor(FakeExtractor(sample_df))
        p.add_transformer(RejectingTransformer(reject_count=1))
        p.add_loader(InMemoryLoader())

        result = p.run()
        assert result.success is False
        assert any("fail-fast" in e.lower() for e in result.errors)


class TestPipelineErrors:
    """Tests for error handling during extraction and loading."""

    def test_extraction_failure(self) -> None:
        p = Pipeline()
        p.add_extractor(FailingExtractor())
        p.add_loader(InMemoryLoader())

        result = p.run()
        assert result.success is False
        assert len(result.errors) > 0

    def test_loader_failure(self, sample_df: pd.DataFrame) -> None:
        p = Pipeline()
        p.add_extractor(FakeExtractor(sample_df))
        p.add_loader(FailingLoader())

        result = p.run()
        assert result.success is False

    def test_no_extractors(self) -> None:
        p = Pipeline()
        p.add_loader(InMemoryLoader())
        result = p.run()
        assert result.success is False


class TestPipelineResult:
    """Tests for PipelineResult formatting."""

    def test_summary_success(self) -> None:
        r = PipelineResult(
            run_id="abc",
            success=True,
            records_extracted=100,
            records_loaded=95,
            records_rejected=5,
        )
        text = r.summary()
        assert "SUCCESS" in text
        assert "100" in text
        assert "95" in text

    def test_summary_failure_with_errors(self) -> None:
        r = PipelineResult(
            run_id="def",
            success=False,
            errors=["Connection refused", "Timeout"],
        )
        text = r.summary()
        assert "FAILED" in text
        assert "Connection refused" in text

    def test_summary_truncates_long_error_list(self) -> None:
        r = PipelineResult(
            run_id="ghi",
            success=False,
            errors=[f"Error {i}" for i in range(10)],
        )
        text = r.summary()
        assert "... and 5 more" in text
