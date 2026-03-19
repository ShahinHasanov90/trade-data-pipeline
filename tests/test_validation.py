"""Unit tests for the ValidationTransformer.

Tests cover every supported rule type: required, type, min/max,
min_length/max_length, pattern, allowed values, and custom validators.
"""

from __future__ import annotations

import pandas as pd
import pytest

from etl.transformers.validation import ValidationTransformer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def customs_df() -> pd.DataFrame:
    """Sample customs declaration data with some intentional issues."""
    return pd.DataFrame(
        {
            "declaration_id": ["D001", "D002", "", "D004", None],
            "hs_code": ["847130", "3004", "271019XXXX", "620342", "090111"],
            "declared_value": [1500.0, -200.0, 87000.0, 450.0, 3200.0],
            "weight_kg": [10.5, None, 500.0, 2.3, 15.0],
            "country_origin": ["CN", "DE", "RU", "TR", "BR"],
            "currency": ["USD", "EUR", "GBP", "AZN", "INVALID"],
        }
    )


# ---------------------------------------------------------------------------
# Required field tests
# ---------------------------------------------------------------------------


class TestRequiredRule:
    def test_rejects_null_values(self) -> None:
        df = pd.DataFrame({"name": ["Alice", None, "Charlie"]})
        v = ValidationTransformer(rules={"name": {"required": True}})
        result, rejected = v.transform(df)
        assert rejected == 1
        assert len(result) == 2
        assert list(result["name"]) == ["Alice", "Charlie"]

    def test_rejects_empty_strings(self) -> None:
        df = pd.DataFrame({"name": ["Alice", "", "  "]})
        v = ValidationTransformer(rules={"name": {"required": True}})
        result, rejected = v.transform(df)
        assert rejected == 2
        assert list(result["name"]) == ["Alice"]

    def test_missing_required_column_rejects_all(self) -> None:
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        v = ValidationTransformer(rules={"name": {"required": True}})
        result, rejected = v.transform(df)
        assert rejected == 3
        assert result.empty

    def test_missing_optional_column_keeps_all(self) -> None:
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        v = ValidationTransformer(rules={"name": {"required": False}})
        result, rejected = v.transform(df)
        assert rejected == 0
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Type check tests
# ---------------------------------------------------------------------------


class TestTypeRule:
    def test_string_type(self) -> None:
        df = pd.DataFrame({"code": ["ABC", 123, "DEF"]})
        v = ValidationTransformer(rules={"code": {"type": "str"}})
        result, rejected = v.transform(df)
        assert rejected == 1
        assert len(result) == 2

    def test_float_type(self) -> None:
        df = pd.DataFrame({"value": [1.5, "not_a_number", 3.0, None]})
        v = ValidationTransformer(rules={"value": {"type": "float"}})
        result, rejected = v.transform(df)
        # "not_a_number" fails, None passes (nullable)
        assert rejected == 1
        assert len(result) == 3

    def test_float_accepts_integers(self) -> None:
        df = pd.DataFrame({"value": [1, 2.5, 3]})
        v = ValidationTransformer(rules={"value": {"type": "float"}})
        result, rejected = v.transform(df)
        assert rejected == 0


# ---------------------------------------------------------------------------
# Range tests
# ---------------------------------------------------------------------------


class TestRangeRules:
    def test_min_value(self) -> None:
        df = pd.DataFrame({"amount": [100.0, -5.0, 0.0, 50.0]})
        v = ValidationTransformer(rules={"amount": {"min": 0}})
        result, rejected = v.transform(df)
        assert rejected == 1
        assert len(result) == 3

    def test_max_value(self) -> None:
        df = pd.DataFrame({"amount": [100.0, 5000.0, 999.0]})
        v = ValidationTransformer(rules={"amount": {"max": 1000}})
        result, rejected = v.transform(df)
        assert rejected == 1

    def test_min_and_max(self) -> None:
        df = pd.DataFrame({"amount": [-1.0, 0.0, 500.0, 1000.0, 1001.0]})
        v = ValidationTransformer(rules={"amount": {"min": 0, "max": 1000}})
        result, rejected = v.transform(df)
        assert rejected == 2
        assert len(result) == 3

    def test_null_values_pass_range_check(self) -> None:
        df = pd.DataFrame({"amount": [100.0, None, 50.0]})
        v = ValidationTransformer(rules={"amount": {"min": 0}})
        result, rejected = v.transform(df)
        assert rejected == 0


# ---------------------------------------------------------------------------
# String length tests
# ---------------------------------------------------------------------------


class TestLengthRules:
    def test_min_length(self) -> None:
        df = pd.DataFrame({"code": ["AB", "ABCDE", "A"]})
        v = ValidationTransformer(rules={"code": {"min_length": 2}})
        result, rejected = v.transform(df)
        assert rejected == 1

    def test_max_length(self) -> None:
        df = pd.DataFrame({"code": ["AB", "ABCDEFGHIJK", "ABC"]})
        v = ValidationTransformer(rules={"code": {"max_length": 5}})
        result, rejected = v.transform(df)
        assert rejected == 1


# ---------------------------------------------------------------------------
# Pattern tests
# ---------------------------------------------------------------------------


class TestPatternRule:
    def test_hs_code_pattern(self) -> None:
        df = pd.DataFrame({"hs_code": ["847130", "3004", "ABCDEF", "09011100"]})
        v = ValidationTransformer(
            rules={"hs_code": {"pattern": r"^\d{6,10}$"}}
        )
        result, rejected = v.transform(df)
        # "3004" is only 4 digits, "ABCDEF" is not digits
        assert rejected == 2
        assert len(result) == 2

    def test_email_pattern(self) -> None:
        df = pd.DataFrame(
            {"email": ["user@example.com", "not-an-email", "a@b.co"]}
        )
        v = ValidationTransformer(
            rules={"email": {"pattern": r"^[^@]+@[^@]+\.[^@]+$"}}
        )
        result, rejected = v.transform(df)
        assert rejected == 1


# ---------------------------------------------------------------------------
# Allowed values tests
# ---------------------------------------------------------------------------


class TestAllowedRule:
    def test_allowed_values(self) -> None:
        df = pd.DataFrame({"currency": ["USD", "EUR", "INVALID", "GBP"]})
        v = ValidationTransformer(
            rules={"currency": {"allowed": ["USD", "EUR", "GBP"]}}
        )
        result, rejected = v.transform(df)
        assert rejected == 1
        assert "INVALID" not in result["currency"].values

    def test_null_passes_allowed(self) -> None:
        df = pd.DataFrame({"currency": ["USD", None, "EUR"]})
        v = ValidationTransformer(
            rules={"currency": {"allowed": ["USD", "EUR"]}}
        )
        result, rejected = v.transform(df)
        assert rejected == 0


# ---------------------------------------------------------------------------
# Custom validator tests
# ---------------------------------------------------------------------------


class TestCustomRule:
    def test_custom_validator(self) -> None:
        """Custom rule: value must be even."""
        df = pd.DataFrame({"number": [2, 3, 4, 5, 6]})
        v = ValidationTransformer(
            rules={"number": {"custom": lambda s: s % 2 == 0}}
        )
        result, rejected = v.transform(df)
        assert rejected == 2
        assert list(result["number"]) == [2, 4, 6]

    def test_failing_custom_validator_does_not_reject(self) -> None:
        """If custom validator raises, fail open (no rejection)."""
        df = pd.DataFrame({"value": [1, 2, 3]})

        def bad_validator(s: pd.Series) -> pd.Series:
            raise ValueError("Broken validator")

        v = ValidationTransformer(rules={"value": {"custom": bad_validator}})
        result, rejected = v.transform(df)
        assert rejected == 0
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Combined rules tests
# ---------------------------------------------------------------------------


class TestCombinedRules:
    def test_multiple_rules_on_one_column(self) -> None:
        df = pd.DataFrame({"hs_code": ["847130", "30", "ABCDEF", None]})
        v = ValidationTransformer(
            rules={
                "hs_code": {
                    "required": True,
                    "pattern": r"^\d{6,10}$",
                },
            }
        )
        result, rejected = v.transform(df)
        # "30" too short, "ABCDEF" not digits, None is null
        assert rejected == 3
        assert len(result) == 1

    def test_multiple_columns(self, customs_df: pd.DataFrame) -> None:
        v = ValidationTransformer(
            rules={
                "declaration_id": {"required": True},
                "declared_value": {"min": 0},
                "currency": {"allowed": ["USD", "EUR", "GBP", "AZN"]},
            }
        )
        result, rejected = v.transform(customs_df)
        # Row 1 (idx 0): ok
        # Row 2 (idx 1): declared_value = -200 (fails min)
        # Row 3 (idx 2): declaration_id = "" (fails required)
        # Row 4 (idx 3): ok
        # Row 5 (idx 4): declaration_id = None (fails required), currency = INVALID
        assert rejected == 3
        assert len(result) == 2

    def test_empty_dataframe(self) -> None:
        df = pd.DataFrame({"col": pd.Series([], dtype="object")})
        v = ValidationTransformer(rules={"col": {"required": True}})
        result, rejected = v.transform(df)
        assert rejected == 0
        assert result.empty


# ---------------------------------------------------------------------------
# Reject handler tests
# ---------------------------------------------------------------------------


class TestRejectHandler:
    def test_handler_receives_rejected_rows(self) -> None:
        rejected_store: list[pd.DataFrame] = []

        df = pd.DataFrame({"value": [1, -1, 2, -2]})
        v = ValidationTransformer(
            rules={"value": {"min": 0}},
            reject_handler=lambda rdf: rejected_store.append(rdf),
        )
        result, rejected = v.transform(df)
        assert rejected == 2
        assert len(rejected_store) == 1
        assert len(rejected_store[0]) == 2

    def test_handler_not_called_when_no_rejections(self) -> None:
        handler = lambda rdf: None  # noqa: E731
        handler_mock = pytest.importorskip("unittest.mock").MagicMock(
            side_effect=handler
        )

        df = pd.DataFrame({"value": [1, 2, 3]})
        v = ValidationTransformer(
            rules={"value": {"min": 0}},
            reject_handler=handler_mock,
        )
        v.transform(df)
        handler_mock.assert_not_called()
