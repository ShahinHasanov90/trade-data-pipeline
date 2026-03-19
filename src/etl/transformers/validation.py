"""Data validation transformer with configurable rules.

Validates each record against a rule set and splits the DataFrame into
valid and invalid partitions. Invalid records are counted (and optionally
logged) but not included in the output.

Supported rule keys per column:
    - ``required``  (bool)  -- column must exist and values must be non-null
    - ``type``      (str)   -- expected dtype: "str", "int", "float", "bool"
    - ``min``       (float) -- minimum numeric value (inclusive)
    - ``max``       (float) -- maximum numeric value (inclusive)
    - ``min_length`` (int)  -- minimum string length
    - ``max_length`` (int)  -- maximum string length
    - ``pattern``   (str)   -- regex pattern that values must match
    - ``allowed``   (list)  -- whitelist of allowed values
    - ``custom``    (callable) -- a function ``(pd.Series) -> pd.Series[bool]``
"""

from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd
import structlog

from etl.transformers.base import BaseTransformer

logger = structlog.get_logger(__name__)

# Map user-facing type names to pandas-compatible checks
_TYPE_CHECKERS: dict[str, Callable[[pd.Series], pd.Series]] = {
    "str": lambda s: s.apply(lambda v: isinstance(v, str)),
    "int": lambda s: pd.to_numeric(s, errors="coerce").notna() & s.apply(
        lambda v: isinstance(v, (int,)) or (isinstance(v, float) and v == int(v))
        if pd.notna(v)
        else False
    ),
    "float": lambda s: pd.to_numeric(s, errors="coerce").notna(),
    "bool": lambda s: s.apply(lambda v: isinstance(v, bool)),
}


class ValidationTransformer(BaseTransformer):
    """Validate DataFrame records against a declarative rule set.

    Args:
        rules: Dict mapping column names to rule dicts. Example::

            {
                "hs_code": {"required": True, "pattern": r"^\\d{6,10}$"},
                "declared_value": {"required": True, "type": "float", "min": 0},
            }

        reject_handler: Optional callable that receives the rejected
            DataFrame for custom handling (e.g. writing to a dead-letter
            table).
    """

    def __init__(
        self,
        rules: dict[str, dict[str, Any]],
        reject_handler: Callable[[pd.DataFrame], None] | None = None,
    ) -> None:
        self.rules = rules
        self.reject_handler = reject_handler

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Apply all validation rules and return valid records.

        Returns:
            Tuple of (valid_df, rejected_count).
        """
        if df.empty:
            return df, 0

        # Start with all rows valid
        valid_mask = pd.Series(True, index=df.index)

        for column, column_rules in self.rules.items():
            column_mask = self._validate_column(df, column, column_rules)
            valid_mask &= column_mask

        valid_df = df.loc[valid_mask].copy()
        rejected_df = df.loc[~valid_mask]
        rejected_count = len(rejected_df)

        if rejected_count > 0:
            logger.warning(
                "validation_rejections",
                rejected=rejected_count,
                total=len(df),
                rate=round(rejected_count / len(df), 4),
            )
            if self.reject_handler is not None:
                self.reject_handler(rejected_df)

        return valid_df.reset_index(drop=True), rejected_count

    def _validate_column(
        self,
        df: pd.DataFrame,
        column: str,
        rules: dict[str, Any],
    ) -> pd.Series:
        """Build a boolean mask for a single column's rules."""
        mask = pd.Series(True, index=df.index)

        # Check if column exists
        is_required = rules.get("required", False)
        if column not in df.columns:
            if is_required:
                logger.error("validation_missing_column", column=column)
                return pd.Series(False, index=df.index)
            # Column absent but not required -- skip all rules for it
            return mask

        series = df[column]

        # Required: non-null check
        if is_required:
            not_null = series.notna()
            # Also treat empty strings as missing for string-like columns
            if series.dtype == object:
                not_null = not_null & (series.astype(str).str.strip() != "")
            mask &= not_null

        # Type check
        type_name = rules.get("type")
        if type_name and type_name in _TYPE_CHECKERS:
            # Only check type on non-null values
            type_mask = _TYPE_CHECKERS[type_name](series)
            mask &= type_mask | series.isna()

        # Numeric range
        if "min" in rules:
            numeric = pd.to_numeric(series, errors="coerce")
            mask &= (numeric >= rules["min"]) | series.isna()

        if "max" in rules:
            numeric = pd.to_numeric(series, errors="coerce")
            mask &= (numeric <= rules["max"]) | series.isna()

        # String length
        if "min_length" in rules:
            str_series = series.astype(str)
            mask &= (str_series.str.len() >= rules["min_length"]) | series.isna()

        if "max_length" in rules:
            str_series = series.astype(str)
            mask &= (str_series.str.len() <= rules["max_length"]) | series.isna()

        # Regex pattern
        pattern = rules.get("pattern")
        if pattern:
            compiled = re.compile(pattern)
            pattern_mask = series.astype(str).apply(
                lambda v: bool(compiled.match(v)) if pd.notna(v) else True
            )
            mask &= pattern_mask

        # Allowed values whitelist
        allowed = rules.get("allowed")
        if allowed is not None:
            mask &= series.isin(allowed) | series.isna()

        # Custom validator
        custom_fn = rules.get("custom")
        if callable(custom_fn):
            try:
                custom_mask = custom_fn(series)
                mask &= custom_mask
            except Exception as exc:
                logger.error(
                    "custom_validator_failed",
                    column=column,
                    error=str(exc),
                )
                # Fail open -- do not reject on validator errors
                pass

        return mask

    def __repr__(self) -> str:
        columns = list(self.rules.keys())
        return f"<ValidationTransformer columns={columns}>"
