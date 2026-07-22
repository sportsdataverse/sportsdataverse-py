"""Train-time model-build guards (WS6 fold-in).

The reference ``audit_input_file`` discipline as reusable checks a retrain script
runs BEFORE fitting: required-field audit, fail-fast on null targets /
baselines, post-split constant-column drop, and a greedy multicollinearity
cap (keep a feature iff it correlates with the target at least ``min`` and
with every already-kept feature at most ``max``). The publish/validation
harness guards released *data*; these guard *training inputs* — the gap the
reference notes called out.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
retrain scripts and model spines import from here.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np
import polars as pl


def audit_training_frame(
    df: pl.DataFrame,
    *,
    target: str,
    features: Sequence[str],
    baseline: Optional[str] = None,
    allow_null_target: bool = False,
) -> List[str]:
    """Required-field audit for a training frame; non-empty result = do not fit.

    Args:
        df: Candidate training frame.
        target: Target column name.
        features: Feature column names the model will consume.
        baseline: Optional baseline-prediction column (beat-the-baseline
            input); when given it must exist and be fully non-null.
        allow_null_target: Permit null targets (e.g. purposely-masked rows).

    Returns:
        List of error strings (empty = safe to fit).

    Example:
        Retrain preamble::

            from sportsdataverse.modeling.features.train_guards import audit_training_frame
            errors = audit_training_frame(train, target="is_over",
                                          features=FEATURES, baseline="p_vegas")
            assert not errors, errors
    """
    errors: List[str] = []
    if df.height == 0:
        errors.append("training frame is empty")
        return errors
    required = [target, *features, *([baseline] if baseline else [])]
    missing = sorted(set(required) - set(df.columns))
    if missing:
        errors.append(f"missing columns {missing}")
        return errors
    null_target = int(df[target].null_count())
    if null_target and not allow_null_target:
        errors.append(f"target {target!r} has {null_target} null rows")
    if baseline is not None:
        null_baseline = int(df[baseline].null_count())
        if null_baseline:
            errors.append(f"baseline {baseline!r} has {null_baseline} null rows")
    all_null = [f for f in features if df[f].null_count() == df.height]
    if all_null:
        errors.append(f"features entirely null: {all_null}")
    return errors


def drop_constant_columns(df: pl.DataFrame, features: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Split features into (kept, dropped-constant) — run AFTER the split.

    A column constant on the training split carries no signal and can break
    scalers; the reference stack dropped them post-split so a split-induced constant is
    caught too.

    Args:
        df: The training split.
        features: Candidate feature names (must exist in ``df``).

    Returns:
        ``(kept, dropped)`` feature-name lists, original order preserved.

    Example:
        Quick start::

            from sportsdataverse.modeling.features.train_guards import drop_constant_columns
            kept, dropped = drop_constant_columns(train, FEATURES)
    """
    kept: List[str] = []
    dropped: List[str] = []
    for feature in features:
        n_unique = int(df[feature].drop_nulls().n_unique())
        (kept if n_unique > 1 else dropped).append(feature)
    return kept, dropped


def correlation_prune(
    df: pl.DataFrame,
    features: Sequence[str],
    target: str,
    *,
    min_target_corr: float = 0.0,
    max_cross_corr: float = 0.95,
) -> Tuple[List[str], List[str]]:
    """Greedy multicollinearity cap: keep features by target-corr rank.

    Features are ranked by absolute Pearson correlation with the target;
    walking that order, a feature is kept iff its target correlation clears
    ``min_target_corr`` AND its correlation with every already-kept feature
    stays under ``max_cross_corr`` (the reference auto-correlation-selection
    recipe).

    Args:
        df: Training frame (rows with nulls in the used columns are ignored).
        features: Candidate numeric feature names.
        target: Numeric target column.
        min_target_corr: Minimum |corr(feature, target)| to keep.
        max_cross_corr: Maximum |corr(feature, kept-feature)| tolerated.

    Returns:
        ``(kept, dropped)`` feature-name lists (kept in rank order).

    Example:
        Quick start::

            from sportsdataverse.modeling.features.train_guards import correlation_prune
            kept, dropped = correlation_prune(train, FEATURES, "epa",
                                              min_target_corr=0.02)
    """
    cols = [*features, target]
    clean = df.select(cols).drop_nulls()
    if clean.height < 3:
        return list(features), []
    matrix = clean.to_numpy().astype(float)
    with np.errstate(invalid="ignore"):
        corr = np.corrcoef(matrix, rowvar=False)
    corr = np.nan_to_num(corr, nan=0.0)
    target_corr = {f: abs(float(corr[i, -1])) for i, f in enumerate(features)}
    order = sorted(features, key=lambda f: target_corr[f], reverse=True)

    index = {f: i for i, f in enumerate(features)}
    kept: List[str] = []
    dropped: List[str] = []
    for feature in order:
        if target_corr[feature] < min_target_corr:
            dropped.append(feature)
            continue
        cross = [abs(float(corr[index[feature], index[k]])) for k in kept]
        if any(c > max_cross_corr for c in cross):
            dropped.append(feature)
        else:
            kept.append(feature)
    return kept, dropped
