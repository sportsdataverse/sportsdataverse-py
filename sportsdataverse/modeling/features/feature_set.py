"""Declarative rolling-feature layer — spans × splits × aggfuncs (WS3).

The reference WindowedAggregationEngine contract, polars-native: one
:class:`FeatureSetSpec` declares an entity unit, rolling row-count windows
(``spans`` = last-N observations), situational split dimensions
(cross-producted), and per-column aggregations. The engine slices
point-in-time (strictly ``date < as_of`` — leakage-safe by construction),
keeps the most recent N rows per entity × split cell, aggregates, and emits
deterministically named wide columns (``{col}_{agg}___{span}``).

Contracts kept from the reference stack:

* required columns ⊆ frame schema, checked up front (fail fast);
* the splits grid is a full cross-product, so every declared cell exists in
  the output (missing cells carry nulls, never silently drop);
* empty input yields a zero-row frame with the documented schema.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
model spines and shelf builders import from here.
"""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import polars as pl

_AGG_BUILDERS = {
    "mean": lambda c: pl.col(c).mean(),
    "sum": lambda c: pl.col(c).sum(),
    "min": lambda c: pl.col(c).min(),
    "max": lambda c: pl.col(c).max(),
    "std": lambda c: pl.col(c).std(),
    "count": lambda c: pl.col(c).count(),
}

AsOf = Union[datetime.date, datetime.datetime, int, float, str]


@dataclasses.dataclass(frozen=True)
class FeatureSetSpec:
    """Declarative rolling-feature specification.

    Attributes:
        name: Feature-family name (used in provenance / ledger records).
        unit: Entity column the windows roll over (``player_id``, ``team_id``).
        aggfuncs: ``{column: (agg, ...)}`` — aggregations per value column.
            Valid aggs: ``mean`` / ``sum`` / ``min`` / ``max`` / ``std`` /
            ``count``.
        spans: Row-count windows — each span keeps the most recent N rows per
            entity × split cell. ``(0,)`` or an empty tuple means all history.
        splits: Situational dimensions cross-producted into the output grid,
            e.g. ``{"home_away": ("H", "A")}``. Empty = no splits.
        date_col: Point-in-time ordering column.
        min_rows: Cells with fewer observations than this emit nulls for the
            span's features (never a partial silent aggregate).
    """

    name: str
    unit: str
    aggfuncs: Mapping[str, Tuple[str, ...]]
    spans: Tuple[int, ...] = (0,)
    splits: Mapping[str, Tuple[Any, ...]] = dataclasses.field(default_factory=dict)
    date_col: str = "date"
    min_rows: int = 1

    def __post_init__(self) -> None:
        for col, aggs in self.aggfuncs.items():
            unknown = [a for a in aggs if a not in _AGG_BUILDERS]
            if unknown:
                raise ValueError(f"{col}: unknown aggs {unknown}; valid: {sorted(_AGG_BUILDERS)}")
        if not self.aggfuncs:
            raise ValueError("aggfuncs must declare at least one column")
        if any(s < 0 for s in self.spans):
            raise ValueError(f"spans must be non-negative, got {self.spans}")


def feature_column_names(spec: FeatureSetSpec) -> List[str]:
    """Deterministic output feature names: ``{col}_{agg}___{span}``.

    Args:
        spec: The feature specification.

    Returns:
        Feature column names in output order (span-major, then column, agg).

    Example:
        Quick start::

            from sportsdataverse.modeling.features.feature_set import (
                FeatureSetSpec, feature_column_names,
            )
            spec = FeatureSetSpec("shots", "player_id", {"pts": ("mean", "sum")}, spans=(10,))
            feature_column_names(spec)  # ['pts_mean___10', 'pts_sum___10']
    """
    names: List[str] = []
    for span in spec.spans:
        for col, aggs in spec.aggfuncs.items():
            for agg in aggs:
                names.append(f"{col}_{agg}___{span}")
    return names


def splits_grid(spec: FeatureSetSpec, units: pl.Series) -> pl.DataFrame:
    """Cross-product grid: every unit × every declared split combination.

    Args:
        spec: The feature specification.
        units: Entity ids to grid (deduplicated).

    Returns:
        One row per unit × split-value combination.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.modeling.features.feature_set import FeatureSetSpec, splits_grid
            spec = FeatureSetSpec("s", "player_id", {"pts": ("mean",)},
                                  splits={"home_away": ("H", "A")})
            splits_grid(spec, pl.Series([1, 2]))  # 4 rows
    """
    grid = pl.DataFrame({spec.unit: units.unique().sort()})
    for split_col, values in spec.splits.items():
        grid = grid.join(pl.DataFrame({split_col: list(values)}), how="cross")
    return grid


def _validate_columns(df: pl.DataFrame, spec: FeatureSetSpec) -> None:
    required = {spec.unit, spec.date_col, *spec.aggfuncs, *spec.splits}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"feature_set {spec.name!r}: missing columns {missing}")


def rolling_features(
    df: pl.DataFrame,
    spec: FeatureSetSpec,
    *,
    as_of: Optional[AsOf] = None,
) -> pl.DataFrame:
    """Compute the spec's rolling features, optionally as of a cutoff.

    Args:
        df: Long observation frame (one row per unit-event).
        spec: The feature specification.
        as_of: When given, only rows with ``date_col`` STRICTLY before this
            value contribute — the leakage guard is in the engine, not the
            caller.

    Returns:
        One row per unit × split combination present in the grid, with
        :func:`feature_column_names` columns appended. Zero-row input yields
        a zero-row frame with the same schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.modeling.features.feature_set import FeatureSetSpec, rolling_features
            spec = FeatureSetSpec("form", "player_id", {"pts": ("mean",)}, spans=(5, 25))
            feats = rolling_features(games, spec, as_of=game_date)
    """
    _validate_columns(df, spec)
    base = df
    if as_of is not None:
        base = base.filter(pl.col(spec.date_col) < as_of)

    group_cols = [spec.unit, *spec.splits.keys()]
    grid = splits_grid(spec, base.get_column(spec.unit))
    if base.height == 0:
        empty = grid.clear()
        for name in feature_column_names(spec):
            empty = empty.with_columns(pl.lit(None, dtype=pl.Float64).alias(name))
        return empty

    # most-recent-first so head(span) = the trailing window
    base = base.sort(spec.date_col, descending=True)

    out = grid
    for span in spec.spans:
        windowed = base.group_by(group_cols, maintain_order=True).head(span) if span > 0 else base
        aggs: List[pl.Expr] = [pl.len().alias("__n_obs")]
        for col, agg_names in spec.aggfuncs.items():
            for agg in agg_names:
                aggs.append(_AGG_BUILDERS[agg](col).alias(f"{col}_{agg}___{span}"))
        span_frame = windowed.group_by(group_cols).agg(aggs)
        span_cols = [f"{col}_{agg}___{span}" for col, agg_names in spec.aggfuncs.items() for agg in agg_names]
        # cells with fewer than min_rows observations emit nulls, not partial aggregates
        span_frame = span_frame.with_columns(
            [pl.when(pl.col("__n_obs") >= spec.min_rows).then(pl.col(c)).otherwise(None).alias(c) for c in span_cols]
        ).drop("__n_obs")
        out = out.join(span_frame, on=group_cols, how="left")
    return out


def as_of_features(
    df: pl.DataFrame,
    spec: FeatureSetSpec,
    as_of_dates: Sequence[AsOf],
) -> pl.DataFrame:
    """Stack point-in-time feature snapshots for a sequence of as-of dates.

    The trainer view: one :func:`rolling_features` block per cutoff, stacked
    long with an ``as_of`` column — every row's features use strictly
    pre-cutoff observations only.

    Args:
        df: Long observation frame.
        spec: The feature specification.
        as_of_dates: Cutoffs, one block each.

    Returns:
        The stacked frame (``as_of`` column first).

    Example:
        Training table::

            from sportsdataverse.modeling.features.feature_set import as_of_features
            train = as_of_features(games, spec, sorted(games["date"].unique()))
    """
    blocks = []
    for cutoff in as_of_dates:
        block = rolling_features(df, spec, as_of=cutoff)
        blocks.append(block.with_columns(pl.lit(cutoff).alias("as_of")))
    stacked = pl.concat(blocks, how="vertical_relaxed")
    return stacked.select(["as_of", *[c for c in stacked.columns if c != "as_of"]])


def fit_span_blend(
    df: pl.DataFrame,
    span_cols: Sequence[str],
    target_col: str,
) -> Dict[str, float]:
    """LEARN the recency-blend weights across span features (lesson #30).

    The reference stack hand-tuned multi-window blends (``0.6*w16 + 0.3*w8 + 0.1*w4``); this
    fits them instead: non-negative least squares of the target on the span
    columns, normalized to sum to 1. Feed it the ``as_of_features`` trainer
    view with the realized outcome joined on.

    Args:
        df: Frame holding the span feature columns and the realized target
            (rows with nulls in any used column are ignored).
        span_cols: The span variants of one feature
            (e.g. ``["pts_mean___5", "pts_mean___25", "pts_mean___0"]``).
        target_col: Realized outcome column the blend should predict.

    Returns:
        ``{span_col: weight}`` with non-negative weights summing to 1
        (uniform weights when the fit is degenerate).

    Raises:
        ValueError: When fewer than 2 span columns are given or columns are
            missing.

    Example:
        Quick start::

            from sportsdataverse.modeling.features.feature_set import fit_span_blend
            weights = fit_span_blend(train, ["pts_mean___5", "pts_mean___25"], "pts")
    """
    if len(span_cols) < 2:
        raise ValueError("fit_span_blend needs at least 2 span columns")
    missing = sorted({*span_cols, target_col} - set(df.columns))
    if missing:
        raise ValueError(f"fit_span_blend: missing columns {missing}")
    clean = df.select([*span_cols, target_col]).drop_nulls()
    uniform = {c: 1.0 / len(span_cols) for c in span_cols}
    if clean.height < len(span_cols) + 1:
        return uniform
    from scipy.optimize import nnls

    matrix = clean.select(span_cols).to_numpy().astype(float)
    target = clean.get_column(target_col).to_numpy().astype(float)
    weights, _ = nnls(matrix, target)
    total = float(weights.sum())
    if total <= 0:
        return uniform
    return {c: float(w / total) for c, w in zip(span_cols, weights)}
