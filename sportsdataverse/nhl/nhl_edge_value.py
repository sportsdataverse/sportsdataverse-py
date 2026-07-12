"""NHL EDGE skating value (T5.2 model 2, NHL-only).

Builds a per-skater z-composite ``skating_value`` from EDGE player-tracking
aggregates (top speed, distance, 20+ mph speed bursts, offensive-zone time),
each z-scored league-wide and blended by
``get_constants(league).edge_component_weights``.

EDGE tracking is ``api-web.nhle.com``-only -- **the PWHL branch short-circuits
to a documented zero-row frame** (no EDGE feed exists). The EDGE leaderboard
URLs (``*_top_10``) 404 as of 2026-05-23, so the live path reads the per-skater
``nhl_edge_skater_*_detail`` endpoints (see the ``detail_frames`` capture
contract in ``tests/fixtures/nhl_microstat/README.md``).

**Flesh-out (T5.2):** two additive options over the original 4-component
equal-weight z-composite (default output unchanged unless requested):

- ``method="percentile"`` -- a rank-percentile composite (mean of each
  component's percentile rank instead of its z-score). Component scales in
  the captured fixture differ by 3 orders of magnitude (``speed_bursts_20``
  std ~58 vs ``oz_time_pct`` std ~0.03), so a percentile blend is an
  outlier-robust alternative to z-scoring, not a replacement for it.
- ``include_zone_balance=True`` -- adds a 5th component,
  ``oz_dz_time_balance = oz_time_pct - dz_time_pct`` (a "north-skew"
  possession-tendency signal), when ``dz_time_pct`` is present in
  ``detail_frames``. Off by default so the historical 4-component output is
  unchanged unless a caller opts in.

**Documented ceiling** (neither variant escapes this): EDGE-only skating
value has no opponent/team-context adjustment -- a player on a
puck-possession-dominant team accrues elevated ``oz_time_pct`` independent of
personal skating speed, and the single-season snapshot can't separate the
two without shift-level on-ice-vs-off-ice splits, which the current
league-wide EDGE detail endpoints don't expose. The concurrent rank-corr
oracle (below) and the joint-face-validity check confirm internal
consistency, not that the composite isolates individual skill from team
context.

Example:
    Quick start (offline, pre-parsed aggregate)::

        from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

        out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)
        print(out.sort("skating_value_rank").head())

    Percentile composite + zone-balance component::

        out = nhl_edge_skating_value(
            season=2024, detail_frames=edge_df, method="percentile", include_zone_balance=True
        )

See Also:
    * `nhl-api-py`_ -- Python NHL API client (companion data source).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_microstat_constants import get_constants

_COMPONENTS = ("top_speed", "distance_km", "speed_bursts_20", "oz_time_pct")
_ZONE_BALANCE_COMPONENT = "oz_dz_time_balance"

VALUE_SCHEMA = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "top_speed": pl.Float64,
    "distance_km": pl.Float64,
    "speed_bursts_20": pl.Float64,
    "oz_time_pct": pl.Float64,
    "skating_value": pl.Float64,
    "skating_value_rank": pl.Int64,
}


def _edge_zcomposite(
    components: pl.DataFrame,
    weights: dict[str, float],
    *,
    method: Literal["zscore", "percentile"] = "zscore",
    include_zone_balance: bool = False,
) -> pl.DataFrame:
    """Blend weighted components league-wide into a ``skating_value`` composite.

    Args:
        components: Per-skater frame with the raw component columns.
        weights: Component weight by name (:data:`_COMPONENTS`, plus
            :data:`_ZONE_BALANCE_COMPONENT` when ``include_zone_balance=True``).
        method: ``"zscore"`` (default, the original composite) or
            ``"percentile"`` -- mean percentile rank across components
            instead of mean z-score, robust to the components' very
            different raw scales/distributions.
        include_zone_balance: add ``oz_dz_time_balance = oz_time_pct -
            dz_time_pct`` as a 5th component when ``dz_time_pct`` is present.

    Returns:
        ``components`` with ``skating_value`` (Float64) and
        ``skating_value_rank`` (Int64, 1 = highest) appended.
    """
    component_names: tuple[str, ...]
    if include_zone_balance and "dz_time_pct" in components.columns and "oz_time_pct" in components.columns:
        components = components.with_columns(
            (pl.col("oz_time_pct") - pl.col("dz_time_pct")).alias(_ZONE_BALANCE_COMPONENT)
        )
        component_names = (*_COMPONENTS, _ZONE_BALANCE_COMPONENT)
    else:
        component_names = _COMPONENTS

    present = [c for c in component_names if c in components.columns and weights.get(c, 0.0) != 0.0]
    total_w = sum(weights[c] for c in present)
    if not present or total_w == 0:
        return components.with_columns(
            pl.lit(0.0).alias("skating_value"),
            pl.lit(None, dtype=pl.Int64).alias("skating_value_rank"),
        )

    terms = []
    n = components.height
    for c in present:
        if method == "percentile":
            # Percentile rank in [0, 1]; n==1 has no spread, so fall back to 0.5.
            term = (pl.col(c).rank(method="average") - 1.0) / (n - 1) if n > 1 else pl.lit(0.5)
        else:
            mean = components[c].mean()
            std = components[c].std()
            term = ((pl.col(c) - mean) / std) if std not in (None, 0) else pl.lit(0.0)
        terms.append(term * weights[c])
    composite = sum(terms) / total_w
    out = components.with_columns(composite.alias("skating_value"))
    out = out.with_columns(
        pl.col("skating_value").rank(method="ordinal", descending=True).cast(pl.Int64).alias("skating_value_rank")
    )
    return out


@overload
def nhl_edge_skating_value(
    *,
    season: int,
    league: str = ...,
    detail_frames: pl.DataFrame | None = ...,
    method: Literal["zscore", "percentile"] = ...,
    include_zone_balance: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def nhl_edge_skating_value(
    *,
    season: int,
    league: str = ...,
    detail_frames: pl.DataFrame | None = ...,
    method: Literal["zscore", "percentile"] = ...,
    include_zone_balance: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nhl_edge_skating_value(
    *,
    season: int,
    league: str = "nhl",
    detail_frames: pl.DataFrame | None = None,
    method: Literal["zscore", "percentile"] = "zscore",
    include_zone_balance: bool = False,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-skater EDGE skating-value composite (z-score or percentile blend).

    Args:
        season: Season end-year (e.g. ``2024`` for 2023-24).
        league: ``"nhl"`` or ``"pwhl"``. PWHL short-circuits to a zero-row
            frame (no EDGE feed) BEFORE any network access.
        detail_frames: Pre-parsed EDGE aggregate (one row per skater with the
            :data:`_COMPONENTS` columns) for offline use. When ``None`` on the
            NHL path, the live per-skater ``nhl_edge_skater_*_detail`` fetch
            would run -- not implemented offline; supply ``detail_frames``.
        method: ``"zscore"`` (default, original composite) or ``"percentile"``
            -- see the module docstring's flesh-out note.
        include_zone_balance: add the derived ``oz_dz_time_balance`` component
            when ``dz_time_pct`` is present in ``detail_frames`` (default
            ``False`` -- preserves the original 4-component output).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-skater frame: ``player_id``, ``season``, ``top_speed``,
        ``distance_km``, ``speed_bursts_20``, ``oz_time_pct``,
        ``skating_value``, ``skating_value_rank`` (1 = fastest composite),
        plus ``oz_dz_time_balance`` when ``include_zone_balance=True`` and
        derivable. PWHL (or empty/absent input) returns a zero-row frame with
        the base schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

            out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)

        Percentile composite + zone-balance component::

            out = nhl_edge_skating_value(
                season=2024, detail_frames=edge_df, method="percentile", include_zone_balance=True
            )

    See Also:
        * `nhl-api-py`_ -- Python NHL API client (companion data source).

    .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    empty = pl.DataFrame(schema=VALUE_SCHEMA)
    if league == "pwhl" or detail_frames is None or detail_frames.height == 0:
        return empty.to_pandas() if return_as_pandas else empty

    weights = get_constants(league).edge_component_weights
    scored = _edge_zcomposite(detail_frames, weights, method=method, include_zone_balance=include_zone_balance)
    select_cols = [
        pl.col("player_id"),
        pl.col("season"),
        pl.col("top_speed").cast(pl.Float64),
        pl.col("distance_km").cast(pl.Float64),
        pl.col("speed_bursts_20").cast(pl.Float64),
        pl.col("oz_time_pct").cast(pl.Float64),
        pl.col("skating_value").cast(pl.Float64),
        pl.col("skating_value_rank").cast(pl.Int64),
    ]
    if _ZONE_BALANCE_COMPONENT in scored.columns:
        select_cols.append(pl.col(_ZONE_BALANCE_COMPONENT).cast(pl.Float64))
    out = scored.with_columns(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    ).select(select_cols)
    return out.to_pandas() if return_as_pandas else out
