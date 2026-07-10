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

Example:
    Quick start (offline, pre-parsed aggregate)::

        from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

        out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)
        print(out.sort("skating_value_rank").head())

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


def _edge_zcomposite(components: pl.DataFrame, weights: dict[str, float]) -> pl.DataFrame:
    """z-score each weighted component league-wide and blend into skating_value.

    Args:
        components: Per-skater frame with the raw component columns.
        weights: Component weight by name (:data:`_COMPONENTS`).

    Returns:
        ``components`` with ``skating_value`` (Float64) and
        ``skating_value_rank`` (Int64, 1 = highest) appended.
    """
    present = [c for c in _COMPONENTS if c in components.columns and weights.get(c, 0.0) != 0.0]
    total_w = sum(weights[c] for c in present)
    if not present or total_w == 0:
        return components.with_columns(
            pl.lit(0.0).alias("skating_value"),
            pl.lit(None, dtype=pl.Int64).alias("skating_value_rank"),
        )
    z_terms = []
    for c in present:
        mean = components[c].mean()
        std = components[c].std()
        z = ((pl.col(c) - mean) / std) if std not in (None, 0) else pl.lit(0.0)
        z_terms.append(z * weights[c])
    composite = sum(z_terms) / total_w
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
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def nhl_edge_skating_value(
    *,
    season: int,
    league: str = ...,
    detail_frames: pl.DataFrame | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
def nhl_edge_skating_value(
    *,
    season: int,
    league: str = "nhl",
    detail_frames: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-skater EDGE z-composite skating value.

    Args:
        season: Season end-year (e.g. ``2024`` for 2023-24).
        league: ``"nhl"`` or ``"pwhl"``. PWHL short-circuits to a zero-row
            frame (no EDGE feed) BEFORE any network access.
        detail_frames: Pre-parsed EDGE aggregate (one row per skater with the
            :data:`_COMPONENTS` columns) for offline use. When ``None`` on the
            NHL path, the live per-skater ``nhl_edge_skater_*_detail`` fetch
            would run -- not implemented offline; supply ``detail_frames``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-skater frame: ``player_id``, ``season``, ``top_speed``,
        ``distance_km``, ``speed_bursts_20``, ``oz_time_pct``,
        ``skating_value``, ``skating_value_rank`` (1 = fastest composite).
        PWHL (or empty/absent input) returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

            out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)

    See Also:
        * `nhl-api-py`_ -- Python NHL API client (companion data source).

    .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    empty = pl.DataFrame(schema=VALUE_SCHEMA)
    if league == "pwhl" or detail_frames is None or detail_frames.height == 0:
        return empty.to_pandas() if return_as_pandas else empty

    weights = get_constants(league).edge_component_weights
    scored = _edge_zcomposite(detail_frames, weights)
    out = scored.with_columns(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    ).select(
        pl.col("player_id"),
        pl.col("season"),
        pl.col("top_speed").cast(pl.Float64),
        pl.col("distance_km").cast(pl.Float64),
        pl.col("speed_bursts_20").cast(pl.Float64),
        pl.col("oz_time_pct").cast(pl.Float64),
        pl.col("skating_value").cast(pl.Float64),
        pl.col("skating_value_rank").cast(pl.Int64),
    )
    return out.to_pandas() if return_as_pandas else out
