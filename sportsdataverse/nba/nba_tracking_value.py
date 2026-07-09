"""nba_tracking_value -- SportVU-tracking over-expected value models (league-agnostic).

Six descriptive value models, each a residual of a realized tracking outcome
against a computed league/role baseline for the same season slice:
``residual = realized - opportunities * (sum(realized)/sum(opportunities))`` within
each role bucket. Baselines are recomputed every call (no fitted constant, no
artifact). ``league_id`` selects NBA ("00") / WNBA ("10") / G-League ("20").

Methodology follows the published Second Spectrum / SportVU over-expected
conventions (public rebound-chance and defended-FG% definitions) -- original
implementation, nothing ported.
"""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

from sportsdataverse.nba.nba_player_positions import nba_player_positions
from sportsdataverse.nba.nba_stats import nba_stats_leaguedashptstats
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

__all__ = ["_season_str", "_pin_ids", "_fetch_leaguedash_tracking", "_attach_role_bucket", "_over_expected"]


def _season_str(season: "int | str") -> str:
    """Normalize a season to the ``"YYYY-YY"`` string ``stats.nba.com`` expects.

    Args:
        season: Either an ``int`` season-ending year (``2024`` -> the 2023-24
            season) or an already-formatted ``"YYYY-YY"`` string (passthrough).

    Returns:
        The ``"YYYY-YY"`` season string.
    """
    if isinstance(season, str):
        return season
    return f"{season - 1}-{str(season)[-2:]}"


def _pin_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Pin ``player_id``/``team_id`` to ``Utf8`` via an ``Int64`` intermediate cast.

    Casting straight from a float would stringify a float-origin id as
    ``"123.0"``; the ``Int64`` intermediate avoids that (see project ID/join-key
    discipline).

    Args:
        df: Frame that may carry ``player_id`` and/or ``team_id``.

    Returns:
        *df* with those columns cast to ``Utf8`` (columns not present are untouched).
    """
    for col in ("player_id", "team_id"):
        if col in df.columns and df.schema[col] != pl.Utf8:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(col))
    return df


def _fetch_leaguedash_tracking(
    season: "int | str",
    measure: str,
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    player_or_team: str = "Player",
    _get_fn: Optional[Callable[..., dict]] = None,
) -> pl.DataFrame:
    """Fetch + parse one ``leaguedashptstats`` measure, ids pinned to ``Utf8``.

    Args:
        season: Season (``int`` ending-year or ``"YYYY-YY"`` string).
        measure: ``pt_measure_type`` value, e.g. ``"Rebounding"``, ``"Drives"``.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` value (default ``"Totals"``).
        player_or_team: ``"Player"`` or ``"Team"``.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats`` that
            returns the raw ``{resultSets: [...]}`` dict directly -- lets offline
            tests bypass the live transport entirely.

    Returns:
        A tidy ``pl.DataFrame`` with ``player_id``/``team_id`` as ``Utf8``, or a
        zero-row ``pl.DataFrame()`` on an empty/malformed payload.
    """
    season_str = _season_str(season)
    fetch = _get_fn if _get_fn is not None else nba_stats_leaguedashptstats
    raw = fetch(
        pt_measure_type=measure,
        season=season_str,
        league_id=league_id,
        per_mode_simple=per_mode,
        player_or_team=player_or_team,
        return_parsed=False,
    )
    df = parse_nba_stats_result_sets(raw)
    if not isinstance(df, pl.DataFrame) or df.height == 0:
        return pl.DataFrame()
    return _pin_ids(df)


def _position_num_to_bucket(position_num: float) -> str:
    """Map the BPM 1-5 numeric position scale to a ``guard``/``wing``/``big`` bucket."""
    if position_num < 2.5:
        return "guard"
    if position_num < 3.75:
        return "wing"
    return "big"


def _attach_role_bucket(
    df: pl.DataFrame,
    season: "int | str",
    *,
    league_id: str = "00",
    positions: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """Left-join a ``guard``/``wing``/``big`` role bucket onto *df* by ``player_id``.

    When *positions* is not supplied, loads them via :func:`nba_player_positions`
    (numeric 1-5 scale) and buckets guard/wing/big off that scale. Missing
    positions -- or a wholly unavailable positions source (sparse G-League/early
    WNBA tracking) -- degrade to the single ``"all"`` bucket rather than raising.

    Args:
        df: Frame carrying ``player_id`` (``Utf8``).
        season: Season passed through to :func:`nba_player_positions` when *positions*
            is not supplied.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        positions: Optional pre-fetched positions frame with either
            ``player_id:Utf8, position_bucket:Utf8`` (used as-is) or
            ``player_id, position_num`` (bucketed here). Injectable for tests.

    Returns:
        *df* with an added ``position_bucket`` column (``"all"`` fill for any
        player without a resolved bucket).
    """
    if df.height == 0:
        return df.with_columns(pl.lit("all").alias("position_bucket"))

    if positions is None:
        try:
            positions = nba_player_positions(_season_str(season), league_id=league_id)
        except Exception:
            positions = None

    if positions is None or positions.height == 0:
        return df.with_columns(pl.lit("all").alias("position_bucket"))

    if "position_bucket" not in positions.columns and "position_num" in positions.columns:
        positions = positions.with_columns(
            pl.col("position_num").map_elements(_position_num_to_bucket, return_dtype=pl.Utf8).alias("position_bucket")
        )

    positions = _pin_ids(positions)
    assert df.schema["player_id"] == positions.schema["player_id"], "player_id dtype mismatch before role join"
    out = df.join(positions.select("player_id", "position_bucket"), on="player_id", how="left")
    return out.with_columns(pl.col("position_bucket").fill_null("all"))


def _over_expected(
    df: pl.DataFrame,
    *,
    actual: str,
    denom: str,
    group_cols: list[str],
    out_prefix: str,
) -> pl.DataFrame:
    """The shared over-expected engine: ``residual = actual - denom * bucket_rate``.

    ``bucket_rate = Σ(actual)/Σ(denom)`` computed within each ``group_cols`` bucket
    from the SAME rows being scored (never a fitted/stored constant), so
    ``Σ(residual) == 0`` within every bucket by construction.

    Args:
        df: Input frame carrying *actual*, *denom*, and *group_cols*.
        actual: Realized-outcome column name.
        denom: Opportunity/denominator column name.
        group_cols: Baseline-scope columns (e.g. ``["position_bucket"]``); empty
            list computes one league-wide baseline.
        out_prefix: Output-column prefix -- produces ``{prefix}_baseline_rate``,
            ``{prefix}_expected``, ``{prefix}_oe``.

    Returns:
        *df* with the three added columns. When *df* is empty or missing *actual*/
        *denom*, the three columns are added as all-null ``Float64`` (graceful
        degradation, never raises).
    """
    rate_col, exp_col, oe_col = f"{out_prefix}_baseline_rate", f"{out_prefix}_expected", f"{out_prefix}_oe"
    if df.height == 0 or actual not in df.columns or denom not in df.columns:
        return df.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias(rate_col),
                pl.lit(None, dtype=pl.Float64).alias(exp_col),
                pl.lit(None, dtype=pl.Float64).alias(oe_col),
            ]
        )
    gb = group_cols or []
    rate_expr = (
        pl.when(pl.col(denom).sum() > 0)
        .then(pl.col(actual).sum() / pl.col(denom).sum())
        .otherwise(None)
        .alias(rate_col)
    )
    if gb:
        rate = df.group_by(gb).agg(rate_expr)
        out = df.join(rate, on=gb, how="left")
    else:
        rate = df.select(rate_expr)
        out = df.join(rate, how="cross")
    return out.with_columns((pl.col(denom).cast(pl.Float64) * pl.col(rate_col)).alias(exp_col)).with_columns(
        (pl.col(actual).cast(pl.Float64) - pl.col(exp_col)).alias(oe_col)
    )
