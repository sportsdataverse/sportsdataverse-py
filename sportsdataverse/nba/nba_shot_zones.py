"""Shot-zone classification for the NBA v3 engine (pbpstats-core aligned)."""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

SHOT_ZONES: list[str] = [
    "restricted_area",
    "in_the_paint_non_ra",
    "mid_range",
    "corner_3",
    "above_the_break_3",
]


def add_shot_zones(enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Add shot-zone classification to enhanced PBP.

    Classifies field-goal attempts into NBA zones per pbpstats-core rules:
    - 3PT shots: ``corner_3`` if ``|x_legacy| >= 220`` **and**
      ``y_legacy <= 87.5``; else ``above_the_break_3``.
    - 2PT shots: ``restricted_area`` if ``shot_distance < 4``;
      ``in_the_paint_non_ra`` if ``shot_distance < 8``
      **and** ``|x_legacy| <= 80``; else ``mid_range``.
    - All other event types: ``null``.

    Args:
        enhanced_pbp: Enhanced PBP frame from :func:`enhanced_pbp_from_payload`,
            with columns ``is_field_goal``, ``shot_value``, ``shot_distance``,
            ``x_legacy``, ``y_legacy``.

    Returns:
        Frame with appended ``shot_zone`` column (Utf8, null on non-FG events).
        Null-safe: empty input returns zero-row frame with the column schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_shot_zones import add_shot_zones
            pbp = enhanced_pbp_from_payload(payload_dict)
            pbp_zones = add_shot_zones(pbp)
            print(pbp_zones[["shot_distance", "shot_zone"]].head())

        See Also:
            * `enhanced_pbp_from_payload` -- the Phase-1 engine
                that produces the input frame.
    """
    if enhanced_pbp.is_empty():
        return enhanced_pbp.with_columns(pl.lit(None, dtype=pl.Utf8).alias("shot_zone"))

    x = pl.col("x_legacy").abs()
    dist = pl.col("shot_distance")
    is_fg = pl.col("is_field_goal") == 1
    is_three = pl.col("shot_value") == 3

    zone = (
        pl.when(~is_fg)
        .then(None)
        .when(is_three & (x >= 220) & (pl.col("y_legacy") <= 87.5))
        .then(pl.lit("corner_3"))
        .when(is_three)
        .then(pl.lit("above_the_break_3"))
        .when(dist < 4)
        .then(pl.lit("restricted_area"))
        .when((dist < 8) & (x <= 80))
        .then(pl.lit("in_the_paint_non_ra"))
        .otherwise(pl.lit("mid_range"))
        .alias("shot_zone")
    )

    return enhanced_pbp.with_columns(zone)


# ---------------------------------------------------------------------------
# Network fetcher (module-level so tests can monkeypatch it)
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str, league_id: str = "00") -> dict:
    """Fetch raw play-by-play v3 payload from stats.nba.com.

    Args:
        game_id: Ten-character NBA game identifier.
        league_id: League identifier (accepted for API symmetry; not forwarded
            to ``nba_stats_playbyplayv3`` which does not expose it).

    Returns:
        Raw ``dict`` from ``nba_stats_playbyplayv3``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

    return nba_stats_playbyplayv3(game_id=game_id, return_parsed=False)


# ---------------------------------------------------------------------------
# Public fetcher
# ---------------------------------------------------------------------------


def nba_shot_zones(
    game_id: str,
    league_id: str = "00",
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch enhanced play-by-play and classify every field-goal attempt by shot zone.

    Makes a single live network call to ``nba_stats_playbyplayv3``, then
    chains :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
    and :func:`add_shot_zones` to produce a tidy FG-only frame annotated with
    the pbpstats-core shot-zone classification.

    The module-level fetcher (:func:`_fetch_pbp`) is monkeypatchable for
    offline tests.

    Args:
        game_id: Ten-character NBA game identifier (e.g. ``"0022200001"``).
        league_id: League identifier (default ``"00"`` for NBA).  In Phase 2,
            ``playbyplayv3`` has no ``league_id`` parameter, so a non-``"00"``
            value does not change the pbp output.  Full WNBA/G-League support
            is a later phase.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of :class:`polars.DataFrame`.

    Returns:
        Polars (or pandas) DataFrame: the enhanced PBP filtered to field-goal
        attempts (``is_field_goal == 1``) with an appended ``shot_zone`` column.
        Empty or malformed payloads return a zero-row frame (never raises).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_zones import nba_shot_zones
            df = nba_shot_zones("0022200001")
            print(df[["shot_distance", "shot_zone"]].head())

        Pandas output::

            df_pd = nba_shot_zones("0022200001", return_as_pandas=True)
            print(type(df_pd))

        Zone frequency table::

            import polars as pl
            print(df.group_by("shot_zone").len().sort("len", descending=True))

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `hoopR`_ -- R package providing equivalent shot-chart utilities

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    payload = _fetch_pbp(game_id, league_id)
    enh = enhanced_pbp_from_payload(payload, league_id=league_id)
    df = add_shot_zones(enh)

    # Return only field-goal rows (is_field_goal == 1)
    if df.height > 0 and "is_field_goal" in df.columns:
        df = df.filter(pl.col("is_field_goal") == 1)

    if return_as_pandas:
        return df.to_pandas()
    return df
