"""NBA / WNBA season shot profile by zone (BB-D35): ``nba_stats.player_shot_zones`` / ``team_shot_zones``.

Rim / mid / corner 3 / above-the-break 3 attempts, makes, rates and assisted share per player and
per team season, the team's for and allowed, with BB-D2's ``_rank`` / ``_pct`` / ``_n``. The
counting and ranking are in :mod:`sportsdataverse._common.shot_zone_profiles`; this module maps the
``nba_stats_shots`` / ``wnba_stats_shots`` releases (stats.nba.com / stats.wnba.com play-by-play
field-goal rows, regular season only) onto its shot rows. ``league_id="10"`` reads the WNBA
release; :mod:`sportsdataverse.wnba.wnba_shot_zone_profiles` binds it.

* **Zones** come from :func:`sportsdataverse.nba.nba_shot_zones.add_shot_zones` (the restricted
  area is ``shot_distance < 4``; a corner three is a three with ``y_legacy <= 87.5``, the band below
  the NBA break, and for the NBA also ``|x_legacy| >= 220``, which every such three already
  reaches). The WNBA's corner sat nearer than 22 ft until 2013 (19 ft 9 in, then 20 ft 6.25 in),
  so the WNBA keeps the band test alone.
* **Unlocated.** WNBA seasons through 2010 park a share of their attempts at exactly
  ``(0, 0)`` (no location recorded; 5,022 of 27,095 regular-season attempts in 2010, none from 2011): they count
  in ``fga`` and ``unlocated_fga`` and in no zone.
* **Assisted.** A make is assisted when its description credits an assist (``"(Green 5 AST)"``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.shot_zone_profiles import SHOT_ROWS_SCHEMA, player_shot_zones, team_shot_zones
from sportsdataverse.nba.nba_shot_zones import add_shot_zones

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_player_shot_zones", "nba_team_shot_zones"]

_LEVEL = {"00": "nba", "10": "wnba"}
#: Regular-season game ids: ``002...`` NBA, ``102...`` WNBA.
_REGULAR = {"00": "002", "10": "102"}
#: The last WNBA season with hoop-centre placeholder attempts.
WNBA_PLACEHOLDER_THROUGH = 2010
#: The WNBA keeps the corner band test alone (see the module docstring).
_CORNER_MIN_ABS_X = {"00": 220.0, "10": 0.0}
_ZONE = {
    "restricted_area": "rim",
    "in_the_paint_non_ra": "mid",
    "mid_range": "mid",
    "corner_3": "corner_3",
    "above_the_break_3": "above_break_3",
}


def _check(league_id: str) -> None:
    """Raise ValueError for an unknown ``league_id`` (before any data is loaded)."""
    if league_id not in _LEVEL:
        raise ValueError(f"league_id must be one of {sorted(_LEVEL)}, got {league_id!r}")


def nba_shot_zone_rows(shots: pl.DataFrame, *, league_id: str = "00") -> pl.DataFrame:
    """Map ``load_nba_stats_shots`` / ``load_wnba_stats_shots`` rows onto the zone-profile shot rows.

    Args:
        shots: The shots release rows (``game_id``, ``season``, ``team_id``, ``person_id``,
            ``player_name``, ``shot_result``, ``shot_value``, ``shot_distance``, ``x_legacy``,
            ``y_legacy``, ``description``).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.

    Returns:
        One row per regular-season field-goal attempt in
        :data:`~sportsdataverse._common.shot_zone_profiles.SHOT_ROWS_SCHEMA`: ``opp_team_id`` is the
        other team that shot in the game, ``zone`` null for an unlocated attempt, ``assisted``
        null on a miss.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.
    """
    _check(league_id)
    s = shots.filter(pl.col("game_id").str.starts_with(_REGULAR[league_id]) & pl.col("shot_value").is_in([2, 3]))
    if s.height == 0:
        return pl.DataFrame(schema=SHOT_ROWS_SCHEMA)
    x, y = pl.col("x_legacy"), pl.col("y_legacy")
    located = x.is_not_null() & y.is_not_null() & pl.col("shot_distance").is_not_null()
    if league_id == "10":
        located = located & ~((pl.col("season") <= WNBA_PLACEHOLDER_THROUGH) & (x == 0) & (y == 0))
    # a feed glitch can carry team_id 0 (one WNBA 2025 attempt): no team, so no side of the game
    s = s.with_columns(pl.when(pl.col("team_id") != 0).then(pl.col("team_id")).alias("team_id"))
    zoned = add_shot_zones(
        s.with_columns(pl.lit(1).alias("is_field_goal")), corner_min_abs_x=_CORNER_MIN_ABS_X[league_id]
    )
    sides = s.select("game_id", "team_id").drop_nulls().unique()
    opp = (
        sides.join(sides, on="game_id", suffix="_opp")
        .filter(pl.col("team_id") != pl.col("team_id_opp"))
        # a game where a third team id shot (never seen) would fan out: keep one opponent
        .group_by("game_id", "team_id")
        .agg(pl.col("team_id_opp").min().alias("opp_team_id"))
    )
    made = pl.col("shot_result") == "Made"
    return (
        zoned.join(opp, on=["game_id", "team_id"], how="left")
        .select(
            pl.col("season").cast(pl.Int64),
            pl.col("game_id").cast(pl.Utf8),
            pl.col("team_id").cast(pl.Int64),
            pl.col("opp_team_id").cast(pl.Int64),
            pl.col("person_id").cast(pl.Int64).alias("player_id"),
            pl.col("player_name").cast(pl.Utf8),
            pl.when(located).then(pl.col("shot_zone").replace_strict(_ZONE, default=None)).alias("zone"),
            made.alias("made"),
            pl.when(made).then(pl.col("description").str.contains(r"\d+ AST\)").fill_null(False)).alias("assisted"),
        )
        .cast(SHOT_ROWS_SCHEMA)
    )


def _rows(seasons: int | list[int], league_id: str) -> pl.DataFrame:
    """The shot rows of ``seasons`` (END years) from the league's shots release."""
    _check(league_id)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_shots

        shots = load_wnba_stats_shots(seasons_list)
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_stats_shots

        shots = load_nba_stats_shots([s - 1 for s in seasons_list])  # the NBA loader takes the START year
    return nba_shot_zone_rows(shots, league_id=league_id)


@overload
def nba_player_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_player_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_player_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

    One row per player, team and season over the regular season (a traded player has a row per
    team). Zones and the unlocated attempts are as in the module docstring.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)`` (stats.nba.com ids, Int64) with
        ``player_name``, ``games``, ``fga``, ``fgm``, ``unlocated_fga`` and, for each zone ``z``
        (``rim``, ``mid``, ``corner_3``, ``above_break_3``), ``{z}_fga``, ``{z}_fgm``, ``{z}_ast``
        (assisted makes), ``{z}_fg_pct``, ``{z}_freq`` (share of the located attempts) and
        ``{z}_ast_pct`` (share of the makes assisted). The four ``{z}_fga`` plus ``unlocated_fga``
        add up to ``fga``. Every rate has ``<m>_n`` (its denominator), ``<m>_rank`` (1 = highest)
        and ``<m>_pct`` (the Weibull percentile), ranked within the season among players whose
        ``<m>_n`` reaches the rate's gate and null under ten players. Empty input returns that
        schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_zone_profiles import nba_player_shot_zones
            zones = nba_player_shot_zones(2025)
            zones.sort("corner_3_fga", descending=True).select("player_name", "corner_3_fga", "corner_3_fg_pct").head()

        WNBA via ``league_id``::

            wnba = nba_player_shot_zones(2025, league_id="10")
    """
    out = player_shot_zones(_rows(seasons, league_id), level=_LEVEL[league_id])
    return out.to_pandas() if return_as_pandas else out


@overload
def nba_team_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_team_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_team_shot_zones(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Season shot profile by zone per team, for and allowed: rim / mid / corner 3 / above-the-break 3.

    One row per team and season over the regular season: the team's own attempts and, with an
    ``opp_`` prefix, its opponents' attempts against it.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)`` (the stats.nba.com team id, Int64) with ``games`` and the
        columns of :func:`nba_player_shot_zones` for the team and again with an ``opp_`` prefix for
        its opponents (``opp_fga``, ``opp_rim_fg_pct``, ...), each rate with ``<m>_n``,
        ``<m>_rank`` and ``<m>_pct``, ranked within the season among teams whose ``<m>_n`` reaches
        the gate and null under five teams. ``<m>_rank`` is 1 = highest, except the allowed
        ``opp_{z}_fg_pct``, where 1 is the lowest (the stingiest defense). Empty input returns that
        schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_zone_profiles import nba_team_shot_zones
            teams = nba_team_shot_zones(2025)
            teams.sort("opp_rim_freq").select("team_id", "opp_rim_freq", "opp_rim_fg_pct").head()

        WNBA via ``league_id``::

            wnba = nba_team_shot_zones(2025, league_id="10")
    """
    out = team_shot_zones(_rows(seasons, league_id), level=_LEVEL[league_id])
    return out.to_pandas() if return_as_pandas else out
