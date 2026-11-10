"""College season shot profile by zone (BB-D35): ``mbb.player_shot_zones`` / ``team_shot_zones``.

Rim / mid / corner 3 / above-the-break 3 attempts, makes, rates and assisted share per player and
per team season, the team's for and allowed, with BB-D2's ``_rank`` / ``_pct`` / ``_n``. The
counting and ranking are in :mod:`sportsdataverse._common.shot_zone_profiles`; this module maps the
stats.ncaa.org shot charts (``ncaa_{mbb,wbb}_shots``) onto its shot rows, every game of the season.
``league="womens"`` reads the women's releases; :mod:`sportsdataverse.wbb.wbb_shot_zone_profiles`
binds it.

* **Zones** are the shot chart's own ``shot_zone``
  (:func:`sportsdataverse.mbb.mbb_shots_adapter.classify_zone_geometry`): ``rim`` within 4 ft,
  ``paint`` and ``mid`` both ``mid`` here, ``corner3`` a three with ``|x| >= 21`` ft and
  ``|y| <= 9`` ft (the baseline band), ``abovebreak3`` every other three.
* **Ids.** ``team_id`` is the ESPN team id (Int64), ``player_id`` the stats.ncaa.org player id
  (text), as in BB-D6's ``game_player_impact``. A non-D1 team has no ESPN id: its attempts count
  against its D1 opponent, never as a team or player row.
* **Assisted.** The shot chart carries no assist and no clock, but it lists a contest's attempts in
  play-by-play order: the k-th chart attempt of a contest is linked to the k-th field-goal row of
  ``ncaa_{mbb,wbb}_pbp`` when both agree on made / missed, the point value and the shooter (2024-25
  men: 99.6% of attempts). A linked make is assisted when that row names an ``assist_player``; an
  unlinked make's assist is unknown and stays out of ``{z}_ast_pct`` and its ``_n``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.shot_zone_profiles import player_shot_zones, shot_rows_schema, team_shot_zones

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["mbb_player_shot_zones", "mbb_team_shot_zones"]

_LEVEL = {"mens": "mbb", "womens": "wbb"}
_ZONE = {"rim": "rim", "paint": "mid", "mid": "mid", "corner3": "corner_3", "abovebreak3": "above_break_3"}
SCHEMA = shot_rows_schema(pl.Utf8())


def _check(league: str) -> None:
    if league not in _LEVEL:
        raise ValueError(f"league must be one of {sorted(_LEVEL)}, got {league!r}")


def ncaa_shot_zone_rows(shots: pl.DataFrame, pbp: pl.DataFrame) -> pl.DataFrame:
    """Map ``load_ncaa_{mbb,wbb}_shots`` rows, with the play-by-play's assists, onto the shot rows.

    Args:
        shots: The shot-chart release rows (``season``, ``contest_id``, ``team_id`` (the NCAA team
            name), ``espn_team_id``, ``shooter_player_id``, ``shooter_clean_name``, ``shot_zone``,
            ``made``, ``point_value``), in the release's order.
        pbp: ``load_ncaa_{mbb,wbb}_pbp`` rows (``contest_id``, ``shot_value``, ``event_result``,
            ``player_1_id``, ``assist_player``), in the release's order. Pass an empty frame to
            leave every assist unknown.

    Returns:
        One row per field-goal attempt in
        :data:`~sportsdataverse._common.shot_zone_profiles.SHOT_ROWS_SCHEMA` with a text
        ``player_id``: ``team_id`` / ``opp_team_id`` the ESPN ids of the shooting side and the
        other side of the contest (null for a non-D1 side), ``assisted`` null on a miss and on an
        unlinked make.
    """
    s = shots.filter(pl.col("point_value").is_in([2, 3]))
    if s.height == 0:
        return pl.DataFrame(schema=SCHEMA)
    s = s.with_columns(pl.int_range(pl.len()).over("contest_id").alias("_k"))
    if pbp.height:
        fg = (
            pbp.filter(pl.col("shot_value").is_in([2, 3]))
            .with_columns(pl.int_range(pl.len()).over("contest_id").alias("_k"))
            .select(
                "contest_id",
                "_k",
                (pl.col("event_result") == "made").alias("_p_made"),
                pl.col("shot_value").alias("_p_value"),
                pl.col("player_1_id").alias("_p_shooter"),
                pl.col("assist_player").is_not_null().alias("_p_ast"),
            )
        )
        s = s.join(fg, on=["contest_id", "_k"], how="left")
        shooter_ok = (
            pl.col("shooter_player_id").is_null()
            | pl.col("_p_shooter").is_null()
            | (pl.col("shooter_player_id") == pl.col("_p_shooter"))
        )
        linked = (pl.col("made") == pl.col("_p_made")) & (pl.col("point_value") == pl.col("_p_value")) & shooter_ok
        assisted = pl.when(linked.fill_null(False)).then(pl.col("_p_ast"))
    else:
        assisted = pl.lit(None, dtype=pl.Boolean)
    # the two sides of a contest are its two NCAA team names; the opponent is the other one's ESPN id
    sides = s.group_by("contest_id", "team_id").agg(pl.col("espn_team_id").drop_nulls().first().alias("_espn"))
    opp = (
        sides.join(sides, on="contest_id", suffix="_opp")
        .filter(pl.col("team_id") != pl.col("team_id_opp"))
        .group_by("contest_id", "team_id")
        .agg(pl.col("_espn_opp").min().alias("_opp"))
    )
    made = pl.col("made") == True  # noqa: E712
    return (
        s.join(opp, on=["contest_id", "team_id"], how="left")
        .select(
            pl.col("season").cast(pl.Int64),
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            pl.col("espn_team_id").cast(pl.Int64).alias("team_id"),
            pl.col("_opp").cast(pl.Int64).alias("opp_team_id"),
            pl.col("shooter_player_id").cast(pl.Utf8).alias("player_id"),
            pl.col("shooter_clean_name").cast(pl.Utf8).alias("player_name"),
            pl.col("shot_zone").replace_strict(_ZONE, default=None).alias("zone"),
            made.alias("made"),
            pl.when(made).then(assisted).alias("assisted"),
        )
        .cast(SCHEMA)
    )


def _rows(seasons: int | list[int], league: str) -> pl.DataFrame:
    _check(league)
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_pbp as load_pbp
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_shots as load_shots
    else:
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp as load_pbp
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_shots as load_shots
    return ncaa_shot_zone_rows(load_shots(seasons_list), load_pbp(seasons_list))


def mbb_player_shot_zones(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

    One row per player, team and season over every game of the season. Zones, ids and the assist
    link are as in the module docstring.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, player_id)`` (ESPN team id Int64, stats.ncaa.org player id
        text) with ``player_name``, ``games``, ``fga``, ``fgm``, ``unlocated_fga`` and, for each
        zone ``z`` (``rim``, ``mid``, ``corner_3``, ``above_break_3``), ``{z}_fga``, ``{z}_fgm``,
        ``{z}_ast`` (assisted makes), ``{z}_fg_pct``, ``{z}_freq`` (share of the located attempts)
        and ``{z}_ast_pct`` (share of the linked makes assisted). The four ``{z}_fga`` plus
        ``unlocated_fga`` add up to ``fga``. Every rate has ``<m>_n``, ``<m>_rank`` (1 = highest)
        and ``<m>_pct``, ranked within the season among players whose ``<m>_n`` reaches the rate's
        gate and null under ten players. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shot_zone_profiles import mbb_player_shot_zones
            zones = mbb_player_shot_zones(2025)
            zones.sort("rim_fga", descending=True).select("player_name", "rim_fga", "rim_fg_pct").head()
    """
    out = player_shot_zones(_rows(seasons, league), level=_LEVEL[league])
    return out.to_pandas() if return_as_pandas else out


def mbb_team_shot_zones(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College season shot profile by zone per team, for and allowed: rim / mid / corner 3 / above-the-break 3.

    One row per team and season over every game: the team's own attempts and, with an ``opp_``
    prefix, its opponents' attempts against it (a non-D1 opponent's attempts included).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id)`` (the ESPN team id, Int64) with ``games`` and the columns
        of :func:`mbb_player_shot_zones` for the team and again with an ``opp_`` prefix for its
        opponents, each rate with ``<m>_n``, ``<m>_rank`` and ``<m>_pct``, ranked within the season
        among teams whose ``<m>_n`` reaches the gate and null under five teams. ``<m>_rank`` is
        1 = highest, except the allowed ``opp_{z}_fg_pct``, where 1 is the lowest. Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shot_zone_profiles import mbb_team_shot_zones
            teams = mbb_team_shot_zones(2025)
            teams.sort("opp_corner_3_freq").select("team_id", "opp_corner_3_freq").head()
    """
    out = team_shot_zones(_rows(seasons, league), level=_LEVEL[league])
    return out.to_pandas() if return_as_pandas else out
