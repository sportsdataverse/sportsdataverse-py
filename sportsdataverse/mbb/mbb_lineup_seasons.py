"""College lineup seasons (BB-D4): ``mbb.lineup_seasons`` / ``wbb.lineup_seasons``.

Builds :func:`sportsdataverse._common.lineup_seasons.lineup_seasons` from the two stats.ncaa.org
releases the college game impact box reads (:mod:`sportsdataverse.mbb.mbb_game_impact`):
possessions, minutes and points are the season sum of ``mbb_game_lineups_used``, and the
four-factor and shot-zone counts come from the ``ncaa_{mbb,wbb}_pbp`` events, each on the lineup
on the floor at the event (a free throw on the lineup at the last foul, as its points).

The counts follow the bigballR ``get_lineups`` rules (:mod:`sportsdataverse.mbb.mbb_ncaa_lineups`),
the rules behind ``ncaa_{mbb,wbb}_team_box``: a rim attempt is a dunk, layup, hook or tip-in
(:data:`~sportsdataverse.mbb.mbb_ncaa_lineups.RIM_TYPES_FIXED`), a mid-range attempt any other two.
Summed per team and game they equal that team box on every count (every 2024-25 D1 men's
team-game). They are counted from the play-by-play rather than read from the
``ncaa_{mbb,wbb}_lineups`` stints: those name players (no NCAA player id to join on), have no 2026
rows, and come from the cbb-explorer port, whose rim excludes tip-in layups and hooks outside the
paint.

``team_id`` is the ESPN team id (Int64) and the player ids are stats.ncaa.org ids (String), as in
the game impact box. A non-D1 opponent has no ESPN team id, so its units get no row.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.rate_gates import NCAA_LEVEL
from sportsdataverse._common.game_impact import credit_free_throws_to_foul_lineup
from sportsdataverse._common.lineup_seasons import COUNTS, ZONE_COUNTS, lineup_count_schema, lineup_seasons
from sportsdataverse.mbb.mbb_game_impact import _check_league, _load_releases, _slots, _teams, ncaa_impact_rows
from sportsdataverse.mbb.mbb_ncaa_lineups import RIM_TYPES_FIXED

__all__ = ["mbb_lineup_seasons", "ncaa_lineup_count_rows"]

_PLAYER = pl.Utf8()
_COUNT_ROWS = lineup_count_schema(_PLAYER, zones=True)
#: The default rank gate, offensive plus defensive possessions (about 100 each way): a D1 team
#: plays some 2,100 possessions a side, and 200 keeps each season's main units in the cohort.
MIN_POSS = 200


def _event_counts() -> dict[str, pl.Expr]:
    """Each count as a 0/1 expression on one pbp event (the bigballR ``get_lineups`` rules)."""
    made = pl.col("event_result") == "made"
    shot = pl.col("shot_value")
    rim = pl.col("event_type").is_in(list(RIM_TYPES_FIXED))
    flags = {
        "fga": shot.is_in([2, 3]),
        "fgm": shot.is_in([2, 3]) & made,
        "fg3a": shot == 3,
        "fg3m": (shot == 3) & made,
        "fta": shot == 1,
        "oreb": pl.col("event_type") == "Offensive Rebound",
        "dreb": pl.col("event_type") == "Defensive Rebound",
        "tov": pl.col("event_type") == "Turnover",
        "rima": rim,
        "rimm": rim & made,
        "mida": (shot == 2) & ~rim,
        "midm": (shot == 2) & ~rim & made,
    }
    return {c: flags[c].fill_null(False).cast(pl.Int64) for c in (*COUNTS, *ZONE_COUNTS)}


def ncaa_lineup_count_rows(pbp: pl.DataFrame) -> pl.DataFrame:
    """Map the stats.ncaa.org pbp onto the lineup-seasons count frame.

    Each shot, free throw, rebound and turnover counts for the side whose ``event_team`` it is,
    on the lineup at the event; a free throw on the lineup at the most recent foul
    (:func:`sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`). Events with
    no count are dropped.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release (game) order.

    Returns:
        One row per counted event in
        :func:`sportsdataverse._common.lineup_seasons.lineup_count_schema` (with the shot zones)
        and String player ids.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp
            from sportsdataverse.mbb.mbb_lineup_seasons import ncaa_lineup_count_rows
            rows = ncaa_lineup_count_rows(load_ncaa_mbb_pbp(2025))
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=_COUNT_ROWS)
    counts = _event_counts()
    side = {s: pl.col("event_team") == pl.col(s) for s in ("home", "away")}
    # No sort: the release is in game order, and the foul fill runs ``over`` the game, which keeps
    # each game's rows in that order.
    events = pbp.select(
        *_teams(),
        *_slots(),
        *[pl.when(side[s]).then(e).otherwise(0).alias(f"{s}_{c}") for s in side for c, e in counts.items()],
        "event_type",
        "foul_class",
        # The foul-lineup helper moves points and splits clocked rows; with neither, it only moves
        # the slots of each free throw.
        pl.lit(0.0).alias("seconds"),
        pl.lit(0).alias("home_pts"),
        pl.lit(0).alias("away_pts"),
    )
    events = credit_free_throws_to_foul_lineup(
        events, is_free_throw=pl.col("event_type") == "Free Throw", is_foul=pl.col("foul_class").is_not_null()
    )
    counted = pl.any_horizontal([pl.col(f"{s}_{c}") > 0 for s in side for c in counts])
    return events.filter(counted).select(list(_COUNT_ROWS)).cast(_COUNT_ROWS)


def mbb_lineup_seasons(
    seasons: int | list[int],
    *,
    league: str = "mens",
    min_poss: int = MIN_POSS,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """College lineup seasons: 2-, 3- and 5-man units with ratings, four factors and shot zones.

    The definitions are in :mod:`sportsdataverse._common.lineup_seasons`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2025`` is 2024-25).
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_lineup_seasons.wbb_lineup_seasons`).
        min_poss: The rank gate, offensive plus defensive possessions (default 200).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, n_players, lineup_id)``: ``n_players`` (2, 3 or 5),
        ``lineup_id`` (the NCAA player ids sorted and joined with ``-``), ``player_1..5``,
        ``minutes``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``, ``off_rtg`` /
        ``def_rtg`` / ``net_rtg`` (per 100 possessions), the four factors for and against
        (``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate``, ``opp_*``), the shot zones for and
        against (``rim_rate``, ``mid_rate``, ``fg3_rate`` shares of field-goal attempts and
        ``rim_pct``, ``mid_pct``, ``fg3_pct`` make rates, ``opp_*``), and for every rate
        ``<m>_rank`` / ``<m>_pct`` (within the season and unit size, among units with at least
        ``min_poss``) and ``<m>_n``. ``team_id`` is the ESPN team id (Int64). Empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_lineup_seasons import mbb_lineup_seasons
            units = mbb_lineup_seasons(2025)
            units.filter(n_players=2).sort("net_rtg_rank").head()
    """
    _check_league(league)
    pbp, possessions = _load_releases(seasons, league)
    counts = ncaa_lineup_count_rows(pbp)
    rows = ncaa_impact_rows(pbp, possessions, league=league)
    del pbp, possessions  # the pbp read is most of the peak memory; free it before the roll-up
    out = lineup_seasons(rows, counts, player_dtype=_PLAYER, min_poss=min_poss, zones=True, level=NCAA_LEVEL[league])
    return out.to_pandas() if return_as_pandas else out
