"""NBA / WNBA lineup seasons (BB-D4): ``nba.lineup_seasons``.

Builds :func:`sportsdataverse._common.lineup_seasons.lineup_seasons` from the four stats.nba.com
releases the game impact box reads (:mod:`sportsdataverse.nba.nba_game_impact`): possessions,
minutes and points are the season sum of ``nba_game_lineups_used``, and the four-factor counts come
from the ``nba_stats_possessions`` rows (each possession's shots, free throws, rebounds and
turnovers on the lineup that started it). ``league_id="10"`` reads the WNBA twins;
:mod:`sportsdataverse.wnba.wnba_lineup_seasons` binds it.

Checked against stats.nba.com's five-man ``lineups`` dashboard (Base and Advanced, Regular Season,
2024-25), on the 113 units over 200 of its possessions: the points scored are equal on 57% of them
(median difference 0), the minutes agree at the median, the net rating differs by a median of 1.5
(64.6% within 2.0, max 10.8) and the eFG% by under 0.05. stats.nba.com counts a possession for
every lineup on the floor during it, so its possession counts run about 4% above these (each
possession here belongs to the lineup that started it, which keeps the lineups adding up to the
team), and ``off_rtg`` / ``def_rtg`` each run about 5 above its ratings; in ``net_rtg`` the two
offsets largely cancel (the median gap of 1.5 above). ``lineup_id`` sorts the stats.nba.com ids numerically.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.rate_gates import NBA_LEVEL
from sportsdataverse._common.game_impact import AWAY_SLOTS, HOME_SLOTS
from sportsdataverse._common.lineup_seasons import COUNTS, lineup_count_schema, lineup_seasons
from sportsdataverse.nba.nba_game_impact import _check_league, _load_releases, _possession_rows, nba_impact_rows

__all__ = ["nba_lineup_count_rows", "nba_lineup_seasons"]

_PLAYER = pl.Int64()
_COUNT_ROWS = lineup_count_schema(_PLAYER)
#: The schedule ``season_type`` values a season table is built for.
_SEASON_TYPES = ("regular-season", "preseason", "playoffs", "play-in")
#: league_id -> the default rank gate (offensive plus defensive possessions): 400 is about 200
#: each way, the roadmap's population for the stats.nba.com check; the WNBA's 44-game season
#: halves it.
_MIN_POSS = {"00": 400, "10": 200}


def nba_lineup_count_rows(possessions: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    """Map ``nba_stats_possessions`` rows onto the lineup-seasons count frame.

    Each possession's counts go to the side that had the ball, its ``dreb`` (the defense's
    defensive rebounds) to the other side, on the possession's lineup. A possession whose offense
    is neither team of its ``schedule`` game is dropped, as in the game impact box.

    Args:
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows
            (``fg2a``, ``fg2m``, ``fg3a``, ``fg3m``, ``fta``, ``oreb``, ``dreb``, ``tov``).
        schedule: The games to keep (``game_id``, ``home_team_id``, ``away_team_id``).

    Returns:
        One row per possession in :func:`sportsdataverse._common.lineup_seasons.lineup_count_schema`
        with Int64 player ids.

    Raises:
        ValueError: If a count column holds a null in a possession of a ``schedule`` game.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
            rows = nba_lineup_count_rows(L.load_nba_stats_possessions(2024), L.load_nba_stats_schedules(2024))
    """
    if possessions.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_COUNT_ROWS)
    games = schedule.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64)
    ).unique("game_id")
    sources = ("fg2a", "fg2m", "fg3a", "fg3m", "fta", "oreb", "dreb", "tov")
    # Only the schedule's games: a null in a possession the build never reads must not stop it.
    possessions = possessions.filter(pl.col("game_id").cast(pl.Utf8).is_in(games["game_id"].implode()))
    nulls = [c for c in sources if possessions[c].null_count()]
    if nulls:  # a missing count would zero a known one (fga = fg2a + fg3a) and skew every rate over it
        raise ValueError(f"possessions carry null counts in {nulls}; the four factors need every count")
    poss = possessions.with_columns(
        (pl.col("fg2a") + pl.col("fg3a")).alias("fga"), (pl.col("fg2m") + pl.col("fg3m")).alias("fgm")
    )
    rows = _possession_rows(poss, games, keep=COUNTS)
    on_ball = {"home": pl.col("home_poss") == 1, "away": pl.col("away_poss") == 1}
    # Every count is the offense's except dreb, which the defense grabbed.
    owner = {side: {c: on_ball[other] if c == "dreb" else on_ball[side] for c in COUNTS} for side, other in
             (("home", "away"), ("away", "home"))}  # fmt: skip
    keys = ("season", "game_id", "home_team_id", "away_team_id", *HOME_SLOTS, *AWAY_SLOTS)
    return rows.select(
        *keys,
        *[
            pl.when(owner[side][c]).then(pl.col(c)).otherwise(0).fill_null(0).alias(f"{side}_{c}")
            for side in ("home", "away")
            for c in COUNTS
        ],
    ).cast(_COUNT_ROWS)


def nba_lineup_seasons(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    min_poss: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Lineup seasons: 2-, 3- and 5-man units with possessions, minutes, ratings and four factors.

    The definitions are in :mod:`sportsdataverse._common.lineup_seasons`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        min_poss: The rank gate, offensive plus defensive possessions. Defaults to 400 (NBA) or
            200 (WNBA).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, n_players, lineup_id)``: ``n_players`` (2, 3 or 5),
        ``lineup_id`` (the player ids sorted and joined with ``-``), ``player_1..5``, ``minutes``,
        ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``, ``off_rtg`` / ``def_rtg`` /
        ``net_rtg`` (per 100 possessions), the four factors for (``efg_pct``, ``tov_pct``,
        ``orb_pct``, ``ft_rate``) and against (``opp_*``) as fractions, and for every rate
        ``<m>_rank`` / ``<m>_pct`` (within the season and unit size, among units with at least
        ``min_poss``) and ``<m>_n``. Ids are the stats.nba.com ids (Int64). Empty input returns
        that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, ``season_type`` is not one
            of the four above, or the possessions carry a null shot, free-throw, rebound or
            turnover count.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_seasons
            units = nba_lineup_seasons(2025)
            units.filter(n_players=5).sort("net_rtg_rank").head()
    """
    _check_league(league_id)
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    if schedule.height:
        schedule = schedule.filter(pl.col("season_type") == season_type)
    out = lineup_seasons(
        nba_impact_rows(pbp, game_lineups, possessions, schedule, league_id=league_id),
        nba_lineup_count_rows(possessions, schedule),
        player_dtype=_PLAYER,
        min_poss=_MIN_POSS[league_id] if min_poss is None else min_poss,
        level=NBA_LEVEL[league_id],
    )
    return out.to_pandas() if return_as_pandas else out
