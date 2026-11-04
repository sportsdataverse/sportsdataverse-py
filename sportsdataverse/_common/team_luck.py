"""Luck and close-game record (BB-D22): the shared core behind ``{lg}_team_luck``.

One row per ``(season, team_id)``:

* **Expected wins** -- the Pythagorean expectation (Bill James's, carried to basketball by Dean
  Oliver and Daryl Morey) on the season's final scores:
  ``pythag_win_pct = pts^x / (pts^x + opp_pts^x)`` and ``exp_wins = pythag_win_pct * games``.
  ``luck_wins`` is ``wins - exp_wins``: positive means the team won more than its scoring margin
  says it should have. The exponent ``x`` is :data:`PYTHAG_EXPONENT`, fitted per league.
* **Close-game record** -- ``close_games`` / ``close_wins`` / ``close_losses``: the games that
  were close by BB-D3's definition (:mod:`sportsdataverse._common.team_season_rates`), i.e. with
  at least one possession that started with 5:00 or less left in the last regulation period or
  in an overtime, the score within 5. That is stats.nba.com's clutch window
  (``ClutchTime="Last 5 Minutes"``, ``PointDiff=5``). A game is only classed when its
  possessions are published; ``pbp_games`` counts the team's games that are, so
  ``close_games`` covers ``pbp_games`` of ``games``.

Points, games and wins come from the final scores (every completed game), not from the
possessions: the college possessions do not cover every game, and the Pythagorean exponents were
fitted on final scores.

Ranks, percentiles and ``_n`` (BB-D2, :mod:`sportsdataverse._common.league_context`) for
``pythag_win_pct``, ``luck_wins`` and ``close_win_pct`` are within the season, among teams that
played at least half the most games any team played, with a floor of
:data:`~sportsdataverse._common.league_context.MIN_COHORT_TEAMS` teams. ``close_win_pct`` is
ranked only for a team with possessions for at least half its games and at least
:data:`MIN_CLOSE_GAMES` close games. Rank 1 is the highest value (the luckiest team for
``luck_wins``).

Scale: ``luck_wins`` is mostly noise. On the fit's holdout seasons its spread (RMSE) is 2.98
wins a season in the NBA, 1.68 WNBA, 2.03 men's college, 1.88 women's; a rank orders teams
within that noise.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import logging

import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_ranks, attach_sample_sizes
from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA, _with_clutch

__all__ = [
    "CLOSE_SCHEMA",
    "GAME_RESULTS_SCHEMA",
    "MIN_CLOSE_GAMES",
    "PYTHAG_EXPONENT",
    "QUALIFIER",
    "TEAM_LUCK_SCHEMA",
    "close_game_flags",
    "pythag_win_pct",
    "season_records",
    "team_luck",
]

logger = logging.getLogger(__name__)

#: league -> the Pythagorean exponent on season points for and against. Fitted by
#: ``tools/fit_team_luck_pythag.py`` (2026-11-04): the 0.01-grid value with the least squared
#: error in wins over the qualified team-seasons of the TRAIN span, scored on a later HOLDOUT
#: span by mean absolute error in wins (records in ``tests/fixtures/bb_team_luck/``).
#:
#: ====== ======= =========== ========================= ============================================
#: league x       train       holdout MAE (team-seasons) published exponents on the same holdout
#: ====== ======= =========== ========================= ============================================
#: nba    14.02   1997-2016   2.360 (300), 2017-2026    Morey 13.91: 2.356, Oliver 14: 2.359,
#:                                                      Hollinger 16.5: 2.791
#: wnba   10.64   1997-2015   1.363 (121), 2016-2025    Morey 13.91: 1.580, Hollinger 16.5: 2.205
#: mbb    8.07    2002-2017   1.629 (3197), 2018-2026   KenPom 10.25: 1.853, 11.5: 2.126
#: wbb    6.80    2002-2017   1.499 (3151), 2018-2026   KenPom 10.25: 2.339, 11.5: 2.762
#: ====== ======= =========== ========================= ============================================
#:
#: The NBA fit lands on the published value (Oliver's 14 in Basketball on Paper, Morey's 13.91):
#: paired by holdout season it is within noise of both (+0.004 wins, sd 0.009, vs Morey). The
#: WNBA fit beats 13.91 by 0.21 wins (sd 0.39, 6 of 10 seasons); the college fits beat KenPom's
#: 10.25 in 9 of 9 seasons (men's by 0.22, sd 0.03; women's by 0.83, sd 0.20). The college exponents sit under KenPom's, which are fitted on schedule-adjusted efficiencies
#: rather than raw final scores.
PYTHAG_EXPONENT: dict[str, float] = {"nba": 14.02, "wnba": 10.64, "mbb": 8.07, "wbb": 6.80}

#: Completed games, one row per (game, team): the team's points and its opponent's.
GAME_RESULTS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "team_id": pl.Int64(),
    "opp_team_id": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
}

#: Close-game flags, one row per game with published possessions.
CLOSE_SCHEMA: dict[str, pl.DataType] = {"game_id": pl.Utf8(), "close": pl.Boolean()}

_RECORD_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "games": pl.Int64(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
    "pts": pl.Int64(),
    "opp_pts": pl.Int64(),
}

#: Rated metric -> its sample size.
_RATED: dict[str, pl.Expr] = {
    "pythag_win_pct": pl.col("games"),
    "luck_wins": pl.col("games"),
    "close_win_pct": pl.col("close_games"),
}
#: The fewest close games a ``close_win_pct`` is ranked on.
MIN_CLOSE_GAMES = 5
QUALIFIER = pl.col("games") >= 0.5 * pl.col("games").max().over("season")
#: ``close_win_pct`` also needs possessions for at least half the team's games and
#: :data:`MIN_CLOSE_GAMES` close games: a team whose games are mostly unclassed, or with two or
#: three close games, would otherwise top the ranking on a coin flip.
# ponytail: a fixed floor; a share of the season's close-game median if college ranks look thin.
_CLOSE_QUALIFIER = (
    QUALIFIER & (pl.col("pbp_games") >= 0.5 * pl.col("games")) & (pl.col("close_games") >= MIN_CLOSE_GAMES)
)

TEAM_LUCK_SCHEMA: dict[str, pl.DataType] = {
    **_RECORD_SCHEMA,
    "win_pct": pl.Float64(),
    "pythag_win_pct": pl.Float64(),
    "exp_wins": pl.Float64(),
    "luck_wins": pl.Float64(),
    "pbp_games": pl.Int64(),
    "close_games": pl.Int64(),
    "close_wins": pl.Int64(),
    "close_losses": pl.Int64(),
    "close_win_pct": pl.Float64(),
    **{f"{m}_rank": pl.Float64() for m in _RATED},
    **{f"{m}_pct": pl.Float64() for m in _RATED},
    **{f"{m}_n": pl.Int64() for m in _RATED},
}


def close_game_flags(rows: pl.DataFrame) -> pl.DataFrame:
    """Whether each game was close, from its possessions (BB-D3's ``close``).

    Args:
        rows: Possessions in :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

    Returns:
        One row per game in :data:`CLOSE_SCHEMA`.

    Raises:
        KeyError: If ``rows`` is missing a schema column.
    """
    missing = sorted(set(RATE_ROWS_SCHEMA) - set(rows.columns))
    if missing:
        raise KeyError(f"rows missing columns: {missing}")
    if rows.height == 0:
        return pl.DataFrame(schema=CLOSE_SCHEMA)
    flagged = _with_clutch(rows.select(list(RATE_ROWS_SCHEMA)).cast(RATE_ROWS_SCHEMA))
    return flagged.group_by("game_id").agg(pl.col("close").first()).sort("game_id").cast(CLOSE_SCHEMA)


_WON = pl.col("pts") > pl.col("opp_pts")
_RECORD_AGGS = (
    pl.len().cast(pl.Int64).alias("games"),
    _WON.sum().cast(pl.Int64).alias("wins"),
    (~_WON).sum().cast(pl.Int64).alias("losses"),
    pl.col("pts").sum(),
    pl.col("opp_pts").sum(),
)


def _clean_results(results: pl.DataFrame) -> pl.DataFrame:
    """``results`` in :data:`GAME_RESULTS_SCHEMA`, tied rows dropped (see :func:`season_records`)."""
    res = results.select(list(GAME_RESULTS_SCHEMA)).cast(GAME_RESULTS_SCHEMA)
    unknown = res.filter(pl.col("team_id").is_null() | pl.col("opp_team_id").is_null()).height
    if unknown:  # a null id would pool unrelated games into one phantom team
        logger.warning("dropped %d team-games with no team or opponent id", unknown)
        res = res.filter(pl.col("team_id").is_not_null() & pl.col("opp_team_id").is_not_null())
    tied = pl.col("pts") == pl.col("opp_pts")
    ties = res.filter(tied).height
    if ties:
        logger.warning("dropped %d tied team-games (unplayed, or a bad final score)", ties)
        res = res.filter(~tied)
    if res.select(pl.struct("game_id", "team_id").is_duplicated().any()).item():
        raise ValueError("results: a (game_id, team_id) appears twice")
    return res


def season_records(results: pl.DataFrame) -> pl.DataFrame:
    """Games, wins, losses and points for and against per (season, team).

    A row with no team or opponent id is dropped with a warning, and so is a tied row. A final
    score cannot be tied, so it is a game never
    played (the stats schedule lists the cancelled 2013-04-16 Celtics-Pacers game at 0-0; ESPN
    marks 33 college games 0-0 Final) or a bad score (two women's college games, 56-56 and 27-27).

    Args:
        results: Completed games in :data:`GAME_RESULTS_SCHEMA`.

    Returns:
        One row per (season, team), sorted, with ``games``, ``wins``, ``losses``, ``pts`` and
        ``opp_pts``.

    Raises:
        ValueError: If a (game, team) repeats.
    """
    return (
        _clean_results(results)
        .group_by("season", "team_id")
        .agg(*_RECORD_AGGS)
        .sort("season", "team_id")
        .cast(_RECORD_SCHEMA)
    )


def pythag_win_pct(exponent: float) -> pl.Expr:
    """The Pythagorean win fraction of ``pts`` / ``opp_pts`` at ``exponent``."""
    ratio = (pl.col("opp_pts") / pl.col("pts")) ** exponent  # pts^x / (pts^x + opp^x), overflow-safe
    return 1.0 / (1.0 + ratio)


def team_luck(results: pl.DataFrame, close: pl.DataFrame, *, exponent: float) -> pl.DataFrame:
    """Expected wins, luck and the close-game record per (season, team).

    Args:
        results: Completed games in :data:`GAME_RESULTS_SCHEMA`.
        close: :func:`close_game_flags` output; games missing from it are unclassed.
        exponent: The Pythagorean exponent (:data:`PYTHAG_EXPONENT`).

    Returns:
        One row per (season, team) in :data:`TEAM_LUCK_SCHEMA`, sorted by season and team.
        ``close_win_pct`` is null for a team with no close game. Empty input returns that
        schema with zero rows.

    Raises:
        ValueError: If a (game, team) repeats in ``results``.
        TypeError: If ``game_id`` is not text in ``results`` or ``close`` (an integer id cast to
            text can drop the leading zeros of a stats.nba.com id and match nothing).
    """
    for name, frame in (("results", results), ("close", close)):
        if "game_id" in frame.columns and frame.schema["game_id"] != pl.Utf8:  # a cast could mis-key ids
            raise TypeError(f"{name}.game_id must be Utf8, got {frame.schema['game_id']}")
    if results.height == 0:
        return pl.DataFrame(schema=TEAM_LUCK_SCHEMA)
    res = _clean_results(results)
    flags = close.select(list(CLOSE_SCHEMA)).cast(CLOSE_SCHEMA)
    is_close = pl.col("close") == True  # noqa: E712
    record = (
        res.join(flags, on="game_id", how="left", validate="m:1")
        .group_by("season", "team_id")
        .agg(
            *_RECORD_AGGS,
            pl.col("close").is_not_null().sum().cast(pl.Int64).alias("pbp_games"),
            is_close.sum().cast(pl.Int64).alias("close_games"),
            (is_close & _WON).sum().cast(pl.Int64).alias("close_wins"),
            (is_close & ~_WON).sum().cast(pl.Int64).alias("close_losses"),
        )
    )
    pythag = pythag_win_pct(exponent)
    out = record.with_columns(
        (pl.col("wins") / pl.col("games")).alias("win_pct"),
        pythag.alias("pythag_win_pct"),
        (pythag * pl.col("games")).alias("exp_wins"),
    ).with_columns(
        (pl.col("wins") - pl.col("exp_wins")).alias("luck_wins"),
        pl.when(pl.col("close_games") > 0)
        .then(pl.col("close_wins") / pl.col("close_games"))
        .otherwise(None)
        .alias("close_win_pct"),
    )
    return (
        out.pipe(
            attach_ranks,
            ["pythag_win_pct", "luck_wins"],
            min_cohort=MIN_COHORT_TEAMS,
            by=("season",),
            qualifier=QUALIFIER,
        )
        .pipe(attach_ranks, ["close_win_pct"], min_cohort=MIN_COHORT_TEAMS, by=("season",), qualifier=_CLOSE_QUALIFIER)
        .pipe(attach_sample_sizes, _RATED)
        .sort("season", "team_id")
        .select(list(TEAM_LUCK_SCHEMA))
        .cast(TEAM_LUCK_SCHEMA)
    )
