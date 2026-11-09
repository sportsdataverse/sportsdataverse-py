"""NBA / WNBA four-factor RAPM split (BB-D10): ``nba.player_factor_rapm``.

Builds :mod:`sportsdataverse._common.factor_rapm` from two stats.nba.com releases:
``nba_stats_possessions`` (the ten players on the floor and the possession's points, shots,
free-throw attempts, offensive rebounds and turnovers) and ``nba_stats_schedules`` (each game's season type).
``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_player_factor_rapm` binds it.

``o_rapm`` / ``d_rapm`` are :func:`sportsdataverse.nba.nba_rapm.nba_rapm` on the season's
possessions, the plain RAPM of ``nba.player_impact``, at the league's fixed ridge penalty
(:data:`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY`, NBA 2,683, WNBA 1,931, the same every season).
The ``player_impact`` releases published before November 2026 were fit at RidgeCV's per-season pick
instead (NBA 2024-25 5,179, 2025-26 1,931): at the fixed penalty the NBA o_rapm differs in scale from
them (2024-25 r 0.986, max difference 1.66; 2025-26 r 0.994, 1.24) until they are re-published. At the release's own
penalty (regular season): WNBA 2024 and 2025 equal it to 1e-11 on every player (157 and 181; their
pick was 1,931, the fixed WNBA value); NBA 2024-25 and 2025-26 correlate 0.997 (o_rapm) and 0.997
(d_rapm) on all 569 and 582 players, a max difference of 1.31. The cause is
lineups, not possessions: the possessions release (2026-08-13) predates the ``player_impact`` fit
(2026-10-12), which ran on a re-compile with the same 243,735 / 245,646 possessions but some on-floor
lineups changed (players' possessions differ by up to 184, net zero over the season). The live test
``test_nba_totals_track_the_published_release`` holds both numbers.
"""

from __future__ import annotations

import warnings

import pandas as pd
import polars as pl

from sportsdataverse._common.factor_rapm import COUNTS, factor_rapm
from sportsdataverse.nba.nba_game_impact import _check_league
from sportsdataverse.nba.nba_lineup_seasons import _MIN_POSS, _SEASON_TYPES

__all__ = ["nba_factor_rapm_rows", "nba_player_factor_rapm"]

_PLAYER = pl.Int64()
_SLOTS = [f"{side}_player_{i}" for side in ("off", "def") for i in range(1, 6)]


def nba_factor_rapm_rows(possessions: pl.DataFrame, schedule: pl.DataFrame, season_type: str) -> pl.DataFrame:
    """The possessions of ``schedule``'s ``season_type`` games, with the columns the split reads.

    Args:
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows.
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows (``game_id``,
            ``season_type``).
        season_type: The schedule ``season_type`` to keep.

    Returns:
        ``game_id``, ``season``, ``off_player_1..5``, ``def_player_1..5`` (Int64) and the counts, one row
        per possession. Warns (``UserWarning``) when possessions are given but none is in such a game.
    """
    cols = ["season", *_SLOTS, *COUNTS]
    if possessions.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema={"game_id": pl.Utf8(), **{c: pl.Int64() for c in cols}})
    games = schedule.filter(pl.col("season_type") == season_type).select(pl.col("game_id").cast(pl.Utf8)).unique()
    poss = possessions.with_columns(pl.col("game_id").cast(pl.Utf8))
    assert poss.schema["game_id"] == games.schema["game_id"], "game_id dtypes differ between possessions and schedule"
    out = poss.join(games, on="game_id", how="semi").select("game_id", pl.col(cols).cast(pl.Int64))
    if out.height == 0:
        warnings.warn(
            f"nba_factor_rapm_rows: none of {poss.height} possessions is in a {season_type!r} game of the schedule",
            UserWarning,
            stacklevel=2,
        )
    return out


def _load(seasons: int | list[int], league_id: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The (possessions, schedule) releases for ``seasons`` (named by the year they end)."""
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if league_id == "10":
        from sportsdataverse.wnba import wnba_loaders as W

        return W.load_wnba_stats_possessions(seasons_list), W.load_wnba_stats_schedules(seasons_list)
    from sportsdataverse.nba import nba_loaders as N

    start_years = [s - 1 for s in seasons_list]  # the NBA loaders take the start year
    return N.load_nba_stats_possessions(start_years), N.load_nba_stats_schedules(start_years)


def nba_player_factor_rapm(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = "regular-season",
    min_poss: int | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Player RAPM split into Dean Oliver's four factors, offense and defense, per player-season.

    The definitions are in :mod:`sportsdataverse._common.factor_rapm`: ``o_efg_rapm + o_tov_rapm +
    o_orb_rapm + o_ftr_rapm + o_other_rapm`` is ``o_rapm`` and the five ``d_*`` components add up to
    ``d_rapm``, each season fit on its own.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"preseason"``, ``"playoffs"`` or
            ``"play-in"``, matched to the schedule's ``season_type``.
        min_poss: The rank gate, offensive plus defensive possessions. Defaults to 400 (NBA) or 200 (WNBA).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, player_id)``: ``off_poss`` / ``def_poss``, ``rapm`` / ``o_rapm`` /
        ``d_rapm``, the ten components (the four factors and the remainder, each way; per 100
        possessions, higher is better), ``<m>_rank`` / ``<m>_pct`` within the season among players
        with at least ``min_poss`` possessions, ``<m>_se`` (clustered by game), ``<m>_n``, and the
        ``pts_per_fga`` and ``ridge_alpha`` of the fit. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` or ``season_type`` is not one
            of the four above.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_player_factor_rapm import nba_player_factor_rapm
            split = nba_player_factor_rapm(2025)
            split.select("player_id", "rapm", "o_efg_rapm", "o_tov_rapm", "d_drb_rapm").head()

        The six-factor view (free throws folded into shooting)::

            six = split.select(
                "season", "player_id",
                o_shooting=pl.col("o_efg_rapm") + pl.col("o_ftr_rapm"),
                o_tov=pl.col("o_tov_rapm"), o_reb=pl.col("o_orb_rapm"),
                d_shooting=pl.col("d_efg_rapm") + pl.col("d_ftr_rapm"),
                d_tov=pl.col("d_tov_rapm"), d_reb=pl.col("d_drb_rapm"),
            )
    """
    _check_league(league_id)
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")
    rows = nba_factor_rapm_rows(*_load(seasons, league_id), season_type)
    gate = _MIN_POSS[league_id] if min_poss is None else min_poss
    out = factor_rapm(rows, player_dtype=_PLAYER, min_poss=gate, league_id=league_id)
    return out.to_pandas() if return_as_pandas else out
