"""NBA / WNBA four-factor RAPM split (BB-D10): ``nba.player_factor_rapm``.

Builds :mod:`sportsdataverse._common.factor_rapm` from two stats.nba.com releases:
``nba_stats_possessions`` (the ten players on the floor and the possession's points, shots,
offensive rebounds and turnovers) and ``nba_stats_schedules`` (each game's season type).
``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_player_factor_rapm` binds it.

``o_rapm`` / ``d_rapm`` are :func:`sportsdataverse.nba.nba_rapm.nba_rapm` on the season's
possessions, the plain RAPM of ``nba.player_impact``. Checked against that release (regular season):
WNBA 2024 and 2025 equal it to 1e-11 on every player (157 and 181); NBA 2024-25 and 2025-26 correlate
0.997 (o_rapm) and 0.997 (d_rapm) on all 569 and 582 players, a max difference of 1.3, because the
possessions release is not the exact frame the RAPM was fit on (off_poss differs by up to 184).
"""

from __future__ import annotations

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
        per possession.
    """
    cols = ["season", *_SLOTS, *COUNTS]
    if possessions.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema={"game_id": pl.Utf8(), **{c: pl.Int64() for c in cols}})
    games = schedule.filter(pl.col("season_type") == season_type).select(pl.col("game_id").cast(pl.Utf8)).unique()
    return (
        possessions.with_columns(pl.col("game_id").cast(pl.Utf8))
        .join(games, on="game_id", how="semi")
        .select("game_id", pl.col(cols).cast(pl.Int64))
    )


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
    o_orb_rapm + o_ftr_rapm`` is ``o_rapm`` and ``d_efg_rapm + d_tov_rapm + d_drb_rapm + d_ftr_rapm``
    is ``d_rapm``, each season fit on its own.

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
        ``d_rapm``, the eight components (per 100 possessions, higher is better), ``<m>_rank`` /
        ``<m>_pct`` within the season among players with at least ``min_poss`` possessions,
        ``<m>_se`` (clustered by game), ``<m>_n``, and the ``pts_per_fga`` and ``ridge_alpha`` of the fit. Ids are the
        stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

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

            split.with_columns(o_shooting=pl.col("o_efg_rapm") + pl.col("o_ftr_rapm"))
    """
    _check_league(league_id)
    if season_type not in _SEASON_TYPES:
        raise ValueError(f"season_type must be one of {list(_SEASON_TYPES)}, got {season_type!r}")
    rows = nba_factor_rapm_rows(*_load(seasons, league_id), season_type)
    gate = _MIN_POSS[league_id] if min_poss is None else min_poss
    out = factor_rapm(rows, player_dtype=_PLAYER, min_poss=gate)
    return out.to_pandas() if return_as_pandas else out
