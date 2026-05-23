"""sportsdataverse.mlb loaders — release-asset loaders for pre-built MLB datasets.

This file is currently a **skeleton**. The sdv-data MLB release pipeline does not
yet exist (no ``sdv-data/mlb-data`` release repo published). When it does, this
module is the convergence point for fast tabular loads — mirroring the pattern
used by :mod:`sportsdataverse.nfl` (nflverse releases) and
:mod:`sportsdataverse.nhl` (sdv-hosted releases).

Candidate signatures (matching the NHL pattern):

    load_mlb_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame
    load_mlb_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame
    load_mlb_team_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame
    load_mlb_player_boxscore(seasons: List[int], return_as_pandas=False) -> pl.DataFrame
    load_mlb_rosters(seasons: List[int], return_as_pandas=False) -> pl.DataFrame

Until those releases exist, callers should reach for the live ESPN wrappers
(:mod:`sportsdataverse.mlb.mlb_pbp`, :mod:`sportsdataverse.mlb.mlb_schedule`)
or the MLB Stats API wrappers (:mod:`sportsdataverse.mlb.mlb_api`).
"""

from __future__ import annotations

from typing import List


def load_mlb_pbp(seasons: List[int], return_as_pandas: bool = False):
    """load_mlb_pbp - planned: load pre-built season-level MLB play-by-play.

    TODO: Implement once an MLB-data release pipeline is in place.

    Raises:
        NotImplementedError: always — release pipeline not yet published.
    """
    raise NotImplementedError(
        "load_mlb_pbp() is not yet implemented. The sdv-data MLB release pipeline "
        "does not yet publish per-season play-by-play parquet files. For now, use "
        "sportsdataverse.mlb.espn_mlb_pbp(game_id) or "
        "sportsdataverse.mlb.mlb_api_pbp(game_pk) for live per-game fetches.",
    )


def load_mlb_schedule(seasons: List[int], return_as_pandas: bool = False):
    """load_mlb_schedule - planned: load pre-built season-level MLB schedule.

    TODO: Implement once an MLB-data release pipeline is in place.

    Raises:
        NotImplementedError: always — release pipeline not yet published.
    """
    raise NotImplementedError(
        "load_mlb_schedule() is not yet implemented. Use "
        "sportsdataverse.mlb.espn_mlb_schedule(dates=season_year) for now.",
    )


def load_mlb_team_boxscore(seasons: List[int], return_as_pandas: bool = False):
    """load_mlb_team_boxscore - planned: load pre-built season-level MLB team boxscores."""
    raise NotImplementedError("load_mlb_team_boxscore() is not yet implemented. Awaiting MLB release pipeline.")


def load_mlb_player_boxscore(seasons: List[int], return_as_pandas: bool = False):
    """load_mlb_player_boxscore - planned: load pre-built season-level MLB player boxscores."""
    raise NotImplementedError("load_mlb_player_boxscore() is not yet implemented. Awaiting MLB release pipeline.")


def load_mlb_rosters(seasons: List[int], return_as_pandas: bool = False):
    """load_mlb_rosters - planned: load pre-built season-level MLB rosters."""
    raise NotImplementedError("load_mlb_rosters() is not yet implemented. Awaiting MLB release pipeline.")
