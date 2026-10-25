"""PWHL player-prop projections -- thin shim over
:mod:`sportsdataverse.nhl.nhl_player_props`.

Runs the NHL empirical-Bayes props + game-total surface with ``league="pwhl"``
defaulted (the EB projection is league-agnostic; PWHL prop priors/kappa come from
``LEAGUE_CONSTANTS["pwhl"]``), bound to PWHL inputs: the per-player-game box is
``load_pwhl_skater_boxscores`` (team and date from ``load_pwhl_schedules`` /
``load_pwhl_game_info``) and the matchup ratings are
:func:`sportsdataverse.pwhl.pwhl_team_ratings.pwhl_team_ratings`. No NHL loader is read.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_player_props import pwhl_player_props

        props = pwhl_player_props(2025)

See Also:
    * `nhl-api-py`_ -- companion NHL Python client (the core this shim re-exports).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import importlib
from typing import Any

import polars as pl

from sportsdataverse._codegen_runtime import _as_season_list
from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings

# ponytail: importlib.import_module -- the nhl package __init__ exports a
# function nhl_player_props that shadows the submodule attribute; import_module
# reads sys.modules so _core is always the real module.
_core = importlib.import_module("sportsdataverse.nhl.nhl_player_props")

__all__ = ["pwhl_player_props", "pwhl_game_total"]

_PROPS_BOX_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "player_id": pl.Int64(),
    "game_date": pl.Utf8(),
    "season": pl.Int64(),
    "position": pl.Utf8(),
    "team_abbrev": pl.Utf8(),
    "shots_on_goal": pl.Int64(),
    "points": pl.Int64(),
}


def _pwhl_props_box(seasons: Any) -> pl.DataFrame:
    """``load_pwhl_skater_boxscores`` reshaped to the box ``nhl_player_props`` reads.

    ``team_abbrev`` is the schedule's team name (the ``team`` key :func:`pwhl_team_ratings`
    emits), resolved by ``team_id``; ``game_date`` is ``game_info``'s ISO date; ``LD`` / ``RD``
    become ``D`` so the defence prior applies.
    """
    from sportsdataverse.pwhl.pwhl_loaders import (
        load_pwhl_game_info,
        load_pwhl_schedules,
        load_pwhl_skater_boxscores,
    )

    frames = []
    for season in _as_season_list(seasons):
        box = load_pwhl_skater_boxscores(season)
        sched = load_pwhl_schedules(season)
        info = load_pwhl_game_info(season)
        # a season with no published asset comes back as a column-less pl.DataFrame() (the
        # loaders warn and skip it); without dates the strictly-prior projection has no order.
        if box.is_empty() or sched.is_empty() or info.is_empty():
            continue
        teams = pl.concat(
            [
                sched.select(
                    pl.col("game_id").cast(pl.Int64),
                    pl.col(f"{side}_team_id").cast(pl.Int64).alias("team_id"),
                    pl.col(f"{side}_team").alias("team_abbrev"),
                )
                for side in ("home", "away")
            ]
        )
        dates = info.select(
            pl.col("game_id").cast(pl.Int64), pl.col("game_date_iso").str.slice(0, 10).alias("game_date")
        )
        box = box.with_columns(pl.col("game_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64))
        for key in ("game_id", "team_id"):
            assert box.schema[key] == teams.schema[key], f"{key}: {box.schema[key]} vs {teams.schema[key]}"
        frames.append(
            box.join(teams, on=["game_id", "team_id"], how="inner")
            .join(dates, on="game_id", how="left")
            .select(
                "game_id",
                pl.col("player_id").cast(pl.Int64),
                "game_date",
                pl.lit(season, dtype=pl.Int64).alias("season"),
                pl.when(pl.col("position").is_in(["LD", "RD", "D"]))
                .then(pl.lit("D"))
                .otherwise(pl.col("position"))
                .alias("position"),
                "team_abbrev",
                pl.col("shots").cast(pl.Int64).alias("shots_on_goal"),
                pl.col("points").cast(pl.Int64),
            )
        )
    return pl.concat(frames) if frames else pl.DataFrame(schema=_PROPS_BOX_SCHEMA)


def pwhl_player_props(seasons: Any, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL empirical-Bayes shots/points player-prop projections.

    Delegates to :func:`sportsdataverse.nhl.nhl_player_props.nhl_player_props`
    with ``league="pwhl"`` defaulted and the PWHL box loader and ratings bound
    (see the module docstring).

    Args:
        seasons: an int or iterable of seasons.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``as_of_date``, ``stats``, ``return_as_pandas``).

    Returns:
        The NHL core's per-(player, game, stat) projection frame.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_props import pwhl_player_props
            props = pwhl_player_props(2025)
    """
    return _core.nhl_player_props(
        seasons,
        league=league,
        load_skater_boxscores=_pwhl_props_box,
        team_ratings=pwhl_team_ratings,
        **kwargs,
    )


def pwhl_game_total(games: Any, ratings: Any, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL per-game expected total goals (re-export of the expected-goals helper).

    Delegates to :func:`sportsdataverse.nhl.nhl_player_props.nhl_game_total`
    with ``league="pwhl"`` defaulted.

    Args:
        games: a schedule-shaped frame.
        ratings: a :func:`pwhl_team_ratings`-shaped frame.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``return_as_pandas``).

    Returns:
        The NHL core's ``game_id``/``exp_total`` frame, computed with PWHL constants.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_props import pwhl_game_total
            totals = pwhl_game_total(games, ratings)
    """
    return _core.nhl_game_total(games, ratings, league=league, **kwargs)
