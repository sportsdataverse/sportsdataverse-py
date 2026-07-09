"""WNBA play-type/impact -- by-reference shims over the league-agnostic NBA core (league_id="10").

All four models (Synergy play-type ratings, matchup DRAPM, foul-drawing,
expected turnovers) are league-agnostic algorithms parameterized on
``league_id``; WNBA-specific inputs are handled entirely by the shared
``nba_stats``/``wnba_stats`` runtime routing. These thin wrappers only fix the
league default, mirroring the shipped ``wnba_shot_value`` wraps
``nba_shot_value`` pattern. G-League needs no shim -- call the NBA core
functions directly with ``league_id="20"``.

Synergy + matchup coverage is sparse for the WNBA relative to the NBA; every
wrapped function degrades to a zero-row frame with the documented schema when
the upstream fetch is empty (never raises).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

from sportsdataverse.nba.nba_expected_turnovers import nba_expected_turnovers as _tov
from sportsdataverse.nba.nba_foul_drawing import nba_foul_drawing as _foul
from sportsdataverse.nba.nba_matchup_drapm import nba_matchup_drapm as _drapm
from sportsdataverse.nba.nba_playtype import nba_playtype_ratings as _ratings
from sportsdataverse.nba.nba_playtype_constants import PlaytypeConfig

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

__all__ = [
    "wnba_playtype_ratings",
    "wnba_matchup_drapm",
    "wnba_foul_drawing",
    "wnba_expected_turnovers",
]


def wnba_playtype_ratings(
    season: str,
    *,
    off_team: "Optional[pl.DataFrame]" = None,
    def_team: "Optional[pl.DataFrame]" = None,
    schedule: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA Synergy play-type-adjusted offense/defense (``league_id="10"``).

    Thin wrapper binding
    :func:`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` to the
    women's league. Synergy coverage is sparse for the WNBA; an empty upstream
    fetch degrades to a zero-row frame (never raises).

    Args:
        season: Season string, e.g. ``"2024"``.
        off_team: Injected Synergy offensive team frame (bypasses the live fetch).
        def_team: Injected Synergy defensive team frame.
        schedule: Injected ``team_id``/``opp_team_id`` schedule frame.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Same schema as :func:`sportsdataverse.nba.nba_playtype.nba_playtype_ratings`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_playtype_ratings
            r = wnba_playtype_ratings("2024")
            print(r.sort("adj_off", descending=True).head())

        See Also:
            * `wehoop`_ -- women's basketball (R)

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _ratings(
        season,
        league_id="10",
        off_team=off_team,
        def_team=def_team,
        schedule=schedule,
        return_as_pandas=return_as_pandas,
    )


def wnba_matchup_drapm(
    season: str,
    *,
    matchups: "Optional[pl.DataFrame]" = None,
    config: "Optional[PlaytypeConfig]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA matchup defensive RAPM (``league_id="10"``).

    Thin wrapper binding
    :func:`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm` to the
    women's league.

    Args:
        season: Season string, e.g. ``"2024"``.
        matchups: Injected ``nba_stats_leagueseasonmatchups``-shaped frame.
        config: :class:`~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig` override.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Same schema as :func:`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_matchup_drapm
            d = wnba_matchup_drapm("2024")
            print(d.sort("matchup_drapm", descending=True).head())

        See Also:
            * `wehoop`_ -- women's basketball (R)

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _drapm(season, league_id="10", matchups=matchups, config=config, return_as_pandas=return_as_pandas)


def wnba_foul_drawing(
    season: str,
    *,
    base: "Optional[pl.DataFrame]" = None,
    advanced: "Optional[pl.DataFrame]" = None,
    player_mix: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA foul-drawing / FT-generation (``league_id="10"``).

    Thin wrapper binding
    :func:`sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing` to the
    women's league.

    Args:
        season: Season string, e.g. ``"2024"``.
        base: Injected ``nba_stats_leaguedashplayerstats`` (``Base``) frame.
        advanced: Injected ``Advanced``-measure frame.
        player_mix: Injected Synergy player-level offensive mix.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Same schema as :func:`sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_foul_drawing
            f = wnba_foul_drawing("2024")
            print(f.sort("foul_draw_skill", descending=True).head())

        See Also:
            * `wehoop`_ -- women's basketball (R)

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _foul(
        season, league_id="10", base=base, advanced=advanced, player_mix=player_mix, return_as_pandas=return_as_pandas
    )


def wnba_expected_turnovers(
    season: str,
    *,
    base: "Optional[pl.DataFrame]" = None,
    player_mix: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """WNBA expected turnovers / ball-security skill (``league_id="10"``).

    Thin wrapper binding
    :func:`sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`
    to the women's league.

    Args:
        season: Season string, e.g. ``"2024"``.
        base: Injected ``nba_stats_leaguedashplayerstats`` (``Base``) frame.
        player_mix: Injected Synergy player-level offensive mix.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Same schema as :func:`sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_expected_turnovers
            t = wnba_expected_turnovers("2024")
            print(t.sort("ball_security_skill", descending=True).head())

        See Also:
            * `wehoop`_ -- women's basketball (R)

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _tov(season, league_id="10", base=base, player_mix=player_mix, return_as_pandas=return_as_pandas)
