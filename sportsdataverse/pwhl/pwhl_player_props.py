"""PWHL player-prop projections -- thin shim over
:mod:`sportsdataverse.nhl.nhl_player_props`.

Re-exports the NHL empirical-Bayes props + game-total surface with
``league="pwhl"`` defaulted (the EB projection is league-agnostic; PWHL
prop priors/kappa come from ``LEAGUE_CONSTANTS["pwhl"]``). Same by-reference
pattern as ``wbb_rapm`` over ``mbb_rapm``.

**Oracle gate deferred** (design spec Sec 9-7): a real PWHL props validation
waits on xG-bearing PWHL pbp (the matchup multiplier reads model-① ratings).
``load_pwhl_skater_boxscores`` exists, so the usage/efficiency half is
loadable, but the matchup + game-script halves need the deferred ratings.
See ``tests/fixtures/nhl_prediction/README.md`` for the capture contract.
The shim wiring is unit-tested now.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_player_props import pwhl_player_props

        props = pwhl_player_props(2024)

See Also:
    * `nhl-api-py`_ -- companion NHL Python client (the core this shim re-exports).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import importlib
from typing import Any

# ponytail: importlib.import_module -- the nhl package __init__ exports a
# function nhl_player_props that shadows the submodule attribute; import_module
# reads sys.modules so _core is always the real module.
_core = importlib.import_module("sportsdataverse.nhl.nhl_player_props")

__all__ = ["pwhl_player_props", "pwhl_game_total"]


def pwhl_player_props(seasons: Any, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL empirical-Bayes shots/points player-prop projections.

    Delegates to :func:`sportsdataverse.nhl.nhl_player_props.nhl_player_props`
    with ``league="pwhl"`` defaulted.

    Args:
        seasons: an int or iterable of seasons.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``as_of_date``, ``stats``, ``return_as_pandas``).

    Returns:
        The NHL core's per-(player, game, stat) projection frame.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_player_props import pwhl_player_props
            props = pwhl_player_props(2024)
    """
    return _core.nhl_player_props(seasons, league=league, **kwargs)


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
