"""PWHL game market -- thin shim over :mod:`sportsdataverse.nhl.nhl_market`.

Re-exports the NHL pregame + in-game win-probability surface with
``league="pwhl"`` defaulted (the closed-form trio + the logistic scorer are
league-agnostic; PWHL constants come from ``LEAGUE_CONSTANTS["pwhl"]``).
Same by-reference pattern as ``wbb_rapm`` over ``mbb_rapm``.

**Oracle gate deferred** (design spec Sec 9-7): a real PWHL market/in-game
validation waits on xG-bearing PWHL pbp; the in-game scorer additionally
needs a committed ``pwhl_in_game_wp`` artifact, which is trained once that
data lands. See ``tests/fixtures/nhl_prediction/README.md`` for the capture
contract. The shim wiring is unit-tested now.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games

        preds = pwhl_predict_games(games, ratings)

See Also:
    * `nhl-api-py`_ -- companion NHL Python client (the core this shim re-exports).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import importlib
from typing import Any

# ponytail: importlib.import_module for uniformity with the other two shims
# (nhl_market itself isn't name-shadowed, but this keeps the pattern identical
# and monkeypatch-interceptable).
_core = importlib.import_module("sportsdataverse.nhl.nhl_market")

__all__ = ["pwhl_predict_games", "pwhl_in_game_win_prob"]


def pwhl_predict_games(games: Any, ratings: Any, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL vectorized pregame margin/win-prob/total (+ market edge).

    Delegates to :func:`sportsdataverse.nhl.nhl_market.nhl_predict_games` with
    ``league="pwhl"`` defaulted.

    Args:
        games: a schedule-shaped frame (``game_id``, ``home_team``, ``away_team``, ``neutral_site``).
        ratings: a :func:`pwhl_team_ratings`-shaped frame.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``odds``, ``return_as_pandas``).

    Returns:
        The NHL core's per-game prediction frame, computed with PWHL constants.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games
            preds = pwhl_predict_games(games, ratings)
    """
    return _core.nhl_predict_games(games, ratings, league=league, **kwargs)


def pwhl_in_game_win_prob(pbp: Any, pregame_home_prob: float, *, league: str = "pwhl", **kwargs: Any) -> Any:
    """PWHL per-play live home win probability from the bundled in-game model.

    Delegates to :func:`sportsdataverse.nhl.nhl_market.nhl_in_game_win_prob`
    with ``league="pwhl"`` defaulted. NOTE: requires a committed
    ``pwhl_in_game_wp`` artifact, deferred until PWHL data lands (see module
    docstring); calling it before then raises a clear ``FileNotFoundError``
    from the artifact loader, not a silent bad result.

    Args:
        pbp: a play-by-play frame shaped like ``load_nhl_pbp_full``.
        pregame_home_prob: the pregame home win probability anchor.
        league: league key (defaults to ``"pwhl"``).
        **kwargs: forwarded to the NHL core (``return_as_pandas``).

    Returns:
        The NHL core's per-play ``home_win_prob`` frame.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_market import pwhl_in_game_win_prob
            wp = pwhl_in_game_win_prob(pbp, pregame_home_prob=0.5)
    """
    return _core.nhl_in_game_win_prob(pbp, pregame_home_prob, league=league, **kwargs)
