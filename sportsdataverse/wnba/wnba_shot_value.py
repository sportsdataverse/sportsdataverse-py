"""Women's shot-value spine (league_id="10").

Thin shim over :mod:`sportsdataverse.nba.nba_shot_value` -- the models are one
league-agnostic core switched by ``league_id`` (women's court geometry +
shrinkage constant live in ``nba_shot_value_constants`` keyed ``"10"``), so
``wnba_shot_value``, ``score_shot_xpoints`` and ``shooter_talent`` bind
``league_id="10"`` (the WNBA has no location booster, so its shots are scored
from the zone table, and its shrinkage ``k`` is the WNBA's); the league-free
model functions are re-exported **by reference**. G-League needs no shim --
call ``nba_shot_value(..., league_id="20")``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

from sportsdataverse.nba.nba_shot_value import make_prob_by_context as make_prob_by_context
from sportsdataverse.nba.nba_shot_value import make_prob_joint as make_prob_joint
from sportsdataverse.nba.nba_shot_value import nba_shot_value
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints as _score_shot_xpoints
from sportsdataverse.nba.nba_shot_value import shooter_talent as _shooter_talent
from sportsdataverse.nba.nba_shot_value import shot_selection_quality as shot_selection_quality
from sportsdataverse.nba.nba_shot_value import zone_value_map as zone_value_map

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

__all__ = [
    "make_prob_by_context",
    "make_prob_joint",
    "score_shot_xpoints",
    "shooter_talent",
    "shot_selection_quality",
    "wnba_shot_value",
    "zone_value_map",
]


def wnba_shot_value(
    player_ids: "list[int]",
    season: str,
    *,
    include_context: bool = False,
    return_as_pandas: bool = False,
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """WNBA one-call shot-value spine (``league_id="10"``).

    Thin wrapper binding :func:`sportsdataverse.nba.nba_shot_value.nba_shot_value`
    to the women's league; fetches each player's ``shotchartdetail``, scores
    per-shot expected points from the free ``LeagueAverages`` zone table, and
    returns the scored shots plus shooter talent, selection quality, and
    zone-value maps (and the defender/shot-clock context tables when
    ``include_context=True``). Women's court geometry + shrinkage constant are
    keyed ``"10"`` in ``nba_shot_value_constants``.

    Args:
        player_ids: Player ids to fetch.
        season: Season string, e.g. ``"2024"``.
        include_context: Also fetch + return the ``playerdashptshots``
            defender/shot-clock context tables.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"shots", "talent", "selection", "zones"}`` (plus ``"context"`` when
        requested). An empty fetch returns a dict of zero-row frames.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_shot_value
            out = wnba_shot_value([1628886], "2024")
            out["talent"].head()

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    return nba_shot_value(
        player_ids,
        season,
        league_id="10",
        include_context=include_context,
        return_as_pandas=return_as_pandas,
    )


def score_shot_xpoints(
    shots: pl.DataFrame,
    league_avgs: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Score WNBA shots with expected points from the ``LeagueAverages`` zone table.

    :func:`sportsdataverse.nba.nba_shot_value.score_shot_xpoints` bound to ``league_id="10"``. The
    WNBA has no location booster (its fit failed the walk-forward gates), so every shot is scored
    from its zone triple.

    Args:
        shots: Per-shot ``Shot_Chart_Detail`` frame (``shot_type``, the three zone keys,
            ``shot_made_flag``).
        league_avgs: The ``LeagueAverages`` frame.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input plus ``shot_value:Int64, base_fg_pct:Float64, xpoints:Float64,
        actual_points:Float64``.

    Raises:
        ValueError: A zone join key has a different dtype on the two frames.

    Example:
        Quick start::

            from sportsdataverse.wnba import score_shot_xpoints
            scored = score_shot_xpoints(shots, league_avgs)

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    return _score_shot_xpoints(shots, league_avgs, league_id="10", return_as_pandas=return_as_pandas)


def shooter_talent(
    scored_shots: pl.DataFrame,
    *,
    min_attempts: int = 50,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Regressed WNBA shooter talent: make%-above-expected, shrunk by the WNBA's ``k``.

    :func:`sportsdataverse.nba.nba_shot_value.shooter_talent` bound to ``league_id="10"``.

    Args:
        scored_shots: :func:`score_shot_xpoints` output.
        min_attempts: Drop shooters with fewer attempts.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``player_id``: ``player_id:Int64, n_att:Int64, actual_makes:Int64,
        exp_makes:Float64, points_above_expected:Float64, raw_above_pct:Float64,
        talent_pct:Float64``.

    Example:
        Quick start::

            from sportsdataverse.wnba import score_shot_xpoints, shooter_talent
            talent = shooter_talent(score_shot_xpoints(shots, league_avgs))

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    return _shooter_talent(scored_shots, league_id="10", min_attempts=min_attempts, return_as_pandas=return_as_pandas)
