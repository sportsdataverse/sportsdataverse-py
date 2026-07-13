"""Women's college basketball player/team stat aggregation (wbigballR port).

Thin shims over :mod:`sportsdataverse.mbb.mbb_ncaa_stats_agg`. The transform
layer is league-agnostic (wbigballR's copy is byte-identical to bigballR's;
``dev/bigballr_port/design.md``), so these are pure delegations providing
the canonical ``ncaa_wbb_*`` names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_stats_agg import ncaa_mbb_player_stats, ncaa_mbb_team_stats

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "ncaa_wbb_player_stats",
    "ncaa_wbb_team_stats",
]


def ncaa_wbb_player_stats(
    pbp: pl.DataFrame,
    *,
    multi_games: bool = False,
    simple: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate WBB play-by-play into per-player box stats (wbigballR ``get_player_stats``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_player_stats` —
    see it for the algorithm and column contracts.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``ncaa_wbb_game_pbp`` output).
        multi_games: Aggregate across games per (player, team) — the
            season-stat surface.
        simple: Return the reduced surface without the transition / assisted
            / putback / block-location splits.
        fix_tip_in: Count the real ``"Tip In"`` vocabulary (default); False
            reproduces R's ``"Tip-In"`` bug for oracle parity.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per player+team (+game when ``multi_games=False``).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_player_stats
            stats = ncaa_wbb_player_stats(pbp)
            print(stats.shape)
    """
    return ncaa_mbb_player_stats(
        pbp,
        multi_games=multi_games,
        simple=simple,
        fix_tip_in=fix_tip_in,
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_team_stats(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate WBB play-by-play into per-team game stats (wbigballR ``get_team_stats``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_stats_agg.ncaa_mbb_team_stats` — see
    it for the algorithm and column contract.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``ncaa_wbb_game_pbp`` output).
        include_transition: Append the ``_trans``/``_half`` split surface.
        fix_tip_in: Count the real ``"Tip In"`` vocabulary (default); False
            reproduces R's ``"Tip-In"`` bug for oracle parity.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per team per game.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_stats_agg import ncaa_wbb_team_stats
            team = ncaa_wbb_team_stats(pbp)
            print(team.shape)
    """
    return ncaa_mbb_team_stats(
        pbp,
        include_transition=include_transition,
        fix_tip_in=fix_tip_in,
        return_as_pandas=return_as_pandas,
    )
