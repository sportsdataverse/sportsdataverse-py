"""Producer ranks on the NBA / WNBA ``player_stats_advanced`` table, with a possessions minimum.

``nba_stats_leaguedash`` / ``wnba_stats_leaguedash``'s ``player_stats_advanced`` (stats.nba.com /
stats.wnba.com ``leaguedashplayerstats``, the Advanced measure) ships the API's own ``<m>_rank``
columns: every player who appeared is ranked, a 20-possession call-up beside a starter.
:func:`nba_player_stats_advanced_context` replaces the ``_rank`` of the rated measures in
:data:`ADVANCED_RANKED` with the season-table grammar of
:mod:`sportsdataverse._common.league_context` and adds their ``<m>_pct``:

* ranked within ``(season, season_type, per_mode)``, 1 is best (``def_rating`` and ``tm_tov_pct``
  rank ascending, :data:`ADVANCED_LOWER_IS_BETTER`; ``usg_pct`` ranks the heaviest usage first);
* only players whose ``poss`` (possessions played, one side) reaches :func:`advanced_min_poss`: NBA
  1,600, WNBA 800. That is half the on-court net-rating gate of
  :data:`~sportsdataverse._common.rate_gates.ON_OFF_MIN_N` (NBA 3,200 / WNBA 1,600, counted on both
  sides), the possessions at which a player's on-court net rating is half signal, so a net rating
  here is ranked on the same sample as ``player_on_off``'s. The box rates (TS%, usage, PIE) settle
  sooner, so the gate is conservative for them. A full NBA regular season qualifies about 330
  players (2025-26: 333 of 582); a playoff run rarely does;
* null under :data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` qualifiers.

The table's other ``_rank`` columns (``gp_rank``, ``min_rank``, the ``e_`` / ``sp_work_`` variants,
``pace_rank``, the shooting counts) stay the API's.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.league_context import MIN_COHORT_PLAYERS, attach_ranks
from sportsdataverse._common.rate_gates import NBA_LEVEL, ON_OFF_MIN_N

__all__ = [
    "ADVANCED_LOWER_IS_BETTER",
    "ADVANCED_RANKED",
    "advanced_min_poss",
    "nba_player_stats_advanced_context",
]

#: The ``player_stats_advanced`` measures given producer ranks, in the table's column order.
ADVANCED_RANKED: tuple[str, ...] = (
    "off_rating",
    "def_rating",
    "net_rating",
    "ast_pct",
    "ast_to",
    "oreb_pct",
    "dreb_pct",
    "reb_pct",
    "tm_tov_pct",
    "efg_pct",
    "ts_pct",
    "usg_pct",
    "pie",
)
#: Ranked measures where lower is better (1 = the lowest).
ADVANCED_LOWER_IS_BETTER: tuple[str, ...] = ("def_rating", "tm_tov_pct")


def advanced_min_poss(league_id: str = "00") -> float:
    """The possessions (one side) a player needs to be ranked: half the on-court net gate.

    Args:
        league_id: ``"00"`` NBA or ``"10"`` WNBA.

    Returns:
        NBA 1,600.0, WNBA 800.0.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba import advanced_min_poss
            advanced_min_poss("10")  # 800.0
    """
    if league_id not in NBA_LEVEL:
        raise ValueError(f"league_id must be one of {sorted(NBA_LEVEL)}, got {league_id!r}")
    return ON_OFF_MIN_N[NBA_LEVEL[league_id]]["net_rtg_on"] / 2


def nba_player_stats_advanced_context(
    adv: pl.DataFrame, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Replace ``player_stats_advanced``'s API ranks of the rated measures with gated producer ranks.

    For each measure of :data:`ADVANCED_RANKED`, ``<m>_rank`` becomes the rank among the players of
    its ``(season, season_type, per_mode)`` whose ``poss`` reaches :func:`advanced_min_poss`, and
    ``<m>_pct`` is appended (module docstring). A row's rank and percentile are null when it is
    under the gate, when its value is null or non-finite, when a group key is null, when fewer than
    :data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players qualify or every
    qualifying value is equal. Nothing is dropped, and running it on its own output gives the same
    frame.

    Args:
        adv: A ``player_stats_advanced`` table (``load_nba_stats_leaguedash("player_stats_advanced")``
            / ``load_wnba_stats_leaguedash``): ``season``, ``season_type``, ``per_mode``, ``poss``
            and the measures of :data:`ADVANCED_RANKED`.
        league_id: ``"00"`` NBA or ``"10"`` WNBA, which picks the gate.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``adv`` with each ranked measure's ``<m>_rank`` replaced in place (Float64; ties share the
        average rank) and its ``<m>_pct`` (Float64) appended, in :data:`ADVANCED_RANKED` order.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.
        KeyError: If a measure, ``season``, ``season_type``, ``per_mode`` or ``poss`` is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import load_nba_stats_leaguedash, nba_player_stats_advanced_context
            adv = nba_player_stats_advanced_context(load_nba_stats_leaguedash("player_stats_advanced", seasons=2026))
            adv.filter(pl.col("season_type") == "Regular Season").sort("ts_pct_rank").select(
                "player_name", "poss", "ts_pct", "ts_pct_rank", "ts_pct_pct"
            ).head()
    """
    gate = advanced_min_poss(league_id)
    if "poss" not in adv.columns:
        raise KeyError("poss missing from the frame: the possessions gate needs it")
    out = attach_ranks(
        adv,
        list(ADVANCED_RANKED),
        min_cohort=MIN_COHORT_PLAYERS,
        by=("season", "season_type", "per_mode"),
        lower_is_better=ADVANCED_LOWER_IS_BETTER,
        qualifier=pl.col("poss") >= gate,
    )
    return out.to_pandas() if return_as_pandas else out
