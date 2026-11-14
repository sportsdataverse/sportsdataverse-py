"""League context for the NBA / WNBA ``player_impact`` table: ``_n``, ``_rank``, ``_pct`` and league averages.

``nba_player_impact`` / ``wnba_player_impact`` (built by the hoopR-nba-stats-data and
wehoop-wnba-stats-data producers from :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`,
:func:`~sportsdataverse.nba.nba_adj_rapm.nba_adj_rapm`, :func:`~sportsdataverse.nba.nba_spm.nba_spm`,
:func:`~sportsdataverse.nba.nba_bpm.nba_bpm` and :func:`~sportsdataverse.nba.nba_war.nba_war`)
follows the season-table suffix grammar of :mod:`sportsdataverse._common.league_context`:

* ``<m>_n`` -- the possessions the metric is measured over: ``off_poss`` for an offensive
  rating (``o_rapm``, ``o_adj_rapm``, ``ospm``, ``obpm``), ``def_poss`` for a defensive one, and
  ``off_poss + def_poss`` for a net rating and for ``war``.
* ``<m>_rank`` / ``<m>_pct`` -- 1 is best, within ``(season, season_type)``, at least
  :data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` ranked players. A rating
  is ranked only when its ``<m>_n`` reaches the on-court gate of the same side in
  :data:`~sportsdataverse._common.rate_gates.ON_OFF_MIN_N` (the possessions at which a player's
  on-court rating is half signal: NBA 1,700 offensive / 1,600 defensive / 3,200 total, WNBA 610 /
  1,200 / 1,600). A regularized or box rating is less noisy than the raw on-court rating at the
  same sample, so the gate is conservative. ``war`` is a count, not a rate: it has no sample gate.

Every metric is higher-is-better (a defensive rating is positive for a good defender) except
``boban_projected_sd``, which ranks ascending: 1 is the most certain projection. The three BOBAN
columns are multi-season estimates with no single-side sample, so their ``<m>_n`` is the season's
``off_poss + def_poss`` and they take the net on-court gate, the closest existing one (NBA 3,200,
WNBA 1,600): conservative again, since the Kalman filter also carries earlier seasons.
``boban_projected_rating`` forecasts the next season; it is ranked within the season it was made.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.league_context import (
    LEAGUE_AVERAGES_SCHEMA,
    MIN_COHORT_PLAYERS,
    attach_ranks,
    attach_sample_sizes,
    league_averages,
)
from sportsdataverse._common.rate_gates import NBA_LEVEL, ON_OFF_MIN_N

__all__ = [
    "CURRENT_IMPACT_RANKED",
    "IMPACT_LOWER_IS_BETTER",
    "IMPACT_RANKED",
    "nba_current_impact_context",
    "nba_player_impact_context",
    "nba_player_impact_league_averages",
]

#: Ranked impact metric -> the side its ``<m>_n`` counts: ``"o"`` offensive possessions,
#: ``"d"`` defensive, ``"net"`` both. Higher is better except :data:`IMPACT_LOWER_IS_BETTER`.
IMPACT_RANKED: dict[str, str] = {
    "o_rapm": "o",
    "d_rapm": "d",
    "rapm": "net",
    "o_adj_rapm": "o",
    "d_adj_rapm": "d",
    "adj_rapm": "net",
    "ospm": "o",
    "dspm": "d",
    "spm": "net",
    "obpm": "o",
    "dbpm": "d",
    "bpm": "net",
    "war": "net",
    "boban_filtered_skill": "net",
    "boban_projected_rating": "net",
    "boban_projected_sd": "net",
}
#: Ranked metrics where lower is better (1 = the lowest).
IMPACT_LOWER_IS_BETTER: tuple[str, ...] = ("boban_projected_sd",)
_N = {"o": pl.col("off_poss"), "d": pl.col("def_poss"), "net": pl.col("off_poss") + pl.col("def_poss")}
_GATE = {"o": "off_rtg_on", "d": "def_rtg_on", "net": "net_rtg_on"}
_UNGATED = ("war",)  # a count, not a rate


def _level(league_id: str) -> str:
    if league_id not in NBA_LEVEL:
        raise ValueError(f"league_id must be one of {sorted(NBA_LEVEL)}, got {league_id!r}")
    return NBA_LEVEL[league_id]


def _gates(level: str) -> dict[str, float]:
    return {m: ON_OFF_MIN_N[level][_GATE[s]] for m, s in IMPACT_RANKED.items() if m not in _UNGATED}


def nba_player_impact_context(
    impact: pl.DataFrame, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Append ``<m>_n``, ``<m>_rank`` and ``<m>_pct`` to a ``player_impact`` table.

    Ranks are computed within ``(season, season_type)`` over the players whose ``<m>_n`` reaches
    the metric's gate (module docstring); a playoff run rarely reaches the regular-season gates,
    so most playoff ranks are null. A row's rank and percentile are also null when its metric is
    null or non-finite, when its ``season`` or ``season_type`` is null, when fewer than
    :data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players qualify in the
    group, or when every qualifying value in the group is equal. Nothing is dropped, and running
    it on its own output gives the same frame.

    Args:
        impact: A ``player_impact`` table: ``season``, ``season_type``, ``off_poss``, ``def_poss``
            and the 16 metrics of :data:`IMPACT_RANKED`, one row per player, season and season type.
        league_id: ``"00"`` NBA or ``"10"`` WNBA, which picks the sample gates.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``impact`` with ``<m>_n`` (Int64), then ``<m>_rank`` and ``<m>_pct`` (Float64) appended for
        every metric of :data:`IMPACT_RANKED`, in that order.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.
        KeyError: If a metric, ``season``, ``season_type``, ``off_poss`` or ``def_poss`` is missing.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_context
            ranked = nba_player_impact_context(load_nba_player_impact(2026))
            ranked.sort("adj_rapm_rank").select("player_name", "adj_rapm", "adj_rapm_rank", "adj_rapm_pct").head()
    """
    level = _level(league_id)
    out = attach_sample_sizes(impact, {m: _N[s] for m, s in IMPACT_RANKED.items()})
    out = attach_ranks(
        out,
        list(IMPACT_RANKED),
        min_cohort=MIN_COHORT_PLAYERS,
        by=("season", "season_type"),
        lower_is_better=IMPACT_LOWER_IS_BETTER,
        min_n=_gates(level),
    )
    return out.to_pandas() if return_as_pandas else out


def nba_player_impact_league_averages(
    impact: pl.DataFrame, *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """The league-average rows of a ``player_impact`` table, one block per season type.

    The football F1 rows of :func:`~sportsdataverse._common.league_context.league_averages` over
    the players who pass each metric's sample gate, so ``n`` is that metric's qualified cohort:
    the players with a finite value and enough possessions. It is reported even where
    :func:`nba_player_impact_context` leaves the ranks null (fewer than 10 qualifiers, or all
    equal). ``category`` is ``player_impact_`` plus the ``season_type`` lowercased with spaces as
    underscores: ``player_impact_regular_season`` and ``player_impact_playoffs`` for the releases'
    two season types. ``qualifier_min`` is null (the gates differ by metric).

    Args:
        impact: A ``player_impact`` table, as :func:`nba_player_impact_context` takes.
        league_id: ``"00"`` NBA or ``"10"`` WNBA: the ``level`` label and the sample gates.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        One row per ``(season, season type, metric)`` in
        :data:`~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA`; an empty input
        returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.
        KeyError: If a metric, ``season``, ``off_poss`` or ``def_poss`` is missing.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_league_averages
            nba_player_impact_league_averages(load_nba_player_impact(2026))
    """
    level = _level(league_id)
    sized = attach_sample_sizes(impact, {m: _N[s] for m, s in IMPACT_RANKED.items()})
    frames = [
        league_averages(
            sized.filter(pl.col("season_type") == st),
            list(IMPACT_RANKED),
            level=level,
            entity="player",
            category="player_impact_" + st.lower().replace(" ", "_"),
            min_n=_gates(level),
        )
        for st in sorted(sized["season_type"].unique().drop_nulls().to_list())
    ]
    out = pl.concat(frames) if frames else pl.DataFrame(schema=LEAGUE_AVERAGES_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


#: ``current_impact``'s headline columns -> the side its ``<m>_n`` counts (as :data:`IMPACT_RANKED`).
CURRENT_IMPACT_RANKED: dict[str, str] = {"o_current_impact": "o", "d_current_impact": "d", "current_impact": "net"}


def nba_current_impact_context(current: pl.DataFrame, *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Append ``<m>_n``, ``<m>_rank`` and ``<m>_pct`` to a ``current_impact`` table, within each ``as_of`` date.

    ``nba_current_impact``'s three ratings (:data:`CURRENT_IMPACT_RANKED`) are ranked among the
    players of the date who have played this season: ``<m>_n`` (the season-to-date ``off_poss``,
    ``def_poss`` or both) at least 1. There is no larger sample gate, unlike ``player_impact``'s:
    a current rating is the box prior moved by the games, so a player with few possessions sits near
    his prior, never at a raw on-court extreme, and the date's ranks exist from the season's first
    week (the season gates of 1,600 to 3,200 possessions would leave the first seven weeks
    unranked). 1 is best; ``d_current_impact`` is positive for a good defense, so it ranks
    descending too. A rank and percentile are null when the rating is null or non-finite, when
    ``as_of`` is null, when fewer than
    :data:`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players have played (opening
    night), or when every value is equal. Nothing is dropped, and running it on its own output gives
    the same frame.

    Args:
        current: A ``current_impact`` table (:func:`~sportsdataverse.nba.nba_current_impact.nba_current_impact`,
            or the ``nba_current_impact`` release): ``as_of``, ``off_poss``, ``def_poss`` and the three ratings.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        ``current`` with ``<m>_n`` (Int64), then ``<m>_rank`` and ``<m>_pct`` (Float64) appended for
        ``o_current_impact``, ``d_current_impact`` and ``current_impact``, in that order.

    Raises:
        KeyError: If a rating, ``as_of``, ``off_poss`` or ``def_poss`` is missing.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_current_impact, nba_current_impact_context
            cur = nba_current_impact_context(nba_current_impact(possessions, priors, ["2025-01-15", "2025-03-01"]))
            cur.sort("as_of", "current_impact_rank").select("as_of", "player_id", "current_impact", "current_impact_rank")
    """
    missing = [c for c in ("off_poss", "def_poss") if c not in current.columns]
    if missing:
        raise KeyError(f"{missing} missing from the frame: the ranks' sample sizes need them")
    out = attach_sample_sizes(current, {m: _N[s] for m, s in CURRENT_IMPACT_RANKED.items()})
    out = attach_ranks(
        out,
        list(CURRENT_IMPACT_RANKED),
        min_cohort=MIN_COHORT_PLAYERS,
        by=("as_of",),
        min_n=dict.fromkeys(CURRENT_IMPACT_RANKED, 1),
    )
    return out.to_pandas() if return_as_pandas else out
