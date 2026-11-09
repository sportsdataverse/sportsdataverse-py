"""Rank sample gates for the basketball season tables: each rate's split-half r = 0.5 point.

A ``<m>_rank`` / ``<m>_pct`` is published only when the rate's own sample ``<m>_n`` reaches the
gate here. ``<m>_n`` is the rate's real denominator (BB-D2 grammar): field-goal attempts for
eFG% and FT rate, rebound chances for ORB%, possessions for the ratings and turnover rate, the
clock in regulation games for pace, the thinner side's possessions for an on/off difference. A
gate is ``M``, the sample at which the rate is half signal: on odd / even regular-season games its
split-half reliability follows ``r(n) = n / (n + M)``, so ``r(M) = 0.5``. Each ``M`` is rounded UP
to two significant figures, so a ranked row is at least half signal on the seasons it was fitted
on; on later holdout seasons the median rate is about half signal at its gate. ``math.inf`` marks
a rate whose curve never comes near 0.5 (its ``M`` would sit past twice the largest sample seen):
it is never ranked.

Fitted by ``tools/fit_rate_gates.py`` on each league's train seasons (its ``SPANS``; the later
holdout seasons are scored in ``tests/tools/test_fit_rate_gates.py``), from the ESPN team box
scores (team rates) and the BB-D6 game impact box (on/off). The study behind the unit choice
(real denominator against possessions everywhere, and the published conventions) is summarised
in sportsdataverse-py #1126.

* :data:`TEAM_MIN_N` gates the team rates (``{lg}_team_season_rates``, and the BB-D3 rates
  ``{lg}_team_strength`` carries) and the lineup rates (``{lg}_lineup_seasons``). A full team season
  clears the gates of the twelve BB-D3 rates; a split may not (a clutch split never does), and the
  three-point rates (college lineups only) mostly never do. A lineup's rate spreads at
  least as widely as a team's (on the NBA 2017-2026 possessions the five-man ``M`` is 0.3 to 0.9
  times the team one), so the team gate is a conservative lineup gate.
* :data:`ON_OFF_MIN_N` gates ``{lg}_player_on_off``. The on-court ratings clear it for most
  rotation players; an on/off difference rarely does, and in college no difference is ever ranked
  (its curve never comes near half signal), so most difference ranks are null.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping

__all__ = ["LEVELS", "NBA_LEVEL", "NCAA_LEVEL", "ON_OFF_MIN_N", "TEAM_MIN_N", "on_off_gates", "team_gates"]

#: The population labels a gate table is keyed by.
LEVELS: tuple[str, ...] = ("nba", "wnba", "mbb", "wbb")
#: The adapters' league arguments -> their level.
NBA_LEVEL: dict[str, str] = {"00": "nba", "10": "wnba"}
NCAA_LEVEL: dict[str, str] = {"mens": "mbb", "womens": "wbb"}

# Fitted by tools/fit_rate_gates.py (train seasons of its SPANS; tests/fixtures/bb_rate_gates holds
# the curves); refit and paste, never hand-tune. ``math.inf``: never ranked (see the module docstring).
#
# Owner-accepted exceptions (owner decision, 2026-11-09): three WNBA gates stay ranked although on the
# unseen 2023-2025 seasons they are under a third signal (holdout r = gate / (gate + holdout M)): team
# ``pace`` 7 games (0.331), on/off ``off_rtg_on`` 610 (0.267) and ``off_rtg_diff`` 720 (0.314).
# Re-measured season by season on the current releases (possessions PIPELINE_VERSION 7), no data or
# source defect drives them: the gates refit to the same values, on-court possessions tie out to the
# team's, no span season predates 2015 (the two-halves era), and the team off_rtg of both sources
# (ESPN box, stats.nba.com possessions) drops on 2023-2025 as off_rtg_on does. Pace's dip is 2025
# alone (13 teams). tests/tools/test_fit_rate_gates.py pins exactly these (ACCEPTED_LOW_HOLDOUT);
# evidence in sportsdataverse-py #PRNUM and docs/architecture/model-lineage.md.
#: Level -> team / lineup rate -> the smallest ``<m>_n`` it is ranked on (``net_rtg``: poss +
#: opp_poss; ``pace``: the clock in regulation games; ``orb_pct``: rebound chances; ``efg_pct`` /
#: ``ft_rate``: FGA; ``fg3_pct``: 3PA).
TEAM_MIN_N: dict[str, dict[str, float]] = {
    "nba": {
        "off_rtg": 1300, "def_rtg": 2000, "net_rtg": 2000, "pace": 5, "efg_pct": 1200, "tov_pct": 1400,
        "orb_pct": 410, "ft_rate": 1300, "opp_efg_pct": 1900, "opp_tov_pct": 960, "opp_orb_pct": 1200,
        "opp_ft_rate": 1500, "fg3_pct": 1800, "opp_fg3_pct": math.inf,
    },
    "wnba": {
        "off_rtg": 660, "def_rtg": 810, "net_rtg": 840, "pace": 7, "efg_pct": 750, "tov_pct": 1100,
        "orb_pct": 220, "ft_rate": 720, "opp_efg_pct": 1300, "opp_tov_pct": 350, "opp_orb_pct": 860,
        "opp_ft_rate": 880, "fg3_pct": math.inf, "opp_fg3_pct": math.inf,
    },
    "mbb": {
        "off_rtg": 270, "def_rtg": 460, "net_rtg": 430, "pace": 4, "efg_pct": 500, "tov_pct": 450,
        "orb_pct": 160, "ft_rate": 530, "opp_efg_pct": 630, "opp_tov_pct": 340, "opp_orb_pct": 330,
        "opp_ft_rate": 450, "fg3_pct": math.inf, "opp_fg3_pct": math.inf,
    },
    "wbb": {
        "off_rtg": 130, "def_rtg": 470, "net_rtg": 360, "pace": 3, "efg_pct": 230, "tov_pct": 210,
        "orb_pct": 160, "ft_rate": 550, "opp_efg_pct": 600, "opp_tov_pct": 300, "opp_orb_pct": 240,
        "opp_ft_rate": 420, "fg3_pct": 350, "opp_fg3_pct": math.inf,
    },
}  # fmt: skip
#: Level -> on/off rating -> the smallest ``<m>_n`` it is ranked on (``_on``: the on-court possessions
#: of that side, both for net; ``_diff``: the thinner of on and off).
ON_OFF_MIN_N: dict[str, dict[str, float]] = {
    "nba": {"off_rtg_on": 1700, "def_rtg_on": 1600, "net_rtg_on": 3200,
            "off_rtg_diff": 2400, "def_rtg_diff": 3400, "net_rtg_diff": 7900},
    "wnba": {"off_rtg_on": 610, "def_rtg_on": 1200, "net_rtg_on": 1600,
             "off_rtg_diff": 720, "def_rtg_diff": math.inf, "net_rtg_diff": 2600},
    "mbb": {"off_rtg_on": 530, "def_rtg_on": 750, "net_rtg_on": 1100,
            "off_rtg_diff": math.inf, "def_rtg_diff": math.inf, "net_rtg_diff": math.inf},
    "wbb": {"off_rtg_on": 240, "def_rtg_on": 590, "net_rtg_on": 600,
            "off_rtg_diff": math.inf, "def_rtg_diff": math.inf, "net_rtg_diff": math.inf},
}  # fmt: skip


def _gates(table: Mapping[str, Mapping[str, float]], level: str | None, metrics: Iterable[str]) -> dict[str, float]:
    if level is None:
        return {}
    if level not in table:
        raise KeyError(f"no sample gates for level {level!r}; expected one of {sorted(table)}")
    return {m: table[level][m] for m in metrics if m in table[level]}


def team_gates(level: str | None, metrics: Iterable[str]) -> dict[str, float]:
    """The :data:`TEAM_MIN_N` gates of ``level`` for the ``metrics`` that have one (``None``: no gates).

    Args:
        level: ``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``, or ``None`` for no sample gate.
        metrics: The ranked metrics of the table.

    Returns:
        Metric -> the smallest ``<m>_n`` it is ranked on, for the metrics with a gate.

    Raises:
        KeyError: If ``level`` is not one of :data:`LEVELS`.

    Example:
        Quick start::

            from sportsdataverse._common.rate_gates import team_gates
            team_gates("nba", ["off_rtg", "efg_pct"])
    """
    return _gates(TEAM_MIN_N, level, metrics)


def on_off_gates(level: str | None, metrics: Iterable[str]) -> dict[str, float]:
    """The :data:`ON_OFF_MIN_N` gates of ``level`` for the ``metrics`` that have one (``None``: no gates).

    Args:
        level: ``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``, or ``None`` for no sample gate.
        metrics: The ranked on/off ratings.

    Returns:
        Metric -> the smallest ``<m>_n`` it is ranked on, for the metrics with a gate.

    Raises:
        KeyError: If ``level`` is not one of :data:`LEVELS`.

    Example:
        Quick start::

            from sportsdataverse._common.rate_gates import on_off_gates
            on_off_gates("wbb", ["net_rtg_on", "net_rtg_diff"])
    """
    return _gates(ON_OFF_MIN_N, level, metrics)
