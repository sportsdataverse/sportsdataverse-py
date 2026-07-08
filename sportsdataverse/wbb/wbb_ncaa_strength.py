"""Women's college basketball KenPom-style strength-adjustment engine.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_strength` -- the faithful
port of hoop-explorer's ``buildStrengthAdjustedStats``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/bin/buildStrengthAdjustedStats.ts``): the iterative strength-of-
schedule + home-court-advantage adjustment solver for the four per-game
shooting rates (``efg``/``3p``/``2pmid``/``2prim``). The engine operates
purely on the loose, league-agnostic ``team_details`` JSON shape (team name,
conference, and a list of opponent-game stat rows) with no men's/women's
branching anywhere in the algorithm, so the same code serves both the men's
and women's college basketball index. This module re-exports the mbb core
functions **by reference** (not a copy) so ``sportsdataverse.wbb`` callers
get the identical implementation the mbb side uses, with no duplicated logic
to drift out of sync.

``buildStrengthAdjustedStats.ts`` is upstream-licensed under Apache License,
Version 2.0; see the full attribution (copyright notice, upstream URL, what
was derived) in the ``sportsdataverse.mbb.mbb_ncaa_strength`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_strength import build_strength_adjusted_stats

        result = build_strength_adjusted_stats(team_details)
        result.teams[0].adj["3p"]

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_strength import (
    IMBALANCE_MIN,
    MAX_ITERATIONS,
    STRENGTH_ADJUSTED_FIELDS,
    TOLERANCE,
    FieldAverage,
    IterationResult,
    PossessionSplits,
    StrengthAdjustedResult,
    TeamStrengthAdjusted,
    build_strength_adjusted_stats,
    compute_league_averages_from_per_game,
    compute_opponent_strengths,
    compute_possession_splits,
    field_keys,
    get_game_weight,
    get_per_game_raw,
    get_team_raw_from_per_game,
    run_iterative_adjustment_with_hca,
)

__all__ = [
    "STRENGTH_ADJUSTED_FIELDS",
    "MAX_ITERATIONS",
    "TOLERANCE",
    "IMBALANCE_MIN",
    "PossessionSplits",
    "FieldAverage",
    "TeamStrengthAdjusted",
    "StrengthAdjustedResult",
    "IterationResult",
    "field_keys",
    "get_per_game_raw",
    "get_game_weight",
    "compute_possession_splits",
    "compute_league_averages_from_per_game",
    "get_team_raw_from_per_game",
    "compute_opponent_strengths",
    "run_iterative_adjustment_with_hca",
    "build_strength_adjusted_stats",
]
