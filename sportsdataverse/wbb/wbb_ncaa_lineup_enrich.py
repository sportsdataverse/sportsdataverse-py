"""Women's college basketball NCAA lineup enrichment / stat-tree population.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_lineup_enrich` -- the
faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``LineupUtils.scala``: raw-events -> ``LineupEventStats`` stat-tree
population, scramble/transition tagging, assist pairing, the score-swap
fixup, and per-player event splitting. The enrichment logic is entirely
league-agnostic -- it operates purely on the already-league-agnostic
:mod:`~sportsdataverse.mbb.mbb_ncaa_models` stat-tree types and the
already-league-agnostic :mod:`~sportsdataverse.mbb.mbb_ncaa_possessions`
clump machinery, so the same code serves both the men's and women's college
basketball index. This module re-exports the mbb core functions **by
reference** (not a copy) so ``sportsdataverse.wbb`` callers get the
identical implementation the mbb side uses, with no duplicated logic to
drift out of sync.

``LineupUtils.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_lineup_enrich`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_lineup_enrich import enrich_lineup

        enriched = enrich_lineup(lineup)
        enriched.team_stats.pts

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_lineup_enrich import (
    add_stats_to_lineups,
    create_player_events,
    enrich_lineup,
    enrich_stats,
    ensure_ev_uniqueness,
    fix_possible_score_swap_bug,
    is_end_of_game_fouling_vs_fastbreak,
    is_scramble,
    is_transition,
    sum_event_stats,
    sum_shot_infos,
)

__all__ = [
    "enrich_lineup",
    "add_stats_to_lineups",
    "fix_possible_score_swap_bug",
    "enrich_stats",
    "ensure_ev_uniqueness",
    "is_scramble",
    "is_end_of_game_fouling_vs_fastbreak",
    "is_transition",
    "create_player_events",
    "sum_event_stats",
    "sum_shot_infos",
]
