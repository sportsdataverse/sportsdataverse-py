"""Women's college basketball NCAA possession calculator.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_possessions` -- the
faithful port of hoop-explorer's ``cbb-explorer``
(`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_)
``PossessionUtils.scala``. The concurrent-event batching, the per-clump
possession-fragment algorithm, and the lineup-assignment / balancing pass are
entirely league-agnostic -- they operate on the same NCAA raw-event shape
regardless of whether the underlying data came from the men's or women's
college basketball index. This module re-exports the mbb core types and
functions **by reference** (not a copy) so ``sportsdataverse.wbb`` callers
get the identical implementation the mbb side uses, with no duplicated logic
to drift out of sync.

``PossessionUtils.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_possessions`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_possessions import calculate_possessions

        lineups = calculate_possessions(raw_events)
        print(len(lineups))

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_possessions import (
    ConcurrentClump,
    PossState,
    assign_to_right_lineup,
    calculate_possessions,
    calculate_possessions_by_event,
    calculate_stats,
    concurrent_event_handler,
    count_matching,
    lineup_as_raw_clumps,
    lineup_balancer,
    lineup_fixer,
)

__all__ = [
    "ConcurrentClump",
    "PossState",
    "lineup_as_raw_clumps",
    "concurrent_event_handler",
    "count_matching",
    "calculate_stats",
    "calculate_possessions_by_event",
    "calculate_possessions",
    "lineup_balancer",
    "lineup_fixer",
    "assign_to_right_lineup",
]
