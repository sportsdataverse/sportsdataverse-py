"""Women's college basketball lineup aggregation + on/off splits.

Thin shim over :mod:`sportsdataverse.mbb.mbb_lineup_stats` -- the faithful
port of hoop-explorer's ``LineupUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/LineupUtils.ts``). The lineup-aggregation math (possession-
weighted merging, on/off partitioning, replacement diffs) is entirely
league-agnostic -- it operates on the same ``LineupStatSet`` shape regardless
of whether the underlying Elasticsearch buckets came from the men's or
women's college basketball index. This module re-exports the mbb core
functions **by reference** (not a copy) so ``sportsdataverse.wbb`` callers
get the identical implementation the mbb side uses, with no duplicated
logic to drift out of sync.

``LineupUtils.ts`` is upstream-licensed under Apache License, Version 2.0;
see the full attribution (copyright notice, upstream URL, what was derived)
in the ``sportsdataverse.mbb.mbb_lineup_stats`` module docstring and in
``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_lineup_stats import calculate_aggregated_lineup_stats

        buckets = raw_response["responses"][0]["aggregations"]["lineups"]["buckets"]
        team_info = calculate_aggregated_lineup_stats(buckets)
        print(team_info["off_ppp"]["value"], team_info["off_poss"]["value"])

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_lineup_stats import (
    build_efficiency_margins,
    calculate_aggregated_lineup_stats,
    complete_weighted_avg,
    get_stats_diff,
    lineup_to_team_report,
    weighted_avg,
)

__all__ = [
    "weighted_avg",
    "complete_weighted_avg",
    "calculate_aggregated_lineup_stats",
    "build_efficiency_margins",
    "lineup_to_team_report",
    "get_stats_diff",
]
