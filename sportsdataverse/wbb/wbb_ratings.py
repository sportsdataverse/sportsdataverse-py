"""Women's college basketball individual ratings (Dean-Oliver ORtg/DRtg + Adj Rtg+ productivity).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ratings` -- the faithful port of
hoop-explorer's ``RatingUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/RatingUtils.ts``). The individual-rating math (points
produced, possession decomposition, SoS/usage-adjusted productivity) is
entirely league-agnostic -- it operates on the same ``LineupStatSet``-shaped
dicts regardless of whether the underlying Elasticsearch buckets came from
the men's or women's college basketball index. This module re-exports the
mbb core functions **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``RatingUtils.ts`` is upstream-licensed under Apache License, Version 2.0;
see the full attribution (copyright notice, upstream URL, what was derived)
in the ``sportsdataverse.mbb.mbb_ratings`` module docstring and in
``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ratings import build_o_rtg

        o_rtg, adj_o_rtg, _, _, diags = build_o_rtg(
            player, {}, {"total_off_to": {"value": 0}, "sum_total_off_to": {}},
            100.0, True, False,
        )
        print(o_rtg["value"], diags["oRtg"])

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ratings import (
    adjust_off_rating_stats,
    build_d_rtg,
    build_net_points,
    build_o_rtg,
    build_productivity,
)

__all__ = [
    "build_o_rtg",
    "build_d_rtg",
    "build_net_points",
    "adjust_off_rating_stats",
    "build_productivity",
]
