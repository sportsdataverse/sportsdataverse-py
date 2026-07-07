"""Women's college basketball positional classifier (LDA position-confidence model).

Thin shim over :mod:`sportsdataverse.mbb.mbb_positions` -- the faithful port of
hoop-explorer's ``PositionUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/PositionUtils.ts``). The classifier (the LDA constant
tables, the softmax confidence build, the height reweight, the decision-tree
position assignment, the lineup ordering, and the positional-aware search
filter) is entirely league-agnostic -- it operates on the same box-score
stat-bucket shape regardless of whether the underlying data came from the
men's or women's college basketball index. This module re-exports the mbb
core constants and functions **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation the mbb side
uses, with no duplicated logic to drift out of sync.

``PositionUtils.ts`` (and ``PositionalManualFixes.ts``) are upstream-licensed
under Apache License, Version 2.0; see the full attribution (copyright
notice, upstream URL, what was derived) in the
``sportsdataverse.mbb.mbb_positions`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_positions import build_position_confidences

        confs, diags = build_position_confidences(player_bucket)
        print(confs["pos_pg"])

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_positions import (
    ABSOLUTE_POSITION_FIXES,
    AVERAGE_SCORES_BY_POS,
    HEIGHT_MEAN_STDS,
    ID_TO_POSITION,
    POSITION_FEATURE_AVERAGES,
    POSITION_FEATURE_INIT,
    POSITION_FEATURE_WEIGHTS,
    RELATIVE_POSITION_FIXES,
    TRAD_POS_LIST,
    apply_relative_positional_overrides,
    build_position,
    build_position_confidences,
    build_positional_aware_filter,
    incorporate_height,
    order_lineup,
    pos_class_to_score,
    regress_shot_quality,
    test_positional_aware_filter,
    using_roster_pos,
)

__all__ = [
    "POSITION_FEATURE_INIT",
    "TRAD_POS_LIST",
    "POSITION_FEATURE_WEIGHTS",
    "POSITION_FEATURE_AVERAGES",
    "HEIGHT_MEAN_STDS",
    "AVERAGE_SCORES_BY_POS",
    "ID_TO_POSITION",
    "ABSOLUTE_POSITION_FIXES",
    "RELATIVE_POSITION_FIXES",
    "regress_shot_quality",
    "build_position_confidences",
    "incorporate_height",
    "build_position",
    "using_roster_pos",
    "pos_class_to_score",
    "order_lineup",
    "apply_relative_positional_overrides",
    "build_positional_aware_filter",
    "test_positional_aware_filter",
]
