"""Women's college basketball 3P luck-adjustment engine (regress-to-baseline shooting luck).

Thin shim over :mod:`sportsdataverse.mbb.mbb_luck` -- the faithful port of
hoop-explorer's ``LuckUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/LuckUtils.ts``). The luck-regression math (Bayesian shrink
of sample 3P% toward a baseline, eFG%/PPP propagation, the mutate-in-place
``inject_luck`` application glue) is entirely league-agnostic -- it operates
on the same ``LineupStatSet``-shaped dicts regardless of whether the
underlying Elasticsearch buckets came from the men's or women's college
basketball index. This module re-exports the mbb core functions and the
``LUCK_AFFECTED_FIELDS`` constant **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation the mbb
side uses, with no duplicated logic to drift out of sync.

``LuckUtils.ts`` is upstream-licensed under Apache License, Version 2.0;
see the full attribution (copyright notice, upstream URL, what was derived)
in the ``sportsdataverse.mbb.mbb_luck`` module docstring and in
``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_luck import calc_off_team_luck_adj

        diags = calc_off_team_luck_adj(
            sample_team_on, sample_players_on, base_team, base_players_map, 100.0,
        )
        print(diags["deltaOffAdjEff"])

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_luck import (
    LUCK_AFFECTED_FIELDS,
    build_3p_shot_info,
    build_adjusted_3p,
    build_exp_3p,
    calc_def_player_luck_adj,
    calc_def_team_luck_adj,
    calc_off_player_luck_adj,
    calc_off_team_luck_adj,
    inject_luck,
)

__all__ = [
    "LUCK_AFFECTED_FIELDS",
    "build_exp_3p",
    "build_adjusted_3p",
    "build_3p_shot_info",
    "calc_off_team_luck_adj",
    "calc_off_player_luck_adj",
    "calc_def_team_luck_adj",
    "calc_def_player_luck_adj",
    "inject_luck",
]
