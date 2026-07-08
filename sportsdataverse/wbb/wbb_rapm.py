"""Women's college basketball RAPM (regularized adjusted plus-minus) priors + player context.

Thin shim over :mod:`sportsdataverse.mbb.mbb_rapm` -- the faithful port of
hoop-explorer's ``RapmUtils``
(`Alex-At-Home/cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_
``src/utils/stats/RapmUtils.ts``). The RAPM math (prior construction, the
possession-weighted design-matrix build, the ridge-regression solve, the
adaptive-lambda orchestration, the player write-back, and the collinearity
diagnostic) is entirely league-agnostic -- it operates on the same
``LineupStatSet``-shaped dicts regardless of whether the underlying
Elasticsearch buckets came from the men's or women's college basketball
index. This module re-exports the mbb core functions, ``TypedDict`` types,
and constants **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``RapmUtils.ts`` is upstream-licensed under Apache License, Version 2.0; see
the full attribution (copyright notice, upstream URL, what was derived) in
the ``sportsdataverse.mbb.mbb_rapm`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_rapm import build_priors

        priors = build_priors({}, {}, 100.0, ["Wiggins, Aaron"], -1)
        print(priors["players_weak"][0])

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_rapm import (
    AFFECTED_PARTIAL_FIELDNAMES,
    DEFAULT_RAPM_CONFIG,
    ON_OFF_REPORT_REPLACEMENT_KEYS,
    RapmConfig,
    RapmPlayerContext,
    RapmPreProcDiagnostics,
    RapmPriorInfo,
    RapmProcessingInputs,
    apply_weak_priors,
    build_player_context,
    build_priors,
    build_weak_prior_from_rapm,
    calc_collinearity_diag,
    calc_lineup_outputs,
    calc_player_weights,
    calc_slow_pseudo_inverse,
    calculate_predicted_out,
    calculate_rapm,
    calculate_residual_error,
    calculate_sd_rapm,
    inject_rapm_into_players,
    pick_ridge_regression,
    slow_regression,
)

__all__ = [
    "RapmConfig",
    "DEFAULT_RAPM_CONFIG",
    "RapmPriorInfo",
    "RapmPlayerContext",
    "build_priors",
    "build_player_context",
    "calc_player_weights",
    "calc_lineup_outputs",
    "slow_regression",
    "calculate_rapm",
    "calc_slow_pseudo_inverse",
    "calculate_predicted_out",
    "calculate_residual_error",
    "calculate_sd_rapm",
    "RapmProcessingInputs",
    "build_weak_prior_from_rapm",
    "apply_weak_priors",
    "pick_ridge_regression",
    "AFFECTED_PARTIAL_FIELDNAMES",
    "ON_OFF_REPORT_REPLACEMENT_KEYS",
    "inject_rapm_into_players",
    "RapmPreProcDiagnostics",
    "calc_collinearity_diag",
]
