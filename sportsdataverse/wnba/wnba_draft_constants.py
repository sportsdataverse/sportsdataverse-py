"""WNBA by-reference constants shim for the draft/projection spine (T3.4).

The **math** (fitters, metrics, career-value formula, combine-feature
builder) lives once in :mod:`sportsdataverse.nba.nba_draft_constants` and is
re-exported here by reference -- this module supplies no separate algorithm
code, only the women's fitted constants (already registered under
``LEAGUE_CONSTANTS["wnba"]`` in the nba module) and documents the WNBA
coverage caveat below.

**Coverage caveat (WNBA combine data):** live capture (2026-07-08) confirmed
``wnba_stats_draftcombinestats`` returns **0 rows for every WNBA season**
tried (2019-2024) -- stats.wnba.com does not currently publish the
combine-measurement breakdown that stats.nba.com does (no anthro/drill/
spot-shooting/non-stationary-shooting endpoints exist for WNBA at all, and
even the one combine wrapper that *is* generated, ``draftcombinestats``, is
empty). This is **more reduced** than the design doc's original "reduced
combine" expectation (§8: "only draftcombinestats + drafthistory exist") --
in practice there is **no usable WNBA combine signal** at all right now.

Consequently ``wnba_draft_model`` (see ``wnba_draft_model.py``) cannot run
the NBA-side combine-measurement regression. It falls back to
``wnba_stats_drafthistory`` (draft slot) as its feature, which is still a
meaningful, honest, and correctly-scoped signal (draft position is a
real, pre-career-value, non-leaking feature) -- just not a physical-testing
regression. Should stats.wnba.com begin publishing combine breakdowns, the
women's artifact fit in ``dev/nba_draft/fit_draft_model.py --league wnba``
should be re-run with the richer feature set.
"""

from __future__ import annotations

from sportsdataverse.nba.nba_draft_constants import (  # noqa: F401
    BOX_VALUE_FEATURES,
    COMBINE_FEATURES,
    LEAGUE_CONSTANTS,
    LeagueConstants,
    as_of_class_split,
    auc,
    box_value_per100,
    build_combine_features,
    calibration_table,
    career_value_from_seasons,
    get_constants,
    logistic_fit_irls,
    mae,
    ridge_fit,
    spearman_corr,
)

__all__ = [
    "BOX_VALUE_FEATURES",
    "COMBINE_FEATURES",
    "LEAGUE_CONSTANTS",
    "LeagueConstants",
    "as_of_class_split",
    "auc",
    "box_value_per100",
    "build_combine_features",
    "calibration_table",
    "career_value_from_seasons",
    "get_constants",
    "logistic_fit_irls",
    "mae",
    "ridge_fit",
    "spearman_corr",
]
