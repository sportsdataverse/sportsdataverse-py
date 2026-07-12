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
regression. Should stats.wnba.com begin publishing combine breakdowns, a
future revision would need a new combine-feature fit script analogous to
``dev/nba_draft/fit_draft_model.py`` (the NBA core), not the slot-only one
below.

**Phase 5 re-fit (2026-07-11, genuine WNBA data, replacing the T3.4 seeds):**
``dev/wnba_draft/`` captures the real WNBA draft-history + career corpus
(``capture_corpus.py``, 1201 draftees across 29 classes 1997-2025) and fits
all four bundled artifacts on it (``fit_aging_curve.py``, ``fit_draft_model.py``,
``fit_availability.py``, ``fit_rookie_residual.py``) -- see each script's module
docstring and ``tests/wnba/test_wnba_draft_backtest.py`` for the resulting
oracle gates and observed holdout numbers. The one deliberate, permanent
exception is ``draft_prob``: this corpus has no undrafted/invitee negative
class (see above), so it stays a documented constant rather than a faked
classifier fit (see ``fit_draft_model.py``).
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
