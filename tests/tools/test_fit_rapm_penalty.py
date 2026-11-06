"""The plain-RAPM penalty fitter (``tools/fit_rapm_penalty.py``) on the committed WNBA possessions.

``tests/fixtures/bb_factor_rapm`` holds the whole 2024 and 2025 ``wnba_stats_possessions`` and
``_schedules`` releases (README there). The frozen values come from the release assets of every season
(the fitter's ``curves`` stage, network); here its math is checked against sklearn and its pooled
pick is re-run on the two committed seasons, where it lands on the frozen WNBA value.
"""

from __future__ import annotations

import importlib.util
from functools import cache
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.linear_model import RidgeCV

from sportsdataverse.nba.nba_player_factor_rapm import nba_factor_rapm_rows
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, RAPM_PENALTY, build_rapm_design, ridge_rapm_coef

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_rapm_penalty", _ROOT / "tools" / "fit_rapm_penalty.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)  # type: ignore[union-attr]
FIX = _ROOT / "tests" / "fixtures" / "bb_factor_rapm"


@cache
def _season(season: int) -> tuple:
    def read(name: str) -> pl.DataFrame:
        return pl.read_parquet(FIX / f"wnba_{name}_{season}.parquet")

    rows = nba_factor_rapm_rows(read("possessions"), read("schedule"), "regular-season")
    X, y, _ = build_rapm_design(rows)
    return X, y, fit.season_curves(X, y, rows["game_id"].rank("dense").to_numpy())


def test_the_old_grid_is_on_the_fit_grid() -> None:
    old = np.isin(np.round(fit.GRID, 6), np.round(DEFAULT_RAPM_ALPHAS, 6))
    assert old.sum() == DEFAULT_RAPM_ALPHAS.size  # the old rule's pick is read off the same curve
    assert {fit.GRID[np.isclose(fit.GRID, a)][0] for a in RAPM_PENALTY.values()} == set(RAPM_PENALTY.values())


def test_curves_are_sklearn_and_the_exact_ridge() -> None:
    X, y, c = _season(2024)
    old = np.isin(np.round(fit.GRID, 6), np.round(DEFAULT_RAPM_ALPHAS, 6))
    ref = RidgeCV(alphas=DEFAULT_RAPM_ALPHAS, store_cv_results=True).fit(X, y)
    # observed 1.9e-14: the eigenbasis leave-one-out is sklearn's
    assert np.abs(c["loo_mse"][old] / ref.cv_results_.mean(axis=0) - 1).max() < 1e-10
    k = int(np.argmin(np.abs(fit.GRID - RAPM_PENALTY["10"])))
    assert np.abs(c["coef"][:, k] - ridge_rapm_coef(X, y, RAPM_PENALTY["10"])).max() < 1e-10
    assert c["cv_sse"].shape == (fit.FOLDS, fit.GRID.size) and c["cv_n"].sum() == X.shape[0]


def test_pooled_pick_is_the_frozen_wnba_value() -> None:
    # Observed: grouped CV picks 1,638 for 2024 and 2,276 for 2025 on their own (one season's curve
    # is too flat to fix the penalty), and 1,931 pooled over both, the frozen value (pooled over 29).
    picks = [fit._pick(_season(s)[2]["cv_sse"].sum(axis=0)) for s in (2024, 2025)]
    assert np.round(picks).tolist() == [1638, 2276]
    pooled = fit._pick(sum(_season(s)[2]["cv_sse"].sum(axis=0) for s in (2024, 2025)))
    assert pooled == RAPM_PENALTY["10"]
