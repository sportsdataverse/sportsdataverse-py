"""Final-minute gates for the NBA / WNBA in-game WP booster (BB-D28 finding 1).

The pre-refit boosters (``baseline_in_game_wp/*_3e967eb40d.ubj``, the bundled models at 3e967eb40d)
held home WP constant from 0.1 s to 60 s at a fixed margin (WNBA: home +2 with 0.1 s left = 0.279),
so ``score_win_prob``'s settle at the horn was a 30-60 point step. The refit
(``tools/models/nba_prediction/train_in_game_wp.py``) is gated here on the holdout seasons' real
late-game rows (NBA 2026, WNBA 2025, the ``espn_<lg>_pbp`` releases; the fixtures are the last two
minutes of regulation plus overtime that the fitting script writes). Every gate is checked on the
shipped booster AND shown to fail on the baseline, so the test can go red.

Observed values (2026-11-03, ``in_game_wp_<lg>_holdout.json``, baseline -> refit; full holdout):

| league | Brier | calibration slope | horn-jump share | final-minute toward / away |
|---|---|---|---|---|
| NBA 2026 | 0.15223 -> 0.14467 | 0.9158 -> 0.9264 | 0.1370 -> 0.0008 | 0.000 / 0.000 -> 0.437 / 0.000 |
| WNBA 2025 | 0.18958 -> 0.16454 | 1.1220 -> 0.9186 | 0.2917 -> 0.0000 | 0.000 / 0.000 -> 0.231 / 0.000 |

Gates are those observed values with the margin stated at each assert, never lowered. The full-season
Brier / slope gates need the release download and run under ``SDV_PY_LIVE_TESTS=1``.
"""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pytest

xgb = pytest.importorskip("xgboost")

from sportsdataverse.nba import nba_game_predict  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "nba_prediction"
SCRIPT = ROOT / "tools" / "models" / "nba_prediction" / "train_in_game_wp.py"
# league -> (league_id, holdout season)
LEAGUES = {"nba": ("00", 2026), "wnba": ("10", 2025)}


@pytest.fixture(scope="module")
def fit() -> Any:
    """The fitting script, imported (not re-implemented) so the gates and the fit share one metric."""
    spec = importlib.util.spec_from_file_location("train_in_game_wp", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _shipped(lg: str) -> Any:
    nba_game_predict._load_in_game_artifact.cache_clear()
    return nba_game_predict._load_in_game_artifact(LEAGUES[lg][0])["booster"]


def _baseline(lg: str) -> Any:
    b = xgb.Booster()
    b.load_model(bytearray((FIX / "baseline_in_game_wp" / f"{lg}_in_game_wp_3e967eb40d.ubj").read_bytes()))
    return b


def _late(fit: Any, lg: str, booster: Any) -> dict[str, float]:
    lid, season = LEAGUES[lg]
    rows = pl.read_parquet(FIX / f"{lg}_late_game_{season}.parquet")
    reg = fit.game_regulation_periods(lg, season=season)
    return fit.late_game_metrics(fit.serve(lid, rows, booster), reg)


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_shipped_booster_lineage(lg: str) -> None:
    """The bundled booster names its fitting script, and its train / holdout seasons are disjoint."""
    b = _shipped(lg)
    assert b.attr("fitting_script") == "tools/models/nba_prediction/train_in_game_wp.py"
    train = {int(s) for s in b.attr("train_seasons").split(",")}
    holdout = int(b.attr("holdout_season"))
    assert holdout == LEAGUES[lg][1] and holdout not in train
    report = json.loads((SCRIPT.parent / f"in_game_wp_{lg}_holdout.json").read_text(encoding="utf-8"))
    assert set(report["train_seasons"]) == train and report["holdout_season"] == holdout
    assert b.feature_names == report["features"] == nba_game_predict._IN_GAME_FEATURES


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_final_minute_grid_moves_toward_the_leader(fit: Any, lg: str) -> None:
    """At a fixed lead / prior / possession, WP never moves away from the leader as 60 s runs to 0.1 s."""
    # structural (monotone constraint + the clock folded into margin_per_sqrt_sec inside the minute): 0
    assert fit.grid_monotone_violations(_shipped(lg)) == 0


@pytest.mark.parametrize(
    ("lg", "floor_at_horn", "min_rise"),
    # observed (refit, home +1, prior 0.55, either ball): WP at 0.1 s NBA 0.981 / WNBA 0.962; rise over the
    # final minute NBA 0.326 / WNBA 0.313. Floors: observed less 0.03 (horn) / 0.06 (rise). Baseline: 0 rise.
    [("nba", 0.95, 0.26), ("wnba", 0.93, 0.25)],
)
def test_one_point_lead_converges_at_the_horn(lg: str, floor_at_horn: float, min_rise: float) -> None:
    def wp(booster: Any) -> np.ndarray:
        secs = [60.0, 30.0, 10.0, 5.0, 1.0, 0.1]
        f = pl.DataFrame(
            {
                "start_game_seconds_remaining": secs,
                "home_score": [101] * 6,
                "away_score": [100] * 6,
                "team_id": ["B"] * 6,  # the trailing team has the ball
                "home_team_id": ["A"] * 6,
            }
        )
        x = nba_game_predict.in_game_features(f, 0.55).select(booster.feature_names).to_numpy()
        return booster.inplace_predict(x)

    new, old = wp(_shipped(lg)), wp(_baseline(lg))
    assert new[-1] >= floor_at_horn and new[-1] - new[0] >= min_rise, new
    assert old[-1] - old[0] < 1e-6, old  # the defect this gate exists for: flat through the final minute


@pytest.mark.parametrize(
    ("lg", "max_horn_share", "min_toward"),
    # late-game fixture, refit: horn share NBA 0.0008 / WNBA 0.0; toward share NBA 0.437 / WNBA 0.231.
    # Gates: horn share <= 0.02 (baseline 0.137 / 0.292); toward >= observed less 0.08; away == 0.
    [("nba", 0.02, 0.35), ("wnba", 0.02, 0.15)],
)
def test_holdout_late_game_horn_jump_and_monotonicity(
    fit: Any, lg: str, max_horn_share: float, min_toward: float
) -> None:
    new = _late(fit, lg, _shipped(lg))
    assert new["games"] == {"nba": 1321, "wnba": 312}[lg]
    assert new["horn_jump_share"] <= max_horn_share, new
    assert new["final_minute_away_share"] == 0.0, new
    assert new["final_minute_toward_share"] >= min_toward, new
    old = _late(fit, lg, _baseline(lg))
    assert old["horn_jump_share"] > 0.10 and old["final_minute_toward_share"] == 0.0, old


@pytest.mark.skipif(os.environ.get("SDV_PY_LIVE_TESTS") != "1", reason="downloads the holdout pbp release")
@pytest.mark.parametrize(
    ("lg", "season", "max_brier", "slope_band"),
    # observed refit Brier NBA 2026 0.14467 / WNBA 2025 0.16454 (baseline 0.15223 / 0.18958): gate observed + 0.002.
    # slope: observed 0.9264 / 0.9186 +- 0.03, which also keeps |slope - 1| inside the baseline's (0.0842 / 0.1220).
    # WNBA 2026 is a confirmation season no fitting decision looked at (2025 chose 2024's inclusion in training):
    # baseline Brier 0.1729 / slope 0.598, refit 0.1439 / 0.994; same margins.
    [
        ("nba", 2026, 0.1466, (0.8964, 0.9564)),
        ("wnba", 2025, 0.1665, (0.8886, 0.9486)),
        ("wnba", 2026, 0.1459, (0.964, 1.024)),
    ],
)
def test_holdout_brier_and_calibration_slope(
    fit: Any, lg: str, season: int, max_brier: float, slope_band: tuple[float, float]
) -> None:
    lid = LEAGUES[lg][0]
    m = fit.holdout_metrics(lid, fit._load_pbp(lid, season), _shipped(lg))
    assert m["brier"] <= max_brier, m
    assert slope_band[0] <= m["calibration_slope"] <= slope_band[1], m
