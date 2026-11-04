"""Holdout gates for the men's / women's college in-game WP booster (BB-D25).

The refit (``tools/models/mbb_prediction/train_in_game_wp.py``) is the NBA / WNBA refit's contract
(#1075) on college: train 2022, 2023, 2025; holdout 2026; confirmation season 2024 (the pregame and
decile gate season, in neither). Each gate is checked on the shipped booster and, where the pre-refit
booster (``baseline_in_game_wp/*_eb4207476e.ubj``, the bundled models at eb4207476e) failed it, shown to
fail there too, so the test can go red. The holdout is the ``espn_<lg>_pbp`` releases scored through
``score_win_prob`` (the game page's HFA prior: the college releases carry no lines).

Observed values (2026-11-04, ``in_game_wp_<lg>_holdout.json``): see the parametrize comments.
Gates are those values with the margin stated at each assert, never lowered. The full-season
Brier / log-loss / calibration gates need the release download and run under ``SDV_PY_LIVE_TESTS=1``.
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

from sportsdataverse.mbb import mbb_game_predict  # noqa: E402
from sportsdataverse.nba.nba_game_predict import _IN_GAME_FEATURES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools" / "models" / "mbb_prediction" / "train_in_game_wp.py"
LEAGUES = {"mbb": "mens", "wbb": "womens"}
HOLDOUT, CONFIRMATION = 2026, 2024
# the rated prior rates a season with one zero-turnover box score; the drop warning is asserted in
# tests/mbb/test_mbb_team_ratings.py
pytestmark = pytest.mark.filterwarnings(r"ignore:raw_game_efficiency\x3a dropped:UserWarning")


@pytest.fixture(scope="module")
def fit() -> Any:
    """The fitting script, imported (not re-implemented) so the gates and the fit share one metric."""
    spec = importlib.util.spec_from_file_location("train_college_in_game_wp", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _shipped(lg: str) -> Any:
    mbb_game_predict._load_wp_artifact.cache_clear()
    return mbb_game_predict._load_wp_artifact(LEAGUES[lg])["booster"]


def _baseline(lg: str) -> Any:
    b = xgb.Booster()
    path = ROOT / "tests" / "fixtures" / f"{lg}_prediction" / "baseline_in_game_wp" / f"{lg}_in_game_wp_eb4207476e.ubj"
    b.load_model(bytearray(path.read_bytes()))
    return b


def _report(lg: str) -> dict[str, Any]:
    return json.loads((SCRIPT.parent / f"in_game_wp_{lg}_holdout.json").read_text(encoding="utf-8"))


def _late(fit: Any, lg: str, model: Any) -> dict[str, float]:
    rows = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_{HOLDOUT}.parquet")
    return fit.shared.late_game_metrics(fit.serve(lg, rows, model), fit.game_regulation_periods(lg, season=HOLDOUT))


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_shipped_booster_lineage(lg: str) -> None:
    """The bundled booster names its fitting script; train, holdout and confirmation seasons are disjoint."""
    b = _shipped(lg)
    assert b.attr("fitting_script") == "tools/models/mbb_prediction/train_in_game_wp.py"
    train = {int(s) for s in b.attr("train_seasons").split(",")}
    holdout = int(b.attr("holdout_season"))
    report = _report(lg)
    assert holdout == HOLDOUT == report["holdout_season"] and report["confirmation_season"] == CONFIRMATION
    assert not train & {HOLDOUT, CONFIRMATION} and set(report["train_seasons"]) == train
    assert b.feature_names == report["features"] == _IN_GAME_FEATURES


@pytest.mark.parametrize(("lg", "baseline_violations"), [("mbb", 0), ("wbb", 94)])
def test_final_minute_grid_moves_toward_the_leader(fit: Any, lg: str, baseline_violations: int) -> None:
    """At a fixed lead / prior / possession, WP never moves away from the leader as 60 s runs to 0.1 s."""
    # structural (monotone constraint + the clock folded into margin_per_sqrt_sec inside the minute): 0.
    # The pre-refit women's booster moved away from the leader at 94 grid steps; the men's at none.
    assert fit.shared.grid_monotone_violations(_shipped(lg)) == 0
    assert fit.shared.grid_monotone_violations(_baseline(lg)) == baseline_violations


@pytest.mark.parametrize(
    ("lg", "floor_at_horn", "min_rise"),
    # observed (refit, home +1, prior 0.55, the trailing team's ball): WP at 0.1 s MBB 0.970 / WBB 0.979;
    # rise over the final minute MBB 0.278 / WBB 0.345. Floors: observed less 0.03 (horn) / 0.06 (rise).
    # Baseline: 0.681 / 0.695 at 0.1 s, a rise of 0.028 / 0.025, so the horn settle was a ~0.3 step.
    [("mbb", 0.94, 0.21), ("wbb", 0.94, 0.28)],
)
def test_one_point_lead_converges_at_the_horn(lg: str, floor_at_horn: float, min_rise: float) -> None:
    def wp(booster: Any) -> np.ndarray:
        secs = [60.0, 30.0, 10.0, 5.0, 1.0, 0.1]
        f = pl.DataFrame(
            {
                "start_game_seconds_remaining": secs,
                "home_score": [71] * 6,
                "away_score": [70] * 6,
                "team_id": ["B"] * 6,  # the trailing team has the ball
                "home_team_id": ["A"] * 6,
            }
        )
        x = mbb_game_predict.in_game_features(f, 0.55).select(booster.feature_names).to_numpy()
        return booster.inplace_predict(x)

    new, old = wp(_shipped(lg)), wp(_baseline(lg))
    assert new[-1] >= floor_at_horn and new[-1] - new[0] >= min_rise, new
    assert old[-1] < 0.70 and old[-1] - old[0] < 0.05, old  # the defect this gate exists for


@pytest.mark.parametrize(
    ("lg", "games", "min_toward"),
    # late-game fixture (every 2026 holdout game), refit: horn share MBB 0.00016 / WBB 0.0; toward share
    # MBB 0.385 / WBB 0.333; away share 0 / 0. Baseline: horn 0.066 / 0.047, toward 0.283 / 0.317, away 0 /
    # 0.045. Gates: horn share <= 0.005; toward >= observed less 0.03 (above both baselines); away == 0.
    [("mbb", 6275, 0.355), ("wbb", 6011, 0.32)],
)
def test_holdout_late_game_horn_jump_and_monotonicity(fit: Any, lg: str, games: int, min_toward: float) -> None:
    new = _late(fit, lg, _shipped(lg))
    assert new["games"] == games
    assert new["horn_jump_share"] <= 0.005, new
    assert new["final_minute_away_share"] == 0.0, new
    assert new["final_minute_toward_share"] >= min_toward, new
    old = _late(fit, lg, _baseline(lg))
    assert old["horn_jump_share"] > 0.04 and old["final_minute_toward_share"] < min_toward, old


@pytest.mark.parametrize("lg", sorted(LEAGUES))
@pytest.mark.parametrize("season", ["holdout", "confirmation"])
@pytest.mark.parametrize("prior", ["hfa_prior", "rated_prior"])
def test_report_refit_beats_the_shipped_booster_and_the_naive_model(lg: str, season: str, prior: str) -> None:
    """The committed holdout report: lowest Brier and log loss of the three, for each season and serving prior."""
    cell = _report(lg)[season][prior]
    for metric in ("brier", "log_loss"):
        assert cell["refit"][metric] < min(cell["baseline"][metric], cell["naive"][metric]), (metric, cell)
    assert cell["refit"]["horn_jump_share"] <= 0.002 and cell["refit"]["grid_violations"] == 0, cell["refit"]


@pytest.mark.skipif(os.environ.get("SDV_PY_LIVE_TESTS") != "1", reason="downloads the holdout pbp release")
@pytest.mark.parametrize(
    ("lg", "prior", "max_brier", "max_log_loss", "slope_band", "intercept_band"),
    # observed refit, 2026 holdout (in_game_wp_<lg>_holdout.json): Brier / log loss / slope / intercept
    #   MBB hfa   0.13270 / 0.40022 / 0.991 / -0.061 (baseline 0.13322 / 0.40240, naive 0.13409 / 0.41110)
    #   MBB rated 0.12006 / 0.36765 / 0.956 / +0.058 (baseline 0.12166 / 0.37401, naive 0.12237 / 0.38672)
    #   WBB hfa   0.12037 / 0.36430 / 1.093 / -0.037 (baseline 0.12117 / 0.36772, naive 0.12049 / 0.36676)
    #   WBB rated 0.10129 / 0.31310 / 1.011 / -0.009 (baseline 0.10393 / 0.32556, naive 0.10158 / 0.32044)
    # Gates: Brier observed + 0.0002 and log loss + 0.0005 (each below the baseline's); slope +- 0.03,
    # intercept +- 0.05.
    [
        ("mbb", "hfa_prior", 0.1329, 0.4007, (0.961, 1.021), (-0.111, -0.011)),
        ("mbb", "rated_prior", 0.1203, 0.3682, (0.926, 0.986), (0.008, 0.108)),
        ("wbb", "hfa_prior", 0.1206, 0.3648, (1.063, 1.123), (-0.087, 0.013)),
        ("wbb", "rated_prior", 0.1015, 0.3136, (0.981, 1.041), (-0.059, 0.041)),
    ],
)
def test_holdout_brier_log_loss_and_calibration(
    fit: Any,
    lg: str,
    prior: str,
    max_brier: float,
    max_log_loss: float,
    slope_band: tuple[float, float],
    intercept_band: tuple[float, float],
) -> None:
    priors = fit._priors(lg, HOLDOUT) if prior == "rated_prior" else None
    m = fit.holdout_metrics(lg, fit._load_pbp(lg, HOLDOUT), _shipped(lg), priors)
    assert m["brier"] <= max_brier and m["log_loss"] <= max_log_loss, m
    assert slope_band[0] <= m["calibration_slope"] <= slope_band[1], m
    assert intercept_band[0] <= m["calibration_intercept"] <= intercept_band[1], m
