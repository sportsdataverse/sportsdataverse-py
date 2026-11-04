"""Holdout gates for the men's / women's college in-game WP booster (BB-D25).

The refit (``tools/models/mbb_prediction/train_in_game_wp.py``) is the NBA / WNBA refit's contract
(#1075) on college: train 2022, 2023, 2025; holdout 2026; confirmation season 2024 (the pregame and
decile gate season, in neither). Each gate is checked on the shipped booster and, where the pre-refit
booster (``baseline_in_game_wp/*_eb4207476e.ubj``, the bundled models at eb4207476e) failed it, shown to
fail there too, so the test can go red. The holdout is the ``espn_<lg>_pbp`` releases scored through
``score_win_prob`` with two priors: the game page's (HFA only, 0.5 at a neutral site: the college
releases carry no lines) and the release enrichment's weekly as-of rated prior.

The committed report (``in_game_wp_<lg>_holdout.json``) is tied to the shipped booster by its sha256;
its model comparisons are claimed only where the paired game-clustered bootstrap interval excludes 0.
The full-season gates need the release download and run under ``SDV_PY_LIVE_TESTS=1``: they re-score
the holdout and must reproduce the report.
"""

from __future__ import annotations

import hashlib
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


def _report(lg: str, kind: str = "holdout") -> dict[str, Any]:
    return json.loads((SCRIPT.parent / f"in_game_wp_{lg}_{kind}.json").read_text(encoding="utf-8"))


def _late(fit: Any, lg: str, model: Any) -> dict[str, float]:
    rows = pl.read_parquet(ROOT / "tests" / "fixtures" / f"{lg}_prediction" / f"{lg}_late_game_{HOLDOUT}.parquet")
    return fit.shared.late_game_metrics(fit.serve(lg, rows, model), fit.game_regulation_periods(lg, season=HOLDOUT))


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_shipped_booster_lineage(lg: str) -> None:
    """The bundled booster is the one the report scored; it names its script; its seasons are disjoint."""
    b = _shipped(lg)
    report = _report(lg)
    artifact = ROOT / "sportsdataverse" / "mbb" / "models" / f"{lg}_in_game_wp.ubj"
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == report["artifact_sha256"]
    assert b.num_boosted_rounds() == report["rounds"]
    assert b.attr("fitting_script") == "tools/models/mbb_prediction/train_in_game_wp.py"
    train = {int(s) for s in b.attr("train_seasons").split(",")}
    assert int(b.attr("holdout_season")) == HOLDOUT == report["holdout_season"]
    assert report["confirmation_season"] == CONFIRMATION
    assert not train & {HOLDOUT, CONFIRMATION} and set(report["train_seasons"]) == train
    assert b.feature_names == report["features"] == _IN_GAME_FEATURES


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_training_design_is_the_validation_pick_and_was_applied(fit: Any, lg: str) -> None:
    """The share / keep-unrated design is ``--validate``'s pick on 2025, and the fit applied it."""
    val = _report(lg, "validation")
    assert val["validation_season"] == 2025 and set(val["fit_seasons"]).isdisjoint({HOLDOUT, CONFIRMATION})
    best = min(val["grid"], key=lambda c: c["mean_log_loss"])
    assert val["best"] == {"hfa_prior_share": best["hfa_prior_share"], "keep_unrated": best["keep_unrated"]}
    # ties: the best cell's paired 95% interval against the cell includes 0; the simplest tie is picked
    ties = [c for c in val["grid"] if c is best or c["best_minus_this"]["ci95"][1] >= 0]
    assert [c["tied_with_best"] for c in val["grid"]] == [c in ties for c in val["grid"]]
    pick = min(ties, key=lambda c: (c["keep_unrated"], c["hfa_prior_share"]))
    assert val["pick"] == {"hfa_prior_share": pick["hfa_prior_share"], "keep_unrated": pick["keep_unrated"]}
    assert val["pick"] == {"hfa_prior_share": fit.HFA_PRIOR_SHARE[lg], "keep_unrated": fit.KEEP_UNRATED[lg]}
    report = _report(lg)
    assert (report["hfa_prior_share"], report["keep_unrated"]) == (fit.HFA_PRIOR_SHARE[lg], fit.KEEP_UNRATED[lg])
    for stats in report["train_stats"].values():
        unrated = stats["games"] - stats["rated_games"]
        assert unrated > 0  # the season's first days: the case the design is about
        assert stats["unrated_games_dropped"] == (0 if fit.KEEP_UNRATED[lg] else unrated)
        assert stats["hfa_swapped_games"] == round(fit.HFA_PRIOR_SHARE[lg] * stats["rated_games"])
    if lg == "mbb":  # the 2022 MBB release puts overtime plays on the game clock: the re-clock fired
        assert report["train_stats"]["2022"]["ot_rows_reclocked"] > 10_000


@pytest.mark.parametrize(("lg", "baseline_violations"), [("mbb", 0), ("wbb", 94)])
def test_final_minute_grid_moves_toward_the_leader(fit: Any, lg: str, baseline_violations: int) -> None:
    """At a fixed lead / prior / possession, WP never moves away from the leader as 60 s runs to 0.1 s.

    Structural (monotone constraints + the clock folded into margin_per_sqrt_sec inside the minute): 0.
    The pre-refit women's booster moved away from the leader at 94 grid steps. The men's had none, but the
    gate still bites there: the same men's fit without ``monotone_constraints`` has 58.
    """
    assert fit.shared.grid_monotone_violations(_shipped(lg)) == 0
    assert fit.shared.grid_monotone_violations(_baseline(lg)) == baseline_violations


# (floor at 0.1 s, minimum rise over the final minute). Observed (refit, home +1, prior 0.55, the trailing
# team's ball): at 0.1 s MBB 0.970 / WBB 0.980, rise 0.278 / 0.308. Floors: observed less 0.03 / 0.06.
ONE_POINT = {"mbb": (0.939, 0.217), "wbb": (0.949, 0.247)}


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_one_point_lead_converges_at_the_horn(lg: str) -> None:
    floor_at_horn, min_rise = ONE_POINT[lg]

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
    # the defect this gate exists for: 0.681 (MBB) / 0.695 (WBB) at 0.1 s, a ~0.03 rise, a ~0.3 horn step
    assert old[-1] < 0.70 and old[-1] - old[0] < 0.05, old


# (games, minimum toward-the-leader share) on the late-game fixture (every 2026 holdout game). Observed,
# refit: horn share MBB 0.0 / WBB 0.00017, toward 0.385 / 0.352, away 0 / 0. Baseline: horn 0.066 / 0.047,
# toward 0.283 / 0.317, away 0 / 0.045. Gates: horn share <= 0.005; away == 0; toward >= observed less 0.03.
LATE = {"mbb": (6275, 0.355), "wbb": (6011, 0.322)}


@pytest.mark.parametrize("lg", sorted(LEAGUES))
def test_holdout_late_game_horn_jump_and_monotonicity(fit: Any, lg: str) -> None:
    games, min_toward = LATE[lg]
    new = _late(fit, lg, _shipped(lg))
    assert new["games"] == games
    assert new["horn_jump_share"] <= 0.005, new
    assert new["final_minute_away_share"] == 0.0, new
    assert new["final_minute_toward_share"] >= min_toward, new
    old = _late(fit, lg, _baseline(lg))
    assert old["horn_jump_share"] > 0.04 and old["final_minute_toward_share"] < min_toward, old


# season x prior -> per comparator: "win" (the refit's paired 95% interval is below 0 for Brier and
# log loss) or "tie" (an interval includes 0). Read off the committed report; see its refit_vs_* blocks.
OUTCOMES: dict[tuple[str, str, str], dict[str, str]] = {
    ("mbb", "holdout", "hfa_prior"): {"baseline": "win", "naive": "win"},
    ("mbb", "holdout", "rated_prior"): {"baseline": "win", "naive": "win"},
    ("mbb", "confirmation", "hfa_prior"): {"baseline": "win", "naive": "win"},
    ("mbb", "confirmation", "rated_prior"): {"baseline": "win", "naive": "win"},
    # WBB vs the naive closed form: log loss is a win in every cell, Brier a tie with the rated prior
    ("wbb", "holdout", "hfa_prior"): {"baseline": "win", "naive": "win"},
    ("wbb", "holdout", "rated_prior"): {"baseline": "win", "naive": "tie"},
    ("wbb", "confirmation", "hfa_prior"): {"baseline": "win", "naive": "win"},
    ("wbb", "confirmation", "rated_prior"): {"baseline": "win", "naive": "tie"},
}


@pytest.mark.parametrize(("lg", "season", "prior"), sorted(OUTCOMES))
def test_report_comparisons_rest_on_the_paired_bootstrap(lg: str, season: str, prior: str) -> None:
    """A "win" needs both paired intervals below 0; a "tie" has one that includes 0. Never a loss."""
    cell = _report(lg)[season][prior]
    for other, outcome in OUTCOMES[(lg, season, prior)].items():
        d = cell[f"refit_vs_{other}"]
        uppers = [d[m]["ci95"][1] for m in ("brier", "log_loss")]
        lowers = [d[m]["ci95"][0] for m in ("brier", "log_loss")]
        assert all(lo < 0 for lo in lowers), (other, d)  # the refit is never significantly worse
        assert (all(u < 0 for u in uppers)) == (outcome == "win"), (other, outcome, d)
    assert cell["refit"]["horn_jump_share"] <= 0.002 and cell["refit"]["grid_violations"] == 0, cell["refit"]


# Fixed 2026-holdout ceilings, never raised: (Brier, log loss, |slope - 1|, |intercept|), each the observed
# refit value plus 0.0002 / 0.0005 / 0.03 / 0.05. Observed (in_game_wp_<lg>_holdout.json, refit):
#   MBB hfa   0.13245 / 0.39957 / slope 0.990 / intercept -0.001
#   MBB rated 0.12006 / 0.36765 / 0.956 / +0.058
#   WBB hfa   0.12052 / 0.36480 / 1.113 / +0.018
#   WBB rated 0.10146 / 0.31330 / 0.995 / -0.019
# WBB history (game-page prior): the d862d3e077 booster read 0.12037 / intercept -0.037; the f5e91fd935
# one (unrated games kept, the grid's best before ties were broken) 0.12070 / -0.129; this one (the simplest
# tie: unrated dropped) 0.12052 / +0.018.
FIXED_LIVE: dict[tuple[str, str], tuple[float, float, float, float]] = {
    ("mbb", "hfa_prior"): (0.13265, 0.40007, 0.040, 0.051),
    ("mbb", "rated_prior"): (0.12026, 0.36815, 0.074, 0.108),
    ("wbb", "hfa_prior"): (0.12072, 0.36530, 0.143, 0.068),
    ("wbb", "rated_prior"): (0.10166, 0.31380, 0.035, 0.070),
}


@pytest.mark.parametrize(("lg", "prior"), sorted(FIXED_LIVE))
def test_report_sits_under_the_fixed_ceilings(lg: str, prior: str) -> None:
    """A regenerated report cannot move the gates: its refit holdout must sit under the fixed ceilings."""
    m = _report(lg)["holdout"][prior]["refit"]
    max_brier, max_ll, max_slope_dev, max_int = FIXED_LIVE[(lg, prior)]
    assert m["brier"] <= max_brier and m["log_loss"] <= max_ll, m
    assert abs(m["calibration_slope"] - 1) <= max_slope_dev and abs(m["calibration_intercept"]) <= max_int, m


@pytest.mark.skipif(os.environ.get("SDV_PY_LIVE_TESTS") != "1", reason="downloads the holdout pbp release")
@pytest.mark.parametrize("lg", sorted(LEAGUES))
@pytest.mark.parametrize("prior", ["hfa_prior", "rated_prior"])
def test_holdout_reproduces_the_report(fit: Any, lg: str, prior: str) -> None:
    """Re-scored from the release, the shipped booster's holdout matches the committed report.

    Margins absorb release drift: Brier 0.0002 and log loss 0.0005 (each smaller than the refit's paired
    gain over the shipped booster in every cell); calibration slope and intercept may sit no further from
    ideal (1, 0) than the report's value plus 0.03 / 0.05; the same games are scored.
    """
    want = _report(lg)["holdout"][prior]["refit"]
    priors = fit._priors(lg, HOLDOUT) if prior == "rated_prior" else None
    m = fit.holdout_metrics(lg, fit._load_pbp(lg, HOLDOUT), _shipped(lg), priors)
    assert m["games_scored"] == want["games_scored"] and m["plays"] == want["plays"], m
    assert m["brier"] <= want["brier"] + 0.0002 and m["log_loss"] <= want["log_loss"] + 0.0005, m
    assert abs(m["calibration_slope"] - 1) <= abs(want["calibration_slope"] - 1) + 0.03, m
    assert abs(m["calibration_intercept"]) <= abs(want["calibration_intercept"]) + 0.05, m
    max_brier, max_ll, max_slope_dev, max_int = FIXED_LIVE[(lg, prior)]
    assert m["brier"] <= max_brier and m["log_loss"] <= max_ll, m
    assert abs(m["calibration_slope"] - 1) <= max_slope_dev and abs(m["calibration_intercept"]) <= max_int, m
