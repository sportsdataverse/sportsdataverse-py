"""The QB layer's one-time pass rule (``tools/models/nfl_prediction/score_qb_layer.py``), offline.

The scorer runs once on 2026 weeks 5-18, so every threshold is pinned here on each side of its boundary.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "score_qb_layer", _ROOT / "tools/models/nfl_prediction/score_qb_layer.py"
)
S = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(S)  # type: ignore[union-attr]
_rspec = importlib.util.spec_from_file_location("retire", _ROOT / "tools/models/lifecycle/retire.py")
R = importlib.util.module_from_spec(_rspec)
_rspec.loader.exec_module(R)  # type: ignore[union-attr]


def test_cluster_se_is_cr1_by_hand_and_agrees_with_the_lifecycle_helper():
    d = np.array([1.0, 2.0, 3.0, 5.0, 4.0, 3.0])
    w = np.array([1, 1, 2, 2, 3, 3])
    # cluster sums of (d - 3): -3, 2, 1 -> sqrt(3/2 * 14) / 6
    assert S.cluster_se(d, w) == pytest.approx(np.sqrt(1.5 * 14) / 6)
    assert S.cluster_se(d, w) == pytest.approx(R.cr1_se(d, w))
    assert np.isnan(S.cluster_se(d, np.ones(6)))


def _games(n_weeks: int = 14, per_week: int = 2, **cols) -> pl.DataFrame:
    n = n_weeks * per_week
    base = {
        "week": np.repeat(np.arange(5, 5 + n_weeks), per_week),
        "result": np.tile([3.0, -3.0], n // 2),
        "spread_line": np.zeros(n),
        "base_margin": np.zeros(n),
        "qb_margin": np.zeros(n),
        "base_lo80": np.full(n, -10.0),
        "base_hi80": np.full(n, 10.0),
        "qb_lo80": np.full(n, -10.0),
        "qb_hi80": np.full(n, 10.0),
        "base_win_prob": np.full(n, 0.5),
        "qb_win_prob": np.full(n, 0.5),
        "home_status": ["none"] * n,
        "away_status": ["none"] * n,
    }
    base.update(cols)
    return pl.DataFrame(base)


def test_criterion_1_needs_the_improvement_beyond_two_ses():
    n = 28
    noise = np.tile([0.2, -0.2], n // 2)  # within-week spread makes a non-zero SE
    worse = S.score(_games(base_margin=np.full(n, 1.0) + noise))["1_mae_vs_close"]
    assert worse["dbar"] < 0 and worse["pass"]
    tie = S.score(_games(base_margin=np.zeros(n)))["1_mae_vs_close"]
    assert tie["dbar"] == 0 and not tie["pass"]  # no improvement is a fail, not a pass
    # an improvement inside two SEs fails: d = -1 in even weeks and +0.8 in odd ones, dbar -0.1, SE 0.25
    even = np.repeat(np.arange(14) % 2 == 0, 2)
    small = S.score(_games(base_margin=np.where(even, 1.0, 0.0), qb_margin=np.where(even, 0.0, 0.8)))
    c1 = small["1_mae_vs_close"]
    assert c1["dbar"] == pytest.approx(-0.1) and c1["dbar"] + 2 * c1["se"] > 0 and not c1["pass"]


def test_criteria_2_and_4_are_one_sided_upper_bounds():
    n = 28
    ok = S.score(_games(qb_margin=np.full(n, 0.05)))  # |q - result| is 0.05 lower or higher: dbar 0, se 0
    assert ok["2_mae_vs_result"]["upper"] == pytest.approx(0.0) and ok["2_mae_vs_result"]["pass"]
    bad = S.score(_games(qb_margin=np.tile([-0.5, 0.5], n // 2)))  # every game 0.5 further from the result
    assert bad["2_mae_vs_result"]["dbar"] == pytest.approx(0.5) and not bad["2_mae_vs_result"]["pass"]
    brier = S.score(_games(qb_win_prob=np.full(n, 0.55)))["4_brier"]  # a home win then a loss: +0.0025 a game
    assert brier["dbar"] == pytest.approx(0.0025) and brier["upper"] == pytest.approx(0.0025) and brier["pass"]
    brier_bad = S.score(_games(qb_win_prob=np.full(n, 0.56)))["4_brier"]  # +0.0036
    assert brier_bad["dbar"] == pytest.approx(0.0036) and not brier_bad["pass"]


def test_brier_scores_a_tie_as_half():
    n = 28
    g = _games(result=np.zeros(n), qb_win_prob=np.full(n, 0.5), base_win_prob=np.full(n, 0.6))
    assert S.score(g)["4_brier"]["dbar"] == pytest.approx(-0.01)


def test_criterion_3a_is_not_evaluable_under_20_games_and_otherwise_needs_closer_and_within():
    n = 28
    status = ["Questionable"] * 19 + ["none"] * 9
    few = S.score(_games(home_status=status))["3a_coverage_uncertain"]
    assert few == {"n": 19, "cov_q": 1.0, "cov_base": 1.0, "pass": True, "evaluable": False}
    status = ["Doubtful"] * 20 + ["none"] * 8
    # Q covers 16 of 20 (0.80), base all 20: closer and within 0.05 -> pass
    qlo = np.array([5.0] * 4 + [-10.0] * 24)
    good = S.score(_games(away_status=status, qb_lo80=qlo))["3a_coverage_uncertain"]
    assert good["cov_q"] == pytest.approx(0.80) and good["pass"]
    # Q covers 15 of 20 (0.75): exactly 0.05 from 0.80 and closer than base -> passes at the boundary
    edge = S.score(_games(away_status=status, qb_lo80=np.array([5.0] * 5 + [-10.0] * 23)))["3a_coverage_uncertain"]
    assert edge["cov_q"] == pytest.approx(0.75) and edge["pass"]
    out = S.score(_games(away_status=status, qb_lo80=np.array([5.0] * 6 + [-10.0] * 22)))["3a_coverage_uncertain"]
    assert out["cov_q"] == pytest.approx(0.70) and not out["pass"]


def test_criterion_3b_holds_certain_game_coverage_within_two_points_of_base():
    n = 28
    same = S.score(_games())["3b_coverage_certain"]
    assert same["pass"] and same["n"] == n
    off = S.score(_games(qb_lo80=np.array([5.0] + [-10.0] * (n - 1))))["3b_coverage_certain"]  # 1/28 = .036
    assert not off["pass"]


def test_keep_needs_every_criterion():
    n = 28
    noise = np.tile([0.2, -0.2], n // 2)
    res = S.score(_games(base_margin=np.full(n, 1.0) + noise))
    assert res["keep"] == all(res[k]["pass"] for k in res if k != "keep")
    assert not S.score(_games())["keep"]


def test_criterion_2_passes_at_exactly_a_tenth_of_a_point():
    n = 28
    at = S.score(_games(qb_margin=np.tile([-0.1, 0.1], n // 2)))["2_mae_vs_result"]  # +0.1 every game, SE 0
    assert at["upper"] == pytest.approx(0.10) and at["pass"]
    over = S.score(_games(qb_margin=np.tile([-0.11, 0.11], n // 2)))["2_mae_vs_result"]
    assert not over["pass"]


def test_criterion_3a_needs_q_strictly_closer_to_80_than_base():
    status = ["Questionable"] * 20 + ["none"] * 8
    # both cover 16 of 20 (0.80): within 0.05 but not closer than base -> fail
    lo = np.array([5.0] * 4 + [-10.0] * 24)
    same = S.score(_games(home_status=status, qb_lo80=lo, base_lo80=lo))["3a_coverage_uncertain"]
    assert same["cov_q"] == same["cov_base"] == pytest.approx(0.80) and not same["pass"]
    # Q at 0.80, base at 0.75: closer -> pass; Q at 0.75 against base 0.80 -> fail
    base75 = np.array([5.0] * 5 + [-10.0] * 23)
    assert S.score(_games(home_status=status, qb_lo80=lo, base_lo80=base75))["3a_coverage_uncertain"]["pass"]
    assert not S.score(_games(home_status=status, qb_lo80=base75, base_lo80=lo))["3a_coverage_uncertain"]["pass"]
