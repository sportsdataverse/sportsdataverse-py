"""Cricket expected-runs + WPA unit tests and the sum-to-outcome gate (T7.3 Phase 3).

The reconciliation + E[runs] gate is validated on the committed WPA holdout
(``tests/fixtures/league_ports/cricket_wpa_holdout.parquet``): 200 unseen male
T20I+ODI matches, per-over PLUS terminal states so each win-prob trajectory
reaches the pinned outcome. Observed at fit time (never lower — debug the model):
winner net WPA = +0.500, loser = -0.500 (exact telescoping); model terminal
|p - outcome| mean ~0.008; first-innings midpoint E[runs] MAE ~21.7 (T20) /
~41.2 (ODI), bias ~+3.7 / +9.0.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
from sportsdataverse.cricket.cricket_wpa import cricket_expected_runs, cricket_wpa

WPA_HOLDOUT = Path(__file__).resolve().parents[1] / "fixtures/league_ports/cricket_wpa_holdout.parquet"


# --- Task 3.1: expected runs --------------------------------------------------
def test_expected_runs_basic() -> None:
    df = pl.DataFrame({"runs": [80, 120], "proj_final": [160.0, 170.0], "overs_left": [8, 4]})
    out = cricket_expected_runs(df)
    assert out["exp_runs_remaining"].to_list() == [80.0, 50.0]
    assert out["exp_run_rate"].to_list() == [10.0, 12.5]


def test_expected_runs_zero_overs_left_null_rate() -> None:
    df = pl.DataFrame({"runs": [160], "proj_final": [160.0], "overs_left": [0]})
    out = cricket_expected_runs(df)
    assert out["exp_runs_remaining"].to_list() == [0.0]
    assert out["exp_run_rate"].to_list() == [None]


def test_expected_runs_empty() -> None:
    df = pl.DataFrame(schema={"runs": pl.Int64, "proj_final": pl.Float64, "overs_left": pl.Int64})
    out = cricket_expected_runs(df)
    assert out.height == 0
    assert "exp_runs_remaining" in out.columns and "exp_run_rate" in out.columns


# --- Task 3.2: WPA ------------------------------------------------------------
def test_wpa_basic_ascending() -> None:
    df = pl.DataFrame(
        {
            "event_id": ["M", "M", "M"],
            "innings_number": [2, 2, 2],
            "balls_bowled": [6, 12, 18],
            "win_prob": [0.4, 0.5, 0.65],
        }
    )
    out = cricket_wpa(df)
    wb = out["wpa_batting"].to_list()
    assert wb[0] == 0.0  # first state references no prior within the innings
    assert wb[1] > 0.0 and wb[2] > 0.0
    assert out["wpa_bowling"].to_list() == [-x for x in wb]


def test_wpa_over_isolates_matches_and_innings() -> None:
    df = pl.DataFrame(
        {
            "event_id": ["M1", "M1", "M2", "M2"],
            "innings_number": [2, 2, 2, 2],
            "balls_bowled": [6, 12, 6, 12],
            "win_prob": [0.4, 0.7, 0.9, 0.2],
        }
    )
    out = cricket_wpa(df).sort(["event_id", "balls_bowled"])
    # first ball of EACH match is 0 (no leak from M1's last state into M2's first)
    m1 = out.filter(pl.col("event_id") == "M1")["wpa_batting"].to_list()
    m2 = out.filter(pl.col("event_id") == "M2")["wpa_batting"].to_list()
    assert m1[0] == 0.0 and m2[0] == 0.0
    assert abs(m1[1] - 0.3) < 1e-9 and abs(m2[1] - (-0.7)) < 1e-9


def test_wpa_empty() -> None:
    df = pl.DataFrame(
        schema={"event_id": pl.Utf8, "innings_number": pl.Int64, "balls_bowled": pl.Int64, "win_prob": pl.Float64}
    )
    out = cricket_wpa(df)
    assert out.height == 0
    for c in ("win_prob_before", "wpa_batting", "wpa_bowling"):
        assert c in out.columns


# --- Task 3.3 gate: reconciliation + E[runs] calibration ----------------------
def _scored_wpa() -> pl.DataFrame:
    h = pl.read_parquet(WPA_HOLDOUT)
    # Guard against a silently-shrunk fixture passing the gate vacuously.
    assert h.height >= 10000, f"WPA holdout shrank to {h.height} states"
    assert h["event_id"].n_unique() >= 180, "WPA holdout lost matches"
    return cricket_win_probability(h)


def test_wpa_telescoping_identity() -> None:
    # Sum of wpa_batting over an innings must equal win_prob[last] - win_prob[first].
    w = cricket_wpa(_scored_wpa())
    chk = w.group_by(["event_id", "innings_number"]).agg(
        s=pl.col("wpa_batting").sum(),
        last=pl.col("win_prob").last(),
        first=pl.col("win_prob").first(),
    )
    err = (chk["s"] - (chk["last"] - chk["first"])).abs().max()
    assert float(err) < 1e-9


def test_wpa_bowling_is_negation() -> None:
    w = cricket_wpa(_scored_wpa())
    assert bool((w["wpa_bowling"] == -w["wpa_batting"]).all())


def test_wpa_sum_to_outcome() -> None:
    # Contract/arithmetic check of the WPA reconciliation FRAMING: seeding the
    # pre-match 0.5 prior and anchoring the true outcome telescopes to winner
    # +0.5 / loser -0.5. This validates the reconciliation seed+anchor
    # convention (not the surface itself -- the model's genuine convergence is
    # covered by test_wpa_telescoping_identity + test_wpa_model_converges_to_outcome
    # on the real win_prob trajectory).
    s = _scored_wpa().with_columns(
        p_ref=pl.when(pl.col("innings_number") == 1).then(pl.col("win_prob")).otherwise(1.0 - pl.col("win_prob"))
    )
    winners, losers = [], []
    for (_ev,), g in s.sort(["innings_number", "balls_bowled"]).group_by(["event_id"]):
        inn1 = g.filter(pl.col("innings_number") == 1)
        if inn1.height == 0:
            continue
        ref_won = float(inn1["chasing_won"][0])
        traj = np.concatenate([[0.5], g["p_ref"].to_numpy(), [ref_won]])
        net = float(np.diff(traj).sum())
        (winners if ref_won == 1.0 else losers).append(net)
    assert abs(np.mean(winners) - 0.5) <= 0.05
    assert abs(np.mean(losers) + 0.5) <= 0.05


def test_wpa_model_converges_to_outcome() -> None:
    # By the terminal state the model should agree with the result (mean gap small).
    s = _scored_wpa().with_columns(
        p_ref=pl.when(pl.col("innings_number") == 1).then(pl.col("win_prob")).otherwise(1.0 - pl.col("win_prob"))
    )
    gaps = []
    for (_ev,), g in s.sort(["innings_number", "balls_bowled"]).group_by(["event_id"]):
        inn1 = g.filter(pl.col("innings_number") == 1)
        if inn1.height == 0:
            continue
        ref_won = float(inn1["chasing_won"][0])
        gaps.append(abs(float(g["p_ref"].to_numpy()[-1]) - ref_won))
    assert float(np.mean(gaps)) <= 0.05  # mean; a rare DLS-shortened match can end mid-trajectory


def test_expected_runs_first_innings_calibration() -> None:
    er = cricket_expected_runs(_scored_wpa())
    floors = {"t20": (120, 23.0, 6.0), "odi": (300, 43.0, 12.0)}  # (balls_total, MAE floor, bias floor)
    for fmt, (bt, mae_floor, bias_floor) in floors.items():
        mid = er.filter(
            (pl.col("fmt") == fmt)
            & (pl.col("innings_number") == 1)
            & (pl.col("balls_bowled") >= bt * 0.45)
            & (pl.col("balls_bowled") <= bt * 0.55)
        )
        proj = mid["proj_final"].to_numpy()
        actual = mid["innings_final_runs"].to_numpy()
        mae = float(np.abs(proj - actual).mean())
        bias = float((proj - actual).mean())
        assert mae <= mae_floor, f"{fmt} E[runs] MAE {mae:.1f} > {mae_floor}"
        assert abs(bias) <= bias_floor, f"{fmt} E[runs] bias {bias:+.1f} exceeds {bias_floor}"
