"""Offline artifact-gated tests for the cfb4th 4th-down decision surface.

These run against the bundled EP / WP-spread / fd models (+ the bundled punt
distribution) on synthetic 4th-down rows -- no network. The field-goal
assertions are gated on ``FG_MODEL_AVAILABLE`` so the suite stays green whether
or not the cfb4th GAM has been converted + bundled.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_fourth_down import (
    FG_MODEL_AVAILABLE,
    _load_fd_model,
    get_4th_down_probs,
    get_fg_wp,
    get_go_wp,
    get_punt_wp,
)

# The fourth-down yards model is download-on-demand (~16 MB, not bundled); the
# go / combiner / pipeline tests need it. Gate them so the suite stays green
# offline (no cache + no network).
try:
    _load_fd_model()
    FD_MODEL_AVAILABLE = True
except Exception:  # pragma: no cover - environment dependent
    FD_MODEL_AVAILABLE = False

requires_fd = pytest.mark.skipif(
    not FD_MODEL_AVAILABLE, reason="fd_model unavailable (download-on-demand; offline + no cache)"
)

PROB_COLS = [
    "go_wp",
    "first_down_prob",
    "wp_succeed",
    "wp_fail",
    "punt_wp",
    "fg_wp",
    "fg_make_prob",
    "make_fg_wp",
    "miss_fg_wp",
]


def _row(
    down=4,
    dist=2,
    ytg=45,
    sd=0,
    tsr=900,
    adj=900,
    period=2,
    home=1.0,
    recv=1.0,
    pto=3,
    dto=3,
    season=2022,
    ou=55.5,
    hsp=-3.0,
    spread=-3.0,
):
    return {
        "start.down": down,
        "start.distance": dist,
        "start.yardsToEndzone": ytg,
        "start.pos_team_spread": spread,
        "pos_score_diff_start": sd,
        "start.TimeSecsRem": tsr,
        "start.adj_TimeSecsRem": adj,
        "start.pos_team_receives_2H_kickoff": recv,
        "start.posTeamTimeouts": pto,
        "start.defPosTeamTimeouts": dto,
        "start.is_home": home,
        "period": period,
        "season": season,
        "overUnder": ou,
        "homeTeamSpread": hsp,
    }


@pytest.fixture()
def fourth_down_rows():
    return pl.DataFrame(
        [
            _row(4, 2, 45, sd=0),  # midfield 4th & 2, tied
            _row(4, 1, 2, sd=-3),  # 4th & goal at 2, down 3
            _row(4, 8, 25, sd=7),  # 4th & 8 at opp 25 (FG range), up 7
            _row(4, 5, 80, sd=0),  # 4th & 5 at own 20
            _row(4, 1, 5, sd=0),  # 4th & 1 at opp 5
        ]
    )


def _assert_prob_bounds(df):
    for c in PROB_COLS:
        if c not in df.columns:
            continue
        v = df[c].to_numpy().astype(float)
        finite = v[np.isfinite(v)]
        if len(finite):
            assert finite.min() >= -1e-9, (c, finite.min())
            assert finite.max() <= 1 + 1e-9, (c, finite.max())


@requires_fd
def test_go_wp_columns_present_and_bounded(fourth_down_rows):
    out = get_go_wp(fourth_down_rows)
    for c in ("go_wp", "first_down_prob", "wp_succeed", "wp_fail"):
        assert c in out.columns
    # go_wp is always defined
    assert np.isfinite(out["go_wp"].to_numpy()).all()
    _assert_prob_bounds(out)


def test_punt_wp_bounded_and_unsupported_is_nan(fourth_down_rows):
    out = get_punt_wp(fourth_down_rows)
    assert "punt_wp" in out.columns
    _assert_prob_bounds(out)
    # punt distribution covers yards_to_goal 31..99; goal-line plays (ytg < 31)
    # have no support and must be NaN (matches cfb4th left-join NA).
    ytg = fourth_down_rows["start.yardsToEndzone"].to_numpy()
    punt = out["punt_wp"].to_numpy().astype(float)
    assert np.isnan(punt[ytg < 31]).all()
    assert np.isfinite(punt[ytg >= 31]).all()


@requires_fd
def test_go_wp_tracks_first_down_prob():
    # same yard line, increasing distance: first_down_prob falls monotonically
    # and go_wp tracks it (positive correlation) in a tied game.
    rows = pl.DataFrame([_row(4, d, 50, sd=0) for d in (1, 2, 5, 10, 15)])
    g = get_go_wp(rows)
    fdp = g["first_down_prob"].to_numpy()
    assert np.all(np.diff(fdp) <= 1e-6), "first_down_prob should decrease with distance"
    corr = np.corrcoef(g["first_down_prob"].to_numpy(), g["go_wp"].to_numpy())[0, 1]
    assert corr > 0.5, f"go_wp should track first_down_prob (corr={corr:.3f})"


@requires_fd
def test_get_4th_down_probs_recommendation_membership(fourth_down_rows):
    out = get_4th_down_probs(fourth_down_rows)
    for c in (
        "go_wp",
        "punt_wp",
        "fg_wp",
        "fourth_down_recommendation",
        "go_wp_diff",
        "punt_wp_diff",
        "fg_wp_diff",
        "go_boost",
    ):
        assert c in out.columns
    _assert_prob_bounds(out)
    recs = out["fourth_down_recommendation"].dropna().unique().tolist()
    assert set(recs) <= {"go", "punt", "field_goal"}, recs
    # the recommended option's *_wp_diff is 0 (it is the argmax); others <= 0.
    for _, r in out.iterrows():
        rec = r["fourth_down_recommendation"]
        if rec is None or (isinstance(rec, float) and np.isnan(rec)):
            continue
        diff = {"go": r["go_wp_diff"], "punt": r["punt_wp_diff"], "field_goal": r["fg_wp_diff"]}[rec]
        assert abs(diff) < 1e-9, (rec, diff)
        for k, v in (("go", r["go_wp_diff"]), ("punt", r["punt_wp_diff"]), ("field_goal", r["fg_wp_diff"])):
            if np.isfinite(v):
                assert v <= 1e-9, (k, v)


def test_empty_input_returns_empty_decision_cols():
    empty = pl.DataFrame(schema={k: pl.Float64 for k in _row().keys()})
    out = get_4th_down_probs(empty)
    assert len(out) == 0
    assert "fourth_down_recommendation" in out.columns


@pytest.mark.skipif(not FG_MODEL_AVAILABLE, reason="cfb4th FG model not bundled")
def test_fg_make_prob_decreases_with_distance():
    # fg_make_prob should fall as the kick gets longer (yards_to_goal grows).
    rows = pl.DataFrame([_row(4, 10, ytg) for ytg in (3, 10, 17, 25, 33)])
    fg = get_fg_wp(rows)
    p = fg["fg_make_prob"].to_numpy().astype(float)
    assert np.all(np.diff(p) <= 1e-6), f"fg_make_prob should decrease with distance: {p}"


@pytest.mark.skipif(FG_MODEL_AVAILABLE, reason="FG model present; null-path test only when absent")
def test_fg_columns_null_when_model_absent(fourth_down_rows):
    fg = get_fg_wp(fourth_down_rows)
    for c in ("fg_make_prob", "make_fg_wp", "miss_fg_wp", "fg_wp"):
        assert fg[c].isna().all(), c


@requires_fd
def test_cfb_play_process_add_fourth_down_probs_synthetic(monkeypatch):
    """The CFBPlayProcess method writes decision columns onto 4th-down rows.

    Drives a minimal synthetic processed-plays state through
    ``add_fourth_down_probs`` without touching the network: we set
    ``self.plays_json`` directly and mark the pipeline as already run.
    """
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=1)
    proc.ran_pipeline = True
    proc.json = {"plays": []}
    proc.plays_json = [
        _row(4, 2, 45, sd=0),
        _row(1, 10, 60, sd=0),  # not 4th down -> decision cols null
        _row(4, 5, 80, sd=0),
    ]
    out = proc.add_fourth_down_probs()
    assert "fourth_down_recommendation" in out.columns
    assert "go_wp" in out.columns
    fourth = out.filter(pl.col("start.down") == 4)
    non_fourth = out.filter(pl.col("start.down") != 4)
    assert fourth.height == 2
    # 4th-down rows have a finite go_wp; non-4th rows are null
    assert fourth["go_wp"].is_not_null().all()
    assert non_fourth["go_wp"].is_null().all()
    recs = fourth["fourth_down_recommendation"].drop_nulls().unique().to_list()
    assert set(recs) <= {"go", "punt", "field_goal"}
