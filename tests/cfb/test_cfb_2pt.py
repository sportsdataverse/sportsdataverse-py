"""Tests for the cfb4th two-point-conversion decision surface (get_2pt_probs).

Offline-synthetic assertions run against the bundled two-point model + the
bundled EP / WP-spread boosters (no network). A live-gated test additionally
drives a real processed game through ``CFBPlayProcess.add_2pt_probs``.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from xgboost import Booster

from sportsdataverse.cfb.cfb_two_point import (
    TWO_PT_FEATURES,
    TWO_PT_MODEL_AVAILABLE,
    _XP_MAKE_PROB,
    get_2pt_probs,
)
from tests.conftest import fetch_pbp_or_skip, skip_if_no_live

requires_two_pt = pytest.mark.skipif(not TWO_PT_MODEL_AVAILABLE, reason="two_pt_model not bundled")


def _row(
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
    """A post-touchdown state row (scoring team possesses; decide XP vs 2pt)."""
    return {
        "start.down": 1,
        "start.distance": 10,
        "start.yardsToEndzone": 75,
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
def td_rows():
    return pl.DataFrame(
        [
            _row(sd=-2, tsr=120, adj=120, period=4),  # trailing 2, late
            _row(sd=14, tsr=1800, adj=1800, period=3),  # leading 14, comfortable
            _row(sd=0, tsr=900, adj=900, period=2),  # tied, mid
            _row(sd=-8, tsr=200, adj=200, period=4),  # trailing 8, late
        ]
    )


def test_bundled_model_is_4_feature_logistic():
    assert TWO_PT_MODEL_AVAILABLE, "two_pt_model.ubj must be bundled under cfb/models/"
    from sportsdataverse.cfb.cfb_two_point import two_pt_model

    assert isinstance(two_pt_model, Booster)
    assert two_pt_model.feature_names == TWO_PT_FEATURES
    assert TWO_PT_FEATURES == ["posteam_spread", "posteam_total", "pos_score_diff", "era"]
    assert abs(_XP_MAKE_PROB - 0.9851) < 1e-9


@requires_two_pt
def test_columns_present_and_bounded(td_rows):
    out = get_2pt_probs(td_rows)
    for c in ("two_pt_wp", "xp_wp", "prob_2pt", "two_pt_recommendation", "two_pt_wp_diff"):
        assert c in out.columns

    for c in ("two_pt_wp", "xp_wp", "prob_2pt"):
        v = out[c].to_numpy().astype(float)
        assert np.all(v >= -1e-9), (c, v.min())
        assert np.all(v <= 1 + 1e-9), (c, v.max())

    diff = out["two_pt_wp_diff"].to_numpy().astype(float)
    assert np.all(np.isfinite(diff))
    # two_pt_wp_diff == two_pt_wp - xp_wp
    np.testing.assert_allclose(diff, out["two_pt_wp"].to_numpy() - out["xp_wp"].to_numpy(), atol=1e-9)

    recs = set(out["two_pt_recommendation"].dropna().unique().tolist())
    assert recs <= {"go_for_2", "kick_xp"}, recs
    # recommendation is go_for_2 iff diff > 0
    for _, r in out.iterrows():
        expected = "go_for_2" if r["two_pt_wp_diff"] > 0 else "kick_xp"
        assert r["two_pt_recommendation"] == expected


@requires_two_pt
def test_trailing_two_late_favors_go_more_than_leading_big():
    # Sanity: when trailing by 2 late, going for 2 should be favored more
    # (larger two_pt_wp_diff) than in an already-decided game where the try is
    # irrelevant. The "decided" comparison must be late + a blowout (up 28, 1:00
    # left) -- a mid-game lead (e.g. up 21 in Q3) is NOT decided, so the try still
    # moves WP and the ordering would not hold.
    df = pl.DataFrame(
        [
            _row(sd=-2, tsr=120, adj=120, period=4),  # trailing 2, late
            _row(sd=28, tsr=60, adj=60, period=4),  # leading 28 with 1:00 left -> decided
        ]
    )
    out = get_2pt_probs(df)
    trailing_diff = out["two_pt_wp_diff"][0]
    leading_diff = out["two_pt_wp_diff"][1]
    assert trailing_diff > leading_diff
    assert out["two_pt_recommendation"][0] == "go_for_2"


def test_empty_input_returns_empty_decision_cols():
    empty = pl.DataFrame(schema={k: pl.Float64 for k in _row().keys()})
    out = get_2pt_probs(empty)
    assert len(out) == 0
    for c in ("two_pt_wp", "xp_wp", "prob_2pt", "two_pt_recommendation", "two_pt_wp_diff"):
        assert c in out.columns


def test_missing_state_columns_nulls_decision_cols():
    import pandas as pd

    guard = pd.DataFrame({"start.down": [1, 1]})  # required state cols absent
    out = get_2pt_probs(guard)
    assert out["two_pt_wp"].isna().all()
    assert out["xp_wp"].isna().all()
    assert out["two_pt_recommendation"].isna().all()


@requires_two_pt
def test_cfb_play_process_add_2pt_probs_synthetic():
    """The CFBPlayProcess method writes decision columns onto PAT / 2pt rows only."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=1)
    proc.ran_pipeline = True
    proc.json = {"plays": []}
    td = _row(sd=-2, tsr=120, adj=120, period=4)
    pat_good = {**_row(sd=-1, period=4), "pointAfterAttempt.text": "Extra Point Good"}
    not_pat = {**_row(sd=0), "pointAfterAttempt.text": None}
    proc.plays_json = [pat_good, not_pat, td]
    out = proc.add_2pt_probs()
    assert "two_pt_recommendation" in out.columns
    # exactly one PAT row -> one non-null recommendation
    n_rec = out["two_pt_recommendation"].is_not_null().sum()
    assert n_rec == 1
    rec_rows = out.filter(pl.col("two_pt_recommendation").is_not_null())
    assert rec_rows["two_pt_wp"].is_not_null().all()
    recs = set(rec_rows["two_pt_recommendation"].to_list())
    assert recs <= {"go_for_2", "kick_xp"}


@requires_two_pt
def test_pre2014_separate_pat_row_detected_by_type():
    """Pre-2014 games carry the PAT as a SEPARATE play row ("Extra Point Good" /
    "Two-Point Conversion Good") with no pointAfterAttempt / extra_point_result
    columns. It must still be detected (by play type) and scored at its
    already-post-TD pos_score_diff_start (pass_td/rush_td absent -> no +6)."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=1)
    proc.ran_pipeline = True
    proc.json = {"plays": []}
    xp = {**_row(sd=4, period=4), "type.text": "Extra Point Good"}
    two_pt = {**_row(sd=-2, tsr=120, adj=120, period=4), "type.text": "Two-Point Conversion Good"}
    not_pat = {**_row(sd=0), "type.text": "Rush"}
    proc.plays_json = [xp, two_pt, not_pat]
    out = proc.add_2pt_probs()
    rec_rows = out.filter(pl.col("two_pt_recommendation").is_not_null())
    # both PAT rows detected by type; the plain rush row is not
    assert rec_rows.height == 2
    assert rec_rows["two_pt_wp"].is_not_null().all()
    assert set(rec_rows["two_pt_recommendation"].to_list()) <= {"go_for_2", "kick_xp"}


@requires_two_pt
def test_offensive_td_gets_plus6_but_defensive_return_td_does_not():
    """The post-TD +6 score adjustment must apply only to TDs scored BY the posteam.
    `pass_td` also fires for pick-sixes (its `pass & td_play` branch), so the gate
    ANDs with `offense_score_play`. An offensive-TD PAT row (offense_score_play=True)
    is scored at +6; a defensive return-TD PAT row (offense_score_play=False, but
    pass_td=True) is not -- so their two-point decisions must differ."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=1)
    proc.ran_pipeline = True
    proc.json = {"plays": []}
    common = {**_row(sd=-1, period=4), "pointAfterAttempt.text": "Extra Point Good", "pass_td": True, "rush_td": False}
    offensive = {**common, "offense_score_play": True}  # posteam scored -> +6 applies
    defensive = {**common, "offense_score_play": False}  # pick-six PAT -> +6 must NOT apply
    proc.plays_json = [offensive, defensive]
    out = proc.add_2pt_probs()
    rec = out.filter(pl.col("two_pt_recommendation").is_not_null())
    assert rec.height == 2
    # +6 only on the offensive row changes its scored score frame, so its win-prob
    # outputs differ from the (un-adjusted) defensive return-TD row. (prob_2pt alone
    # can tie -- the 2pt booster lands pos_score_diff -1 vs +5 in the same leaf -- so
    # assert on two_pt_wp, which the WP model resolves.)
    assert rec["two_pt_wp"][0] != rec["two_pt_wp"][1]


@skip_if_no_live
def test_add_2pt_probs_live():
    """Live: a real game's PAT / 2pt rows get bounded WPs + valid recommendations."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=401628334)
    fetch_pbp_or_skip(proc)
    out = proc.add_2pt_probs()
    pat = out.filter(pl.col("two_pt_recommendation").is_not_null())
    if pat.height == 0:
        pytest.skip("no PAT / 2pt rows in this game")
    for c in ("two_pt_wp", "xp_wp"):
        v = pat[c].to_numpy().astype(float)
        assert np.all(v >= -1e-9) and np.all(v <= 1 + 1e-9), c
    diff = pat["two_pt_wp_diff"].to_numpy().astype(float)
    assert np.all(np.isfinite(diff))
    recs = set(pat["two_pt_recommendation"].to_list())
    assert recs <= {"go_for_2", "kick_xp"}, recs
