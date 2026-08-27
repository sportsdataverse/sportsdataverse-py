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


@requires_fd
def test_add_fourth_down_probs_idempotent_after_pipeline():
    """run_processing_pipeline() appends the decision columns by default, so the
    documented run_processing_pipeline(); add_fourth_down_probs() flow re-enters
    with them already present. The re-score must overwrite cleanly (no suffixed
    ``*_right`` duplicate columns) and expose ``go_boost`` (schema parity with the
    integrated pipeline path)."""
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    proc = CFBPlayProcess(gameId=1)
    proc.ran_pipeline = True
    proc.json = {"plays": []}
    # a 4th-down row already carrying (stale) decision columns, as if the pipeline ran
    stale = {**_row(4, 2, 45, sd=0)}
    for c in CFBPlayProcess._FOURTH_DOWN_DECISION_COLS:
        stale[c] = "punt" if c == "fourth_down_recommendation" else 0.123
    proc.plays_json = [stale, _row(1, 10, 60, sd=0)]
    out = proc.add_fourth_down_probs()
    assert [c for c in out.columns if c.endswith("_right")] == [], "stale decision cols not overwritten cleanly"
    assert "go_boost" in out.columns
    # the 4th-down row was actually re-scored, not left at the stale sentinel
    assert out.filter(pl.col("start.down") == 4)["go_wp"][0] != 0.123


@pytest.mark.skipif(not FG_MODEL_AVAILABLE, reason="FG model absent")
def test_fg_make_prob_requires_season_when_era_aware():
    """The era-aware fg_model must fail loudly (not silently zero the era dummies)
    when ``season`` is missing from the input frame."""
    from sportsdataverse.cfb.cfb_fourth_down import fg_model

    fnames = fg_model.feature_names or []
    if not any(str(f).startswith("era") for f in fnames):
        pytest.skip("bundled fg_model is not era-aware")
    rows = pl.DataFrame([_row(4, 2, 45)]).drop("season")
    with pytest.raises(ValueError, match="season"):
        get_fg_wp(rows)


# --------------------------------------------------------------------------- #
# End-game clamp perspective (#395)
#
# cfb4th applies end_game_fn to the ALREADY-FLIPPED frame, so the rule reads
# "the team now holding the ball leads, and the team it took the ball from is
# out of timeouts, so whoever is asking loses". Feeding the pre-play team's own
# columns inverts it and pins the team that is AHEAD to zero.
#
# None of these raise when inverted -- they return a confident wrong number, so
# each is asserted directionally rather than against a fixture value.
# --------------------------------------------------------------------------- #


def _late(**kw):
    """A Q4 row with a minute left, the possessing team out of timeouts."""
    base = dict(period=4, tsr=60, adj=60, pto=0, dto=3)
    base.update(kw)
    return pl.DataFrame([_row(**base)])


def test_punt_clamp_pins_the_trailing_team_not_the_leading_one():
    # Punting team out of timeouts, 60s left in Q4. Trailing: you punt, they
    # kneel it out, you lose. Leading: you are still winning.
    trailing = get_punt_wp(_late(dist=10, ytg=50, sd=-3))["punt_wp"].to_numpy()[0]
    leading = get_punt_wp(_late(dist=10, ytg=50, sd=3))["punt_wp"].to_numpy()[0]
    assert trailing < 0.01, trailing
    assert leading > 0.5, leading
    # The inverted form produced exactly this pair the other way round:
    # trailing 0.0999 (unclamped) and leading 0.0 (pinned).
    assert leading > trailing


def test_punt_clamp_does_not_fire_with_timeouts_or_outside_the_fourth():
    trailing_no_to = get_punt_wp(_late(dist=10, ytg=50, sd=-3))["punt_wp"].to_numpy()[0]
    # Timeouts in hand: still a game.
    with_to = get_punt_wp(_late(dist=10, ytg=50, sd=-3, pto=3))["punt_wp"].to_numpy()[0]
    assert with_to > trailing_no_to
    # cfb4th's end_game_fn is gated on period == 4.
    second_qtr = get_punt_wp(pl.DataFrame([_row(dist=10, ytg=50, sd=-3, period=2, tsr=60, adj=1860, pto=0)]))[
        "punt_wp"
    ].to_numpy()[0]
    assert second_qtr > trailing_no_to


@pytest.mark.skipif(not FG_MODEL_AVAILABLE, reason="fg_model not bundled")
def test_fg_clamp_does_not_zero_a_team_that_is_ahead():
    # A team kicking a field goal while up 3 with a minute left is not a team
    # with zero chance of winning. The inverted clamp collapsed BOTH branches
    # (make and miss) to 0, so fg_wp was 0 as well.
    leading = get_fg_wp(_late(dist=10, ytg=20, sd=3))
    assert leading["make_fg_wp"].to_numpy()[0] > 0.5
    assert leading["miss_fg_wp"].to_numpy()[0] > 0.5
    assert leading["fg_wp"].to_numpy()[0] > 0.5

    # Trailing by 3, a MISS hands it over to a team that can kneel it out.
    trailing = get_fg_wp(_late(dist=10, ytg=20, sd=-3))
    assert trailing["miss_fg_wp"].to_numpy()[0] < 0.01
    # ...but making it ties the game, so that branch must not be clamped.
    assert trailing["make_fg_wp"].to_numpy()[0] > 0.1


@requires_fd
def test_go_fail_clamp_pins_the_turnover_that_loses_the_lead():
    # After a turnover the resulting frame's differential is already negated, so
    # `> 0` means the team that just TOOK the ball leads. Reading it as `< 0`
    # pinned a team that turned it over WHILE AHEAD to a certain loss.
    #
    # The clamp needs the OFFENCE's own timeouts at 0: after the turnover
    # new_def_to is the offence's pre-play count. Setting dto=0 instead leaves
    # new_def_to == 3 and the clamp never fires at all -- a test written that
    # way passes with either sign.
    leading = get_go_wp(_late(dist=1, ytg=50, sd=3, pto=0, dto=3))
    trailing = get_go_wp(_late(dist=1, ytg=50, sd=-3, pto=0, dto=3))
    assert leading["wp_fail"].to_numpy()[0] > 0.5
    assert trailing["wp_fail"].to_numpy()[0] == pytest.approx(0.0, abs=1e-9)


def test_return_touchdown_keeps_the_punting_teams_own_lead():
    # cfb4th: -(flipped) - 7 == orig - 7. The double negation reduced to
    # -orig - 7, turning a 10-point lead into a 17-point deficit. Return-TD rows
    # carry <0.2% of the mass, so this is asserted on the arithmetic directly
    # rather than through an aggregate that would hide it.
    orig = np.array([10.0, -4.0, 0.0])
    flipped = -orig
    assert np.allclose(-flipped - 7.0, orig - 7.0)
    assert not np.allclose(-(-flipped) - 7.0, orig - 7.0)


# --------------------------------------------------------------------------- #
# Input contract (#395)
# --------------------------------------------------------------------------- #


def test_missing_required_column_names_itself():
    # _to_pandas used to substitute NaN for an absent column. The NaN poisoned
    # the turnover bucketing, the pivot came back missing a bucket, and the
    # caller died several frames later with "'float' object has no attribute
    # 'to_numpy'" -- naming neither the column nor the frame.
    frame = pl.DataFrame([_row()]).drop("start.distance")
    with pytest.raises(KeyError, match="start.distance"):
        get_go_wp(frame)


def test_optional_odds_columns_are_still_optional():
    # overUnder / homeTeamSpread have a documented fallback in _posteam_total,
    # so they must NOT be caught by the required-column check.
    frame = pl.DataFrame([_row()]).drop(["overUnder", "homeTeamSpread"])
    out = get_punt_wp(frame)
    assert "punt_wp" in out.columns


@requires_fd
def test_go_path_requires_season_like_the_fg_path_does():
    # The FG path has always raised here. The GO path fed the same NaN straight
    # into _era_onehot, so every era dummy came out 0 -- not a valid one-hot --
    # and fd_model scored against a rule era that never existed, returning a
    # plausible 76-class distribution with nothing to flag it.
    rows = pl.DataFrame([_row(4, 2, 45)]).drop("season")
    with pytest.raises(ValueError, match="season"):
        get_go_wp(rows)


def test_return_touchdown_rows_clamp_toward_a_win_not_a_loss():
    # A punt return TD hands the ball straight back, so on those rows `wp` is
    # already the punting team's and possession never changed. cfb4th clamps
    # every row to 0, which pins a punting team still LEADING after conceding
    # the score to a certain loss. Those rows must clamp to 1 instead.
    #
    # Asserted through the clamp helper directly: return-TD rows carry under
    # 0.2% of the punt distribution's mass, so an aggregate would hide the sign.
    from sportsdataverse.cfb.cfb_fourth_down import _end_game_clamp

    wp = np.array([0.5])
    lead, adj, period, def_to = (
        np.array([3.0]),
        np.array([60.0]),
        np.array([4.0]),
        np.array([0.0]),
    )
    assert _end_game_clamp(wp, lead, adj, period, def_to) == pytest.approx(0.0)
    assert _end_game_clamp(wp, lead, adj, period, def_to, value=1.0) == pytest.approx(1.0)
    # Trailing is not a kneel-out for anyone, whichever value is passed.
    trailing = np.array([-3.0])
    assert _end_game_clamp(wp, trailing, adj, period, def_to, value=1.0) == pytest.approx(0.5)


def test_punt_path_itself_routes_return_touchdowns_to_the_winning_clamp(monkeypatch):
    # The helper-level test above pins the clamp's arithmetic but never runs the
    # `return_td` selector inside get_punt_wp, so a regression to value=0.0
    # there would not fail it. Swap the empirical distribution for a certain
    # return touchdown: punt_wp is then decided entirely by that branch, and the
    # 0.2%-of-the-mass problem that forced the helper-level test disappears.
    from sportsdataverse.cfb import cfb_fourth_down as m

    monkeypatch.setattr(
        m,
        "punt_distribution",
        pl.DataFrame({"yards_to_goal": [50], "yards_to_goal_end": [100], "pct": [1.0]}),
    )
    late = dict(period=4, tsr=60, adj=60, dist=10, ytg=50, dto=0)

    # Up 10, concede the return TD -> still up 3, and the receiving team has no
    # timeouts left to stop the clock, so the punting team kneels out a win.
    lead = get_punt_wp(pl.DataFrame([_row(sd=10, pto=3, **late)]))
    assert lead["punt_wp"].to_numpy()[0] == pytest.approx(1.0)

    # Up only 3, the same return TD puts them behind, so nothing is clamped.
    behind = get_punt_wp(pl.DataFrame([_row(sd=3, pto=3, **late)]))
    assert behind["punt_wp"].to_numpy()[0] < 1.0
