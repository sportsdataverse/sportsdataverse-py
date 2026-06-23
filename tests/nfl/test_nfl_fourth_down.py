"""Tests for the nfl4th 4th-down decision surface.

The synthetic-input tests run against the bundled 2-pt model + FG grid + punt
distribution and the download-on-demand fd / wp models; they are gated on model
availability so the suite stays green offline (no cache + no network).  The
oracle-parity test (gated by ``SDV_PY_LIVE_TESTS=1``) validates against nfl4th's
shipped ``pre_computed_go_boost`` outputs on a 2022 slice.
"""

from __future__ import annotations

import os

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.nfl_fourth_down import (
    FD_MODEL_AVAILABLE,
    WP_MODEL_AVAILABLE,
    get_2pt_wp,
    get_4th_down_probs,
    get_fg_wp,
    get_go_wp,
    get_punt_wp,
)
from tests.conftest import skip_if_no_live

# fd + wp models are download-on-demand; the go / fg / punt / combiner paths all
# need them, so gate those tests on availability.
requires_models = pytest.mark.skipif(
    not (FD_MODEL_AVAILABLE and WP_MODEL_AVAILABLE),
    reason="fd_model / wp_model unavailable (download-on-demand; offline + no cache)",
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
    ydstogo: int = 2,
    yardline_100: int = 50,
    sd: int = 0,
    qtr: int = 2,
    qsr: int = 600,
    season: int = 2022,
    spread: float = -3.0,
    total: float = 45.0,
    home: bool = True,
    pto: int = 3,
    dto: int = 3,
    hok: int = 1,
    roof: str = "outdoors",
) -> dict:
    """One nflverse-shape 4th-down row (posteam = home when ``home``)."""
    posteam, defteam = ("KC", "BUF") if home else ("BUF", "KC")
    return {
        "game_id": "2022_01_BUF_KC",
        "play_id": float(np.random.randint(1, 5000)),
        "season": season,
        "week": 1,
        "season_type": "REG",
        "posteam": posteam,
        "defteam": defteam,
        "home_team": "KC",
        "away_team": "BUF",
        "roof": roof,
        "qtr": qtr,
        "quarter_seconds_remaining": qsr,
        "down": 4,
        "ydstogo": ydstogo,
        "yardline_100": yardline_100,
        "score_differential": sd,
        "posteam_timeouts_remaining": pto,
        "defteam_timeouts_remaining": dto,
        "home_opening_kickoff": hok,
        "spread_line": spread,
        "total_line": total,
    }


@pytest.fixture()
def fourth_down_rows() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _row(2, 50, sd=0),  # midfield 4th & 2, tied
            _row(1, 2, sd=-3),  # 4th & goal at 2, down 3
            _row(8, 25, sd=7),  # 4th & 8 in FG range, up 7
            _row(5, 80, sd=0),  # 4th & 5 at own 20 (punt territory)
            _row(1, 5, sd=0),  # 4th & 1 at opp 5
        ]
    )


def _assert_prob_bounds(df) -> None:
    for c in PROB_COLS:
        if c not in df.columns:
            continue
        v = df[c].to_numpy().astype(float)
        finite = v[np.isfinite(v)]
        if len(finite):
            assert finite.min() >= -1e-9, (c, finite.min())
            assert finite.max() <= 1 + 1e-9, (c, finite.max())


@requires_models
def test_go_wp_columns_present_and_bounded(fourth_down_rows) -> None:
    out = get_go_wp(fourth_down_rows)
    for c in ("go_wp", "first_down_prob", "wp_succeed", "wp_fail"):
        assert c in out.columns
    assert np.isfinite(out["go_wp"].to_numpy()).all()
    _assert_prob_bounds(out)


@requires_models
def test_go_wp_tracks_first_down_prob() -> None:
    # same yard line, increasing distance -> first_down_prob falls monotonically.
    rows = pl.DataFrame([_row(d, 50, sd=0) for d in (1, 2, 5, 10, 15)])
    g = get_go_wp(rows)
    fdp = g["first_down_prob"].to_numpy()
    assert np.all(np.diff(fdp) <= 1e-6), f"first_down_prob should decrease with distance: {fdp}"


@requires_models
def test_punt_wp_bounded_and_unsupported_is_nan() -> None:
    rows = pl.DataFrame([_row(5, ytg, sd=0) for ytg in (5, 20, 35, 60, 90)])
    out = get_punt_wp(rows)
    assert "punt_wp" in out.columns
    _assert_prob_bounds(out)
    # punt distribution covers yardline_100 31..99; inside the 31 has no support
    # and must be NaN (matches nfl4th left-join NA).
    ytg = rows["yardline_100"].to_numpy()
    punt = out["punt_wp"].to_numpy().astype(float)
    assert np.isnan(punt[ytg < 31]).all()
    assert np.isfinite(punt[ytg >= 31]).all()


@requires_models
def test_fg_make_prob_decreases_with_distance() -> None:
    rows = pl.DataFrame([_row(10, ytg) for ytg in (3, 10, 17, 25, 33)])
    fg = get_fg_wp(rows)
    p = fg["fg_make_prob"].to_numpy().astype(float)
    assert np.all(np.diff(p) <= 1e-6), f"fg_make_prob should decrease with distance: {p}"
    # zeroed at/beyond the 45 (>= ~63-yard kicks)
    far = get_fg_wp(pl.DataFrame([_row(10, ytg) for ytg in (45, 50, 60)]))
    assert (far["fg_make_prob"].to_numpy() == 0).all()


@requires_models
def test_get_2pt_wp_columns() -> None:
    from sportsdataverse.nfl.nfl_fourth_down import _prepare

    td = _prepare(pl.DataFrame([_row(1, 2, sd=-6), _row(1, 1, sd=0)]).to_pandas())
    td = td.reset_index(drop=True)
    td["go_index"] = np.arange(len(td))
    out = get_2pt_wp(td)
    assert set(out.columns) == {"go_index", "yardline_100", "wp_td"}
    v = out["wp_td"].to_numpy().astype(float)
    finite = v[np.isfinite(v)]
    assert np.all((finite >= -1e-9) & (finite <= 1 + 1e-9))


@requires_models
def test_get_4th_down_probs_recommendation_membership(fourth_down_rows) -> None:
    out = get_4th_down_probs(fourth_down_rows)
    for c in (
        "go_wp",
        "punt_wp",
        "fg_wp",
        "go_boost",
        "go_wp_diff",
        "punt_wp_diff",
        "fg_wp_diff",
        "fourth_down_recommendation",
    ):
        assert c in out.columns
    _assert_prob_bounds(out)
    recs = out["fourth_down_recommendation"].dropna().unique().tolist()
    assert set(recs) <= {"go", "punt", "field_goal"}, recs
    # the recommended option's *_wp_diff is 0 (it is the argmax); the others <= 0.
    for _, r in out.iterrows():
        rec = r["fourth_down_recommendation"]
        if rec is None or (isinstance(rec, float) and np.isnan(rec)):
            continue
        diff = {"go": r["go_wp_diff"], "punt": r["punt_wp_diff"], "field_goal": r["fg_wp_diff"]}[rec]
        assert abs(diff) < 1e-9, (rec, diff)
        for k, v in (("go", r["go_wp_diff"]), ("punt", r["punt_wp_diff"]), ("field_goal", r["fg_wp_diff"])):
            if np.isfinite(v):
                assert v <= 1e-9, (k, v)


def test_empty_input_returns_empty_decision_cols() -> None:
    empty = pl.DataFrame(
        schema={
            k: pl.Float64
            if k not in ("game_id", "posteam", "defteam", "home_team", "away_team", "roof", "season_type")
            else pl.Utf8
            for k in _row().keys()
        }
    )
    out = get_4th_down_probs(empty)
    assert len(out) == 0
    assert "fourth_down_recommendation" in out.columns


@skip_if_no_live
@requires_models
def test_oracle_parity_2022() -> None:
    """Validate against nfl4th's shipped pre_computed_go_boost on a 2022 slice.

    Components (go_wp / fg_wp / punt_wp) must clear corr > 0.99; go_boost is a
    first-difference SNR-limited metric (corr ~0.98 ceiling, like wpa) so it is
    asserted at a looser bar.
    """
    import pandas as pd

    from sportsdataverse.nfl import load_nfl_pbp

    oracle_path = os.path.join("dev", "nfl4th_artifacts", "pre_computed_go_boost.parquet")
    if not os.path.exists(oracle_path):
        pytest.skip("oracle parquet not present")
    oracle = pd.read_parquet(oracle_path)
    o22 = oracle[oracle["game_id"].str.startswith("2022_")]
    pbp = load_nfl_pbp([2022]).to_pandas()
    m = pbp.merge(o22[["game_id", "play_id"]], on=["game_id", "play_id"], how="inner")
    m = m[(m["down"] == 4) & m["yardline_100"].notna() & m["spread_line"].notna()].reset_index(drop=True)
    # keep the slice quick but representative
    m = m.sample(n=min(800, len(m)), random_state=7).reset_index(drop=True)
    res = get_4th_down_probs(m)
    cmp = res[["game_id", "play_id", "go_wp", "fg_wp", "punt_wp", "go_boost"]].merge(
        o22, on=["game_id", "play_id"], suffixes=("_mine", "_o")
    )
    for c, bar in (("go_wp", 0.99), ("fg_wp", 0.99), ("punt_wp", 0.99), ("go_boost", 0.95)):
        a = cmp[c + "_mine"].to_numpy()
        b = cmp[c + "_o"].to_numpy()
        k = np.isfinite(a) & np.isfinite(b)
        corr = np.corrcoef(a[k], b[k])[0, 1]
        assert corr > bar, f"{c} corr={corr:.4f} below {bar}"
