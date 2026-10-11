"""Tests for the nfl4th 4th-down decision surface.

The synthetic-input tests run against the bundled 2-pt model + FG grid + punt
distribution and the download-on-demand fd / wp models; they are gated on model
availability so the suite stays green offline (no cache + no network).  The
nfl4th oracle runs offline on the committed 2022 slice of nfl4th's shipped
``pre_computed_go_boost`` outputs: in CI on a 606-play subsample with the two
download-on-demand boosters replayed from recorded outputs, and locally on all
4,239 plays with the real boosters.
"""

from __future__ import annotations

from pathlib import Path

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


# --------------------------------------------------------------------------- #
# nfl4th oracle (gap-C-125; ledger gate fourth-down-oracles-committed)
# --------------------------------------------------------------------------- #
# The committed slice is nfl4th's own published output for every 2022 fourth down
# (`nfl4th_infrastructure/pre_computed_go_boost.rds`, 4,239 rows: nfl4th_go_wp /
# nfl4th_fg_wp / nfl4th_punt_wp / nfl4th_go_boost) joined to the nflverse-data
# `play_by_play_2022` inputs on (game_id, play_id); provenance in
# tests/nfl/fixtures/README.md. Floors are measured values rounded to the safe side at
# 1e-4, never lowered. Correlation alone survives an affine bias (`go_wp + 0.02`,
# `go_wp * 0.98` keep r = 0.9986), so each component also carries a level floor: MAE
# and max |diff|.
#
# Two tests share the oracle:
# * test_nfl4th_oracle_replay_2022 runs EVERYWHERE, CI included: every 7th play (606)
#   with the two download-on-demand boosters replayed from their recorded outputs
#   (tests/nfl/_booster_replay.py; a feature row the recording never saw fails).
# * test_nfl4th_oracle_full_slice_local_only scores all 4,239 plays with the real
#   boosters; it needs them in the local model cache and is skipped elsewhere.
NFL4TH_SLICE = Path(__file__).resolve().parent / "fixtures" / "nfl4th_2022_slice.parquet"
NFL4TH_REPLAY_STRIDE = 7
NFL4TH_REPLAY_FLOORS = {
    # column: (min corr, max MAE, max |diff|, min comparable)   measured corr / MAE / max (n)
    "go_wp": (0.995, 0.0126, 0.0879, 600),  # 0.998492 / 0.012505 / 0.087811 (606)
    "fg_wp": (0.995, 0.0133, 0.0876, 600),  # 0.998240 / 0.013238 / 0.087517 (606)
    "punt_wp": (0.995, 0.0123, 0.0874, 440),  # 0.998444 / 0.012272 / 0.087383 (443)
    "go_boost": (0.9634, 0.6674, 5.7337, 600),  # 0.963449 / 0.667391 / 5.733650 (pct-pts)
}
NFL4TH_FULL_FLOORS = {
    "go_wp": (0.995, 0.0122, 0.1097, 4200),  # 0.99862 / 0.012167 / 0.109654
    "fg_wp": (0.995, 0.0130, 0.1078, 4200),  # 0.99829 / 0.012910 / 0.107780
    "punt_wp": (0.995, 0.0125, 0.0945, 3000),  # 0.99849 / 0.012450 / 0.094482 (n=3,114 finite)
    "go_boost": (0.9579, 0.6820, 6.0470, 4200),  # 0.95799 / 0.681019 / 6.046908 (pct-pts)
}
#: sha256 of the boosters the replay was recorded from (nfl_4th_down_models release, cached 2026-06-24)
NFL4TH_BOOSTER_SHA256 = {
    "fd_model.ubj": "74020522809537e67a01e058215631c00fd4379d0c4e7db51386ccc5eec05f9d",
    "wp_model.ubj": "56444fc03114dccd9f01fae791408b8eac6636503614b5310359c1a3ae74da1f",
}
LOCAL_ONLY = pytest.mark.skipif(
    not (FD_MODEL_AVAILABLE and WP_MODEL_AVAILABLE),
    reason="LOCAL-ONLY: needs the nfl_4th_down_models fd/wp boosters (download-on-demand, not bundled, "
    "not cached in CI); CI runs test_nfl4th_oracle_replay_2022 instead",
)


def _check_oracle(scored, col: str, floors: dict) -> None:
    min_corr, max_mae, max_abs, min_n = floors[col]
    a = np.asarray(scored[col], dtype=float)
    b = np.asarray(scored[f"nfl4th_{col}"], dtype=float)
    k = np.isfinite(a) & np.isfinite(b)
    assert k.sum() >= min_n, f"{col}: only {k.sum()} comparable rows"
    d = np.abs(a[k] - b[k])
    corr = np.corrcoef(a[k], b[k])[0, 1]
    assert corr >= min_corr, f"{col} corr={corr:.5f} below {min_corr}"
    assert d.mean() <= max_mae, f"{col} MAE={d.mean():.6f} above {max_mae}"
    assert d.max() <= max_abs, f"{col} max|diff|={d.max():.6f} above {max_abs}"
    print("NFL4TH_ORACLE_OK", col, round(corr, 5), round(d.mean(), 6), round(d.max(), 6))


@pytest.fixture(scope="module")
def nfl4th_replay_scored():
    import xgboost

    import sportsdataverse.nfl.nfl_fourth_down as fd
    from tests.nfl._booster_replay import MODELS, REPLAY, KeyedDMatrix, ReplayBooster

    sub = pl.read_parquet(NFL4TH_SLICE).gather_every(NFL4TH_REPLAY_STRIDE)
    assert sub.height == 606 and (sub["down"] == 4).all()
    table = pl.read_parquet(REPLAY)
    boosters = {name: ReplayBooster(name, table) for name in MODELS}
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(xgboost, "DMatrix", KeyedDMatrix)
        mp.setattr(fd, "_try_load", lambda name: boosters.get(name))
        return get_4th_down_probs(sub)


@pytest.mark.parametrize("col", list(NFL4TH_REPLAY_FLOORS))
def test_nfl4th_oracle_replay_2022(nfl4th_replay_scored, col) -> None:
    """606 real 2022 fourth downs vs nfl4th's published values, boosters replayed: runs in CI."""
    _check_oracle(nfl4th_replay_scored, col, NFL4TH_REPLAY_FLOORS)


@pytest.fixture(scope="module")
def nfl4th_full_scored():
    s = pl.read_parquet(NFL4TH_SLICE)
    assert s.height == 4239 and (s["down"] == 4).all()
    return get_4th_down_probs(s)


@LOCAL_ONLY
@pytest.mark.parametrize("col", list(NFL4TH_FULL_FLOORS))
def test_nfl4th_oracle_full_slice_local_only(nfl4th_full_scored, col) -> None:
    """Every 2022 fourth down with the real boosters (local evidence; the replay above is the CI gate)."""
    _check_oracle(nfl4th_full_scored, col, NFL4TH_FULL_FLOORS)


@LOCAL_ONLY
def test_nfl4th_replay_was_recorded_from_the_cached_boosters_local_only() -> None:
    """A re-released booster must be re-recorded (tests/nfl/fixtures/record_nfl4th_replay.py)."""
    import hashlib

    from sportsdataverse.nfl.ep_wp import _model_cache_dir

    for name, want in NFL4TH_BOOSTER_SHA256.items():
        assert hashlib.sha256((_model_cache_dir() / name).read_bytes()).hexdigest() == want, name


@pytest.mark.filterwarnings("error::pandas.errors.PerformanceWarning")
def test_go_wp_on_a_full_nflverse_frame_is_not_fragmented():
    """A "first" agg over every one of nflverse's ~370 columns fragmented the frame (3 warnings a call)."""
    from pathlib import Path

    pbp = pl.read_parquet(Path(__file__).parents[1] / "fixtures" / "nfl_ep_wp" / "overtime_games.parquet")
    fourth = pbp.filter((pl.col("down") == 4) & pl.col("ydstogo").is_not_null() & pl.col("yardline_100").is_not_null())
    out = get_go_wp(fourth)
    assert len(out) == fourth.height and out["go_wp"].notna().all()


# --------------------------------------------------------------------------- #
# PAT spot by season (rules/nfl.yaml#nfl-2015-pat-snap-from-15)
# --------------------------------------------------------------------------- #
def _real_try_rows() -> pl.DataFrame:
    """Real nflverse PAT / 2-pt try rows (2014 BUF-CHI, 2019 DET-ARI) from the committed
    overtime-games capture, in the post-touchdown state shape ``get_2pt_wp`` takes."""
    from pathlib import Path

    from sportsdataverse.nfl.nfl_fourth_down import _EPWP_IN

    pbp = pl.read_parquet(Path(__file__).parents[1] / "fixtures" / "nfl_ep_wp" / "overtime_games.parquet")
    keep = [c for c in _EPWP_IN if c not in ("half_seconds_remaining", "game_seconds_remaining", "down")]
    return (
        pbp.filter(
            ((pl.col("extra_point_attempt") == 1) | (pl.col("two_point_attempt") == 1))
            & pl.col("season").is_in([2014, 2019])
            & pl.col("posteam").is_not_null()
        )
        .select([*keep, *[c for c in ("play_id", "qtr", "quarter_seconds_remaining") if c not in keep]])
        .with_columns(pl.lit(4).alias("down"), pl.lit(2).alias("ydstogo"), pl.lit(2).alias("yardline_100"))
        .sort("season", "play_id")
    )


@requires_models
def test_pat_make_prob_scored_at_3_before_2015_and_15_after(monkeypatch) -> None:
    from sportsdataverse.nfl import nfl_fourth_down as mod
    from sportsdataverse.nfl.nfl_fourth_down import _prepare, _two_pt_components

    rows = _real_try_rows()
    assert rows["season"].unique().sort().to_list() == [2014, 2019]
    td = _prepare(rows.to_pandas()).reset_index(drop=True)
    td["go_index"] = np.arange(len(td))

    seen: dict = {}
    real = mod._fg_make_prob

    def _spy(yl, roof, era):
        seen["yl"] = np.asarray(yl)
        return real(yl, roof, era)

    monkeypatch.setattr(mod, "_fg_make_prob", _spy)
    out = _two_pt_components(td)
    assert out is not None
    season = td["season"].to_numpy()
    assert np.array_equal(seen["yl"][season < 2015], np.full(int((season < 2015).sum()), 3.0))
    assert np.array_equal(seen["yl"][season >= 2015], np.full(int((season >= 2015).sum()), 15.0))
    # A 21-yard kick converts more often than a 33-yard one: the pre-2015 PAT is the surer point.
    conv = out.set_index("go_index").loc[np.arange(len(td)), "conv_1pt"].to_numpy()
    assert conv[season < 2015].min() > conv[season >= 2015].max()
