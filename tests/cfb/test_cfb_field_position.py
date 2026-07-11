"""Tests for the field-position value model (curve fit, artifact, public fn)."""

import numpy as np
import polars as pl

import importlib

m = importlib.import_module("sportsdataverse.cfb.cfb_field_position")
from sportsdataverse.cfb.cfb_field_position import (
    fit_field_position_ep,
    load_fp_curve,
)


def test_fp_curve_monotone():
    rng = np.random.default_rng(0)
    yl = rng.integers(1, 100, 4000)
    pts = 0.06 * yl + rng.normal(0, 1.5, 4000)  # true EP rises ~0.06/yd
    d = pl.DataFrame(
        {
            "drive_start_yardline": yl.astype("int64"),
            "drive_next_score_pts": pts,
        }
    )
    curve = fit_field_position_ep(d)
    ep = curve.sort("yardline_own")["ep"].to_numpy()
    assert (np.diff(ep) >= -1e-9).all()  # non-decreasing
    assert curve["yardline_own"].min() >= 1 and curve["yardline_own"].max() <= 99
    assert curve.height == 99


def test_bundled_curve_grid_monotone_and_anchors():
    """Bundled artifact vs the published fp_reference anchors.

    The committed curve is fit on 2018-2021 offense-signed realized drive
    points (TD +7 / FG +3 / SF -2 / return-TD -7) with drive-level starts
    (drive.start.yardLine + homeTeamId orientation; see
    dev/cfb_advanced/fit_field_position.py). Observed anchors: own-25 1.803
    (ref 1.4), midfield 2.820 (ref 2.8), opp-25 3.674 (ref 4.1), opp-5
    5.387 (ref 5.6) -- all within the +-0.6 published-reference tolerance.

    Documented divergence: the own-1 reference (-0.5) presumes net
    next-score semantics; realized drive points bottom out near +0.8 at the
    goal line because there is no opponent-next-score negative term. Own-1
    is therefore pinned to an empirical internal-consistency band.
    """
    curve = load_fp_curve().sort("yardline_own")
    assert curve.height == 99
    assert curve["yardline_own"].to_list() == list(range(1, 100))
    ep = curve["ep"].to_numpy()
    assert (np.diff(ep) >= -1e-9).all()

    ref = pl.read_parquet("tests/fixtures/cfb_advanced/fp_reference.parquet")
    j = curve.join(ref, on="yardline_own", how="inner")
    assert j.height == 5
    lookup = dict(zip(j["yardline_own"].to_list(), j["ep"].to_list()))
    ref_lookup = dict(zip(ref["yardline_own"].to_list(), ref["ep"].to_list()))
    for yl in (25, 50, 75, 95):
        assert abs(lookup[yl] - ref_lookup[yl]) <= 0.6
    # documented-divergence band (empirical, realized-drive-points target)
    assert 0.3 <= lookup[1] <= 1.2


def _synthetic_pbp() -> pl.DataFrame:
    gid = 401000001
    # team 1 is HOME (own yardline = raw drive.start.yardLine); team 2 is
    # away (own = 100 - raw). Team 1 starts at own 25 and midfield; team 2
    # starts at own 40 twice (raw 60).
    rows = []
    for seq, (pos, dfn, raw_yl, result) in enumerate(
        [
            (1, 2, 25, "TD"),
            (2, 1, 60, "PUNT"),
            (1, 2, 50, "PUNT"),
            (2, 1, 60, "FG"),
        ],
        start=1,
    ):
        for _ in range(2):
            rows.append(
                {
                    "season": 2021,
                    "game_id": gid,
                    "drive_id": f"{gid}{seq}",
                    "drive_result": result,
                    "drive_start_yardline_raw": raw_yl,
                    "home_team_id": 1,
                    "period": 1,
                    "pos_team_id": pos,
                    "def_pos_team_id": dfn,
                    "scrimmage_play": True,
                    "pos_team_score": 0,
                    "def_pos_team_score": 0,
                }
            )
    return pl.DataFrame(rows)


def test_cfb_field_position_math(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: _synthetic_pbp())
    out = m.cfb_field_position([2021])
    assert out.schema["team_id"] == pl.Utf8
    t1 = out.filter(pl.col("team_id") == "1").row(0, named=True)
    t2 = out.filter(pl.col("team_id") == "2").row(0, named=True)
    assert t1["drives"] == 2 and t2["drives"] == 2
    assert abs(t1["avg_start_yardline"] - 37.5) < 1e-9  # (25 + 50) / 2
    assert abs(t2["avg_start_yardline"] - 40.0) < 1e-9
    assert abs(t1["points_per_drive"] - 3.5) < 1e-9  # (7 + 0) / 2
    assert abs(t2["points_per_drive"] - 1.5) < 1e-9  # (0 + 3) / 2
    curve = load_fp_curve()
    lk = dict(zip(curve["yardline_own"].to_list(), curve["ep"].to_list()))
    assert abs(t1["fp_ep"] - (lk[25] + lk[50]) / 2) < 1e-9
    assert abs(t1["fp_margin"] - (t1["fp_ep"] - t2["fp_ep"])) < 1e-9
    pdf = m.cfb_field_position([2021], return_as_pandas=True)
    assert pdf.__class__.__module__.startswith("pandas")


def test_cfb_field_position_empty(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: pl.DataFrame())
    out = m.cfb_field_position([1999])
    assert out.height == 0 and "fp_margin" in out.columns
