"""The shot-make fitter: its zone comparator, its gates and the committed fit report against the bundled boosters.

``tools/models/nba_shot_value/train_shot_make.py`` rebuilds the stats feed's zone triple from a shot's
location (the release has no zone columns) to score the zone table it replaces. The rebuild is checked
here on the committed 2022-23 shot chart, which carries the feed's own zones. The fit reports
(``shot_make_<lg>_holdout.json``) must describe the bundled boosters exactly, their gate lines must be
re-derivable from their season metrics, and a failure must be one the owner was asked to rule on, line for
line (``PENDING_OWNER_RULING``).
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_shot_value import SHOT_MAKE_MODELS, load_shot_make_model, _shot_make_levels

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "train_shot_make", _ROOT / "tools" / "models" / "nba_shot_value" / "train_shot_make.py"
)
tsm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tsm)
_FIX = _ROOT / "tests" / "fixtures" / "nba_shot_value" / "shotchart_2023.parquet"


def _chart() -> pl.DataFrame:
    return pl.read_parquet(_FIX).with_columns(
        pl.when(pl.col("shot_type").str.starts_with("3")).then(3).otherwise(2).alias("shot_value")
    )


def test_zone_triple_matches_the_feed_zones():
    """Observed 0.9975 of 41,587 shots on all three keys (0.998 / 0.9995 / 0.9994 one by one)."""
    z = tsm.zone_triple(_chart(), x="loc_x", y="loc_y")
    assert z.height == 41_587
    hit = (
        (z["zone_basic"] == z["shot_zone_basic"])
        & (z["zone_area"] == z["shot_zone_area"])
        & (z["zone_range"] == z["shot_zone_range"])
    )
    assert hit.mean() >= 0.997


def test_zone_predict_falls_back_to_the_range_then_the_overall_rate():
    table = pl.DataFrame(
        {"zone_basic": ["a", "a", "b"], "zone_area": ["x", "x", "y"], "zone_range": ["r", "r", "s"], "y": [1, 0, 1]}
    )
    scored = pl.DataFrame({"zone_basic": ["a", "c", "c"], "zone_area": ["x", "z", "z"], "zone_range": ["r", "s", "t"]})
    np.testing.assert_allclose(tsm.zone_predict(table, scored), [0.5, 1.0, 2 / 3])


def _metrics(lo: float, bias: float, slope: float) -> dict:
    return {"vs_zone_served": {"d_log_loss_lo": lo}, "model": {"large_bias": bias, "calibration_slope": slope}}


@pytest.mark.parametrize(
    ("lo", "bias", "slope", "fails"),
    [(0.001, 0.0, 1.0, 0), (0.0, 0.0, 1.0, 1), (0.001, 0.011, 1.0, 1), (0.001, 0.0, 1.11, 1), (-1.0, -0.02, 0.5, 3)],
)
def test_gate_fails_each_rule(lo, bias, slope, fails):
    assert len(tsm.gate("s", _metrics(lo, bias, slope))) == fails


@pytest.mark.parametrize(("shift", "slope"), [(0.3, 1.0), (0.0, 0.8)])
def test_calibration_recovers_a_known_intercept_and_slope(shift, slope):
    rng = np.random.default_rng(0)
    z = rng.normal(size=200_000)
    y = (rng.random(z.size) < 1 / (1 + np.exp(-(shift + slope * z)))).astype(float)
    a, s = tsm.calibration(y, 1 / (1 + np.exp(-z)))
    assert s == pytest.approx(slope, abs=0.02)
    if slope == 1.0:
        assert a == pytest.approx(shift, abs=0.03)


@pytest.mark.parametrize("league_id", sorted(SHOT_MAKE_MODELS))
def test_fit_report_describes_the_bundled_booster(league_id):
    cfg = tsm.LEAGUES[league_id]
    report = json.loads((tsm.HERE / f"shot_make_{cfg['lg']}_holdout.json").read_text(encoding="utf-8"))
    booster = load_shot_make_model(league_id)
    # the gate lines come from the season metrics with the module's thresholds, not from a hand edit
    gated = [(s, m) for s, m in report["seasons"].items() if m["split"] != "monitor"]
    assert [f for s, m in gated for f in tsm.gate(f"{m['split']} {s}", m)] == report["failed_gates"]
    monitor = [(s, m) for s, m in report["seasons"].items() if m["split"] == "monitor"]
    assert [f for s, m in monitor for f in tsm.gate(f"monitor {s}", m)] == report["monitor_gates"]
    assert report["gates"]["max_large_bias"] == tsm.MAX_LARGE_BIAS
    assert tuple(report["gates"]["slope_band"]) == tsm.SLOPE_BAND
    # every failure is one the owner is asked to rule on, line for line (values included)
    assert report["failed_gates"] == report["pending_owner_ruling"] == list(tsm.PENDING_OWNER_RULING.get(league_id, ()))
    shipped = report["shipped"]
    path = tsm.MODELS / SHOT_MAKE_MODELS[league_id]
    assert shipped["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert shipped["artifact"] == f"sportsdataverse/nba/models/{SHOT_MAKE_MODELS[league_id]}"
    assert shipped["shot_types"] == _shot_make_levels(booster)
    assert shipped["rounds"] == int(booster.attr("rounds")) == booster.num_boosted_rounds()
    assert ",".join(map(str, shipped["train_seasons"])) == booster.attr("train_seasons")
    assert int(booster.attr("holdout_season")) == shipped["holdout_season"] == cfg["holdout"]
    assert booster.attr("fitting_script") == "tools/models/nba_shot_value/train_shot_make.py"
    # every walk-forward season was scored by a fit on the seasons before it, never on itself
    for s, m in report["seasons"].items():
        if m["split"] != "monitor":
            assert max(m["train_seasons"]) < int(s)
            assert m["vs_zone_served"]["d_log_loss_lo"] > 0
    part = pl.read_parquet(tsm.HERE / f"shot_make_{cfg['lg']}_partition.parquet")
    assert part.schema["game_id"] == pl.Utf8
    seasons = dict(part.group_by("season").agg(pl.col("split").first()).iter_rows())
    assert {s for s, k in seasons.items() if k == "train"} == set(shipped["train_seasons"])
    assert seasons[cfg["holdout"]] == "holdout"
    imp = json.loads((tsm.HERE / f"shot_make_{cfg['lg']}_importance.json").read_text(encoding="utf-8"))
    assert set(imp) == {*booster.feature_names, "bias"}


def test_quoted_figures_are_the_report_s():
    """The figures the docstrings and the changelog quote: 3.0% to 3.9% on every NBA walk-forward season,
    3.0% on the 2026 holdout, gate 2 missed by more than a point on 2019, 2021, 2023 and the 2027 monitor."""
    report = json.loads((tsm.HERE / "shot_make_nba_holdout.json").read_text(encoding="utf-8"))
    rel = {int(s): m["vs_zone_served"]["rel_log_loss"] for s, m in report["seasons"].items() if m["split"] != "monitor"}
    assert 0.0295 <= min(rel.values()) and max(rel.values()) < 0.0395
    assert round(rel[2026], 3) == 0.030
    off = sorted(int(s) for s, m in report["seasons"].items() if abs(m["model"]["large_bias"]) > 0.01)
    assert off == [2019, 2021, 2023, 2027]


def test_wnba_report_records_why_it_is_not_shipped():
    """The WNBA was gated and failed (feed recoding in 2016-2020, the 2020 bubble, 2022's level):
    its report is committed as the record, and no booster ships."""
    report = json.loads((tsm.HERE / "shot_make_wnba_holdout.json").read_text(encoding="utf-8"))
    assert report["failed_gates"]
    assert "10" not in SHOT_MAKE_MODELS
    assert not (tsm.MODELS / tsm.LEAGUES["10"]["artifact"]).exists()
