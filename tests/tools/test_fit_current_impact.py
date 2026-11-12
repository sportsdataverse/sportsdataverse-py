"""The BB-D39 gate, re-derived from what ``tools/fit_current_impact.py`` wrote (run 2026-11-10).

``tests/fixtures/bb_current_impact/gate.parquet`` holds the 28-day forward squared error of every method at
every evaluation date of NBA regular seasons 2007-2026 (README there); ``fit.json`` the fitted SDs. Floors
sit under the observed values and are never lowered to pass: if a refit falls below one, debug the model.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import itertools
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

ci = importlib.import_module("sportsdataverse.nba.nba_current_impact")  # the package re-exports a function of that name
from sportsdataverse.nba.nba_box_prior import BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_current_impact", _ROOT / "tools" / "fit_current_impact.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)  # type: ignore[union-attr]
_FX = _ROOT / "tests" / "fixtures" / "bb_current_impact"
_METHODS = {
    "current", "current_no_drift", "current_no_team", "current_no_season",
    "window_prior", "season_to_date", "season_to_date_window_prior",
    "season_to_date_tuned", "season_to_date_window_prior_tuned",
}  # fmt: skip


@pytest.fixture(scope="module")
def gate() -> pl.DataFrame:
    return pl.read_parquet(_FX / "gate.parquet")


@pytest.fixture(scope="module")
def report() -> dict:
    return json.loads((_FX / "report.json").read_text())


def test_seasons_are_walk_forward_and_disjoint() -> None:
    """Constants fit on 2007-2013, gate on 2014-2026; the filter starts where the box prior is walk-forward."""
    assert fit.START > BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH
    assert not set(fit.FIT_SEASONS) & set(fit.GATE_SEASONS)
    assert max(fit.FIT_SEASONS) < min(fit.GATE_SEASONS) and min(fit.FIT_SEASONS) > fit.START


def test_constants_cite_the_fit(report) -> None:
    """The package constants are fit.json's SDs rounded to 3 significant figures; fit.json is this tool's fit."""
    f = json.loads((_FX / "fit.json").read_text())
    p = f["params"]
    assert p == report["fit"]["params"]
    assert f["fit_seasons"] == [min(fit.FIT_SEASONS), max(fit.FIT_SEASONS)] and f["start"] == fit.START
    for name, key in (("ENTRY", "entry_sd"), ("SEASON", "season_sd"), ("TEAM", "team_sd"), ("DRIFT", "drift_sd")):
        assert getattr(ci, f"CURRENT_IMPACT_{name}_SD") == pytest.approx(p[key], rel=0.01), name
    assert p["level_sd"] == ci.CURRENT_IMPACT_LEVEL_SD


def test_gate_covers_every_method_date_and_possession(gate) -> None:
    assert set(gate["method"].unique()) == _METHODS
    assert set(gate["season"].unique()) == set(range(2007, 2027))
    per = gate.group_by("method").agg(pl.len(), pl.col("n").sum())
    assert per["len"].n_unique() == 1 and per["n"].n_unique() == 1  # every method scored the same possessions
    assert gate.group_by("season").agg(pl.col("as_of").n_unique())["as_of"].min() >= 15


def test_report_rederives_from_gate(gate, report) -> None:
    """Every margin in report.json comes out of gate.parquet with the committed margin function."""
    for m, v in report["gate_vs_season_to_date"].items():
        r = fit.margin(gate, "season_to_date", m, fit.GATE_SEASONS)
        assert r["margin_bp"] == pytest.approx(v["margin_bp"], abs=1e-9)
        assert r["boot_se_bp"] == pytest.approx(v["boot_se_bp"], abs=1e-9)


def test_margin_resamples_whole_seasons(gate) -> None:
    """The point margin is the pooled SSE difference over possessions; the bootstrap draws one unit per season."""
    r = fit.margin(gate, "season_to_date", "current", fit.GATE_SEASONS, boot=2000)
    assert r["seasons"] == len(fit.GATE_SEASONS)
    pooled = gate.filter(pl.col("season").is_in(list(fit.GATE_SEASONS)))
    a = pooled.filter(pl.col("method") == "season_to_date")
    b = pooled.filter(pl.col("method") == "current")
    assert r["margin_bp"] == pytest.approx(1e4 * (a["sse"].sum() - b["sse"].sum()) / a["n"].sum())


def test_score_level_uses_only_reference_possessions() -> None:
    """``score``'s league level is the mean residual over ``ref``: test points never enter it."""
    off, de = np.arange(10, 15)[None, :].repeat(4, 0), np.arange(20, 25)[None, :].repeat(4, 0)
    ref = (off, de, np.array([1.0, 1.0, 1.0, 1.0]))
    r = {10: (5.0, 0.0)}
    sse1, _ = fit.score(r, (off, de, np.array([0.0, 0.0, 3.0, 3.0])), ref)
    sse2, _ = fit.score(r, (off, de, np.array([3.0, 3.0, 0.0, 0.0])), ref)
    assert sse1 == pytest.approx(sse2)  # level = 1 - 0.05 from ref; the prediction is 1.0 either way
    assert sse1 == pytest.approx(1 + 1 + 4 + 4)  # errors -1, -1, 2, 2


def test_gate_passes_and_floors(gate, report) -> None:
    """The BB-D39 gate (fixed before the run), re-derived from gate.parquet: margin > 0, bootstrap 2.5% > 0.

    Observed 2026-11-10: +5.53 bp (season-cluster bootstrap SE 0.35, 95% +4.85..+6.22), better in 13 of 13
    seasons. Floors sit under that and are never lowered.
    """
    r = fit.gate_result(gate)
    assert r["pass"] is True and report["gate_pass"] is True
    m = r["margin"]
    assert m["margin_bp"] == pytest.approx(report["gate_vs_season_to_date"]["current"]["margin_bp"], abs=1e-9)
    assert m["margin_bp"] > 4.0 and m["boot_ci95_bp"][0] > 3.0 and m["seasons_better"] >= 12
    assert m["per_season_t95_bp"][0] > 3.0
    assert r["by_days_in"]["84+"]["margin_bp"] > 2.0 and r["by_days_in"]["84+"]["boot_ci95_bp"][0] > 0
    assert r["ablations_vs_current"]["window_prior"]["margin_bp"] > 3.0  # the filter, not the prior alone
    for k in ("by_days_in", "current_vs_baselines", "ablations_vs_current"):
        for name, v in r[k].items():
            got = report["gate_by_days_in" if k == "by_days_in" else k][name]
            assert v["margin_bp"] == pytest.approx(got["margin_bp"], abs=1e-9), (k, name)
            assert v["boot_ci95_bp"] == pytest.approx(got["boot_ci95_bp"], abs=1e-9), (k, name)


@pytest.mark.parametrize(
    "base", ["season_to_date_window_prior", "season_to_date_tuned", "season_to_date_window_prior_tuned"]
)
def test_current_beats_the_stronger_baselines(gate, base) -> None:
    """Also against the season-to-date rating at the window prior and at a penalty tuned on 2007-2013 (FLOORS_TBD)."""
    m = fit.margin(gate, base, "current", fit.GATE_SEASONS)
    assert m["margin_bp"] > 0 and m["boot_ci95_bp"][0] > 0


def test_data_slices_reference_and_test_by_date(tmp_path) -> None:
    """``_Data.ref`` holds the REF_POSS possessions before the date; ``_Data.test`` exactly [date, date + 28)."""
    poss = pl.read_parquet(_FX / "nba_poss_2014_2015_slice.parquet")
    pri = pl.read_parquet(_FX / "nba_window_priors_2014_2015_slice.parquet").with_columns(
        pl.lit("window").alias("kind")
    )
    for s in (2014, 2015):
        poss.filter(pl.col("season") == s).write_parquet(tmp_path / f"poss_{s}.parquet")
        pri.write_parquet(tmp_path / f"priors_{s}.parquet")
    data = fit._Data(tmp_path, range(2014, 2016))
    allp = poss.sort("game_date", "game_id")
    for d in (dt.date(2014, 10, 28), dt.date(2014, 11, 4)):
        want = allp.filter(pl.col("game_date") < d).tail(fit.REF_POSS)
        _o, _d, y = data.ref(d)
        assert y.size == want.height and np.array_equal(y, want["points"].to_numpy().astype(float))
        t = allp.filter(
            pl.col("season") == 2015, pl.col("game_date") >= d, pl.col("game_date") < d + dt.timedelta(days=28)
        )
        assert np.array_equal(data.test(2015, d)[2], t["points"].to_numpy().astype(float)) and t.height > 0


def test_eval_dates_weekly_from_opening_night() -> None:
    d = fit.eval_dates(pl.Series([dt.date(2014, 10, 28), dt.date(2015, 4, 15)]))
    assert d[0] == dt.date(2014, 10, 28) and all((b - a).days == 7 for a, b in itertools.pairwise(d))
    assert d[-1] <= dt.date(2015, 4, 15) < d[-1] + dt.timedelta(days=7)


def test_gate_rows_carry_the_fit_they_were_scored_with(gate, tmp_path) -> None:
    """Every gate row's fingerprint matches fit.json / baseline_fit.json; a refit makes report refuse them."""
    fp = fit.fingerprints(_FX)
    assert gate.filter(pl.col("fingerprint") != pl.col("method").replace_strict(fp)).height == 0
    for name in ("gate.parquet", "fit.json", "baseline_fit.json"):
        (tmp_path / name).write_bytes((_FX / name).read_bytes())
    f = json.loads((tmp_path / "fit.json").read_text())
    f["params"]["season_sd"] *= 2
    (tmp_path / "fit.json").write_text(json.dumps(f))
    with pytest.raises(SystemExit, match="other parameters"):
        fit.report(tmp_path)
