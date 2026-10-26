"""ODI fielding-restriction regimes in the cricket win-probability model.

sdv-internal-refs rules/cricket.yaml: ``cricket-2005-odi-powerplays``,
``cricket-2011-odi-powerplay-window``, ``cricket-2012-odi-two-new-balls-four-out``,
``cricket-2015-odi-batting-powerplay-removed``.

The bundled surface and ``REGIME_CONSTANTS`` are the Cricsheet refit
(``tools/models/league_ports/fit_cricket_resource_surface.py``, male ODIs 2002-2026). A regime is
applied whole -- its own grid AND its fitted constants -- or not at all: the 13-month
``odi_2011`` window has a thick enough grid but fewer than 100 matches, so the fit emits no
constants for it and every 2011-12 ODI falls back to the pooled surface and constants.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cricket import cricket_win_prob as mod
from sportsdataverse.cricket.cricket_model_constants import (
    FORMAT_TABLE,
    ODI_REGIMES,
    POOLED_REGIME,
    REGIME_CONSTANTS,
    T20_REGIME,
    cricket_regime,
    get_format,
)
from sportsdataverse.cricket.cricket_win_prob import (
    MIN_CELL_SUPPORT,
    MIN_REGIME_CELLS,
    STATE_SCHEMA,
    _load_surface,
    _load_surface_regimes,
    _usable_regimes,
    cricket_match_state,
    cricket_win_probability,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures"
HOLDOUT = FIX / "league_ports/cricket_holdout.parquet"
FITTED = {"odi_pre2005", "odi_2005", "odi_2012", "odi_2015"}
THIN = "odi_2011"


@pytest.mark.parametrize(
    ("date", "regime"),
    [
        (None, POOLED_REGIME),
        (dt.date(2004, 12, 31), "odi_pre2005"),
        (dt.date(2005, 1, 1), "odi_2005"),
        (dt.date(2011, 9, 30), "odi_2005"),
        (dt.date(2011, 10, 1), "odi_2011"),
        (dt.date(2012, 10, 29), "odi_2011"),
        (dt.date(2012, 10, 30), "odi_2012"),
        (dt.date(2015, 7, 4), "odi_2012"),
        (dt.date(2015, 7, 5), "odi_2015"),
        (dt.date(2026, 10, 10), "odi_2015"),
    ],
)
def test_odi_regime_by_match_date(date, regime):
    assert cricket_regime("odi", date) == regime


def test_t20_is_one_regime_and_test_is_deferred():
    assert cricket_regime("t20", dt.date(2010, 1, 1)) == T20_REGIME == cricket_regime("T20", None)
    assert [r for r, _ in ODI_REGIMES] == ["odi_pre2005", "odi_2005", "odi_2011", "odi_2012", "odi_2015"]
    with pytest.raises(ValueError, match="Test cricket deferred"):
        cricket_regime("test", None)


def test_fitted_regime_constants_and_the_thin_window_without_them():
    assert set(REGIME_CONSTANTS) == FITTED and THIN not in REGIME_CONSTANTS
    pooled = FORMAT_TABLE["odi"]
    for regime in FITTED:
        fc = get_format("odi", regime)
        assert set(REGIME_CONSTANTS[regime]) == {"par_score", "sigma_set", "sigma_chase"}
        assert (fc.par_score, fc.sigma_set, fc.sigma_chase) != (pooled.par_score, pooled.sigma_set, pooled.sigma_chase)
        assert (fc.balls_total, fc.max_wickets, fc.name) == (300, 10, "odi")
    assert get_format("odi", THIN) == pooled
    assert get_format("odi", POOLED_REGIME) == pooled
    # the pooled par straddles the fitted regimes (rules/cricket.yaml#cricket-2012-odi-two-new-balls-four-out)
    assert REGIME_CONSTANTS["odi_pre2005"]["par_score"] < pooled.par_score < REGIME_CONSTANTS["odi_2015"]["par_score"]


def test_regime_constants_override_when_present(monkeypatch):
    monkeypatch.setitem(REGIME_CONSTANTS, THIN, {"par_score": 270.0, "sigma_set": 65.0})
    fc = get_format("odi", THIN)
    assert (fc.par_score, fc.sigma_set, fc.sigma_chase) == (270.0, 65.0, FORMAT_TABLE["odi"].sigma_chase)
    assert get_format("odi") == FORMAT_TABLE["odi"]


def test_bundled_surface_carries_every_regime_with_support():
    surf = _load_surface_regimes()
    assert surf.columns == ["fmt", "regime", "overs_left", "wickets_left", "resource", "support"]
    counts = {(r["fmt"], r["regime"]): r["len"] for r in surf.group_by("fmt", "regime").len().iter_rows(named=True)}
    assert counts[("odi", POOLED_REGIME)] == 51 * 11 and counts[("t20", POOLED_REGIME)] == 21 * 11
    for regime, _ in ODI_REGIMES:
        assert counts[("odi", regime)] == 51 * 11
    assert counts[("t20", T20_REGIME)] == 21 * 11
    assert _load_surface().shape == (51 * 11 + 21 * 11, 4)
    assert surf["support"].is_not_null().all() and (surf["support"] >= 0).all()
    # every ODI regime grid is thick by cells; the whole-regime rule still drops the one without constants
    cells = surf.group_by("regime").agg((pl.col("support") >= MIN_CELL_SUPPORT).sum().alias("c"))
    assert (cells["c"] >= MIN_REGIME_CELLS).all()
    assert _usable_regimes(surf) == FITTED | {T20_REGIME}
    assert _usable_regimes(surf, {THIN: {}}) == {THIN, T20_REGIME}
    # the regime grids are real fits, not copies of the pooled grid
    pooled = surf.filter(pl.col("regime") == POOLED_REGIME).select("fmt", "overs_left", "wickets_left", "resource")
    for regime in FITTED:
        own = surf.filter(pl.col("regime") == regime).join(pooled, on=["fmt", "overs_left", "wickets_left"])
        assert (own["resource"] - own["resource_right"]).abs().max() > 0.01


def test_thin_regime_rule_uses_the_stated_thresholds():
    surf = _load_surface_regimes()
    thick = surf.with_columns(
        support=pl.when(pl.col("regime") == "odi_2015").then(MIN_CELL_SUPPORT).otherwise(None).cast(pl.Int64)
    )
    assert _usable_regimes(thick) == {"odi_2015"}
    # one short of MIN_REGIME_CELLS supported cells is thin
    cells = thick.filter(pl.col("regime") == "odi_2015").with_row_index("i")
    thin_support = pl.when(pl.col("i") < MIN_REGIME_CELLS - 1).then(MIN_CELL_SUPPORT).otherwise(None).cast(pl.Int64)
    thin = pl.concat([thick.filter(pl.col("regime") != "odi_2015"), cells.with_columns(support=thin_support).drop("i")])
    assert _usable_regimes(thin) == set()


def test_match_state_carries_the_match_date_from_a_real_summary():
    with open(FIX / "espn/cricket/8048/site-v2/summary.json", encoding="utf-8") as fh:
        summary = json.load(fh)
    st = cricket_match_state(summary, fmt="t20")
    assert st.schema == dict(STATE_SCHEMA)
    assert st["match_date"].to_list() == [dt.date(2026, 5, 31)]
    assert cricket_win_probability(st)["regime"].to_list() == [T20_REGIME]


def test_holdout_dates_select_every_regime_and_no_date_is_pooled():
    h = pl.read_parquet(HOLDOUT)
    assert h.schema["match_date"] == pl.Date and h["match_date"].is_not_null().all()
    out = cricket_win_probability(h)
    assert set(out.filter(pl.col("fmt") == "odi")["regime"].to_list()) == FITTED | {THIN}
    assert set(out.filter(pl.col("fmt") == "t20")["regime"].to_list()) == {T20_REGIME}
    undated = cricket_win_probability(h.drop("match_date"))
    assert set(undated.filter(pl.col("fmt") == "odi")["regime"].to_list()) == {POOLED_REGIME}
    null_dated = cricket_win_probability(h.with_columns(match_date=pl.lit(None, pl.Date)))
    assert undated.drop("match_date").equals(null_dated.drop("match_date"))
    # the thin 2011-12 window is scored exactly as the pooled regime
    thin = out.filter(pl.col("regime") == THIN)
    assert thin.height > 1000
    same = undated.filter(out["regime"] == THIN)
    assert thin.drop("regime", "match_date").equals(same.drop("regime", "match_date"))


def test_dated_odi_states_select_their_regime_grid_and_constants():
    odi = pl.read_parquet(HOLDOUT).filter((pl.col("fmt") == "odi") & (pl.col("balls_bowled") < 240)).drop("match_date")
    odi = odi.head(3000)
    pooled = cricket_win_probability(odi)
    for date, regime in (
        (dt.date(2003, 3, 1), "odi_pre2005"),
        (dt.date(2013, 2, 1), "odi_2012"),
        ("2024-06-15", "odi_2015"),
    ):
        dated = cricket_win_probability(odi.with_columns(match_date=pl.lit(date)))
        assert set(dated["regime"].to_list()) == {regime}
        assert (dated["resources_left"] != pooled["resources_left"]).mean() > 0.5
        assert (dated["win_prob"] != pooled["win_prob"]).mean() > 0.5
    thin = cricket_win_probability(odi.with_columns(match_date=pl.lit(dt.date(2012, 1, 1))))
    assert set(thin["regime"].to_list()) == {THIN}
    assert thin["resources_left"].equals(pooled["resources_left"]) and thin["win_prob"].equals(pooled["win_prob"])


def test_a_regime_whose_grid_goes_thin_falls_back_to_the_pooled_surface(monkeypatch):
    surf = _load_surface_regimes()
    monkeypatch.setattr(mod, "_load_surface_regimes", lambda: surf.with_columns(support=pl.lit(None, pl.Int64)))
    odi = pl.read_parquet(HOLDOUT).filter((pl.col("fmt") == "odi") & (pl.col("balls_bowled") < 240)).drop("match_date")
    odi = odi.head(500)
    pooled = cricket_win_probability(odi)
    dated = cricket_win_probability(odi.with_columns(match_date=pl.lit(dt.date(2024, 6, 15))))
    assert set(dated["regime"].to_list()) == {"odi_2015"}
    assert dated["resources_left"].equals(pooled["resources_left"]) and dated["win_prob"].equals(pooled["win_prob"])


def test_trimmed_summary_fixture_round_trip():
    # a gzipped summary from the ESPN fixtures tree, if any, parses with a date or a null date
    hits = sorted(FIX.glob("espn/cricket/**/summary.json.gz"))
    for path in hits:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            st = cricket_match_state(json.load(fh), fmt="t20")
        assert st.schema == dict(STATE_SCHEMA)
