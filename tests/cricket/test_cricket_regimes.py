"""ODI fielding-restriction regimes in the cricket win-probability model.

sdv-internal-refs rules/cricket.yaml: ``cricket-2005-odi-powerplays``,
``cricket-2011-odi-powerplay-window``, ``cricket-2012-odi-two-new-balls-four-out``,
``cricket-2015-odi-batting-powerplay-removed``.

The bundled surface carries the regime key but its regime rows are copies of the pooled
2002-2026 grid with null support (the Cricsheet corpus is not committed; the per-regime
refit is ``dev/league_ports/fit_cricket_resource_surface.py``). So the plumbing is tested
end to end -- date -> regime -> surface row -> constants -- and the pooled fallback is
pinned: today every regime falls back and the numbers are the pooled numbers.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

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


def test_regime_constants_fall_back_to_the_pooled_format_table():
    assert REGIME_CONSTANTS == {}  # placeholder until the per-regime refit
    for regime, _ in ODI_REGIMES:
        assert get_format("odi", regime) == FORMAT_TABLE["odi"]
    assert get_format("odi", POOLED_REGIME) == FORMAT_TABLE["odi"]


def test_regime_constants_override_when_present(monkeypatch):
    monkeypatch.setitem(REGIME_CONSTANTS, "odi_2015", {"par_score": 270.0, "sigma_set": 65.0})
    fc = get_format("odi", "odi_2015")
    assert (fc.par_score, fc.sigma_set, fc.sigma_chase) == (270.0, 65.0, FORMAT_TABLE["odi"].sigma_chase)
    assert get_format("odi") == FORMAT_TABLE["odi"]


def test_bundled_surface_carries_every_regime_and_the_pooled_grid():
    surf = _load_surface_regimes()
    assert surf.columns == ["fmt", "regime", "overs_left", "wickets_left", "resource", "support"]
    counts = {(r["fmt"], r["regime"]): r["len"] for r in surf.group_by("fmt", "regime").len().iter_rows(named=True)}
    assert counts[("odi", POOLED_REGIME)] == 51 * 11 and counts[("t20", POOLED_REGIME)] == 21 * 11
    for regime, _ in ODI_REGIMES:
        assert counts[("odi", regime)] == 51 * 11
    assert counts[("t20", T20_REGIME)] == 21 * 11
    assert _load_surface().shape == (51 * 11 + 21 * 11, 4)
    # the regime rows are copies of the pooled grid with no support: none is usable yet
    assert surf["support"].is_null().all()
    assert _usable_regimes(surf) == set()


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


def test_holdout_without_dates_scores_on_the_pooled_regime():
    h = pl.read_parquet(HOLDOUT)
    out = cricket_win_probability(h)
    assert out.schema["regime"] == pl.Utf8
    assert set(out.filter(pl.col("fmt") == "odi")["regime"].to_list()) == {POOLED_REGIME}
    assert set(out.filter(pl.col("fmt") == "t20")["regime"].to_list()) == {T20_REGIME}
    # the fixture has no match_date column at all; that is the same as a null date
    assert "match_date" not in h.columns
    assert out.equals(cricket_win_probability(h.with_columns(match_date=pl.lit(None, pl.Date))))


def test_dated_odi_states_select_their_regime_and_fall_back_to_the_pooled_surface():
    odi = pl.read_parquet(HOLDOUT).filter(pl.col("fmt") == "odi").head(3000)
    pooled = cricket_win_probability(odi)
    for date, regime in (
        (dt.date(2003, 3, 1), "odi_pre2005"),
        (dt.date(2013, 2, 1), "odi_2012"),
        ("2024-06-15", "odi_2015"),
    ):
        dated = cricket_win_probability(odi.with_columns(match_date=pl.lit(date)))
        assert set(dated["regime"].to_list()) == {regime}
        # the regime grid is a copy of the pooled one today, so the numbers are the pooled numbers
        assert dated["resources_left"].equals(pooled["resources_left"])
        assert dated["win_prob"].equals(pooled["win_prob"])


def test_a_usable_regime_grid_is_actually_selected(monkeypatch):
    # Give odi_2015 a thick grid whose resource is the pooled one plus 0.01: a 2024 state must
    # read it, a 2003 state must not.
    surf = _load_surface_regimes()
    bumped = surf.with_columns(
        resource=pl.when(pl.col("regime") == "odi_2015").then(pl.col("resource") + 0.01).otherwise(pl.col("resource")),
        support=pl.when(pl.col("regime") == "odi_2015").then(MIN_CELL_SUPPORT).otherwise(None).cast(pl.Int64),
    )
    from sportsdataverse.cricket import cricket_win_prob as mod

    monkeypatch.setattr(mod, "_load_surface_regimes", lambda: bumped)
    odi = pl.read_parquet(HOLDOUT).filter((pl.col("fmt") == "odi") & (pl.col("balls_bowled") < 240)).head(500)
    pooled = cricket_win_probability(odi)
    new = cricket_win_probability(odi.with_columns(match_date=pl.lit(dt.date(2024, 6, 15))))
    old = cricket_win_probability(odi.with_columns(match_date=pl.lit(dt.date(2003, 3, 1))))
    assert (new["resources_left"] - pooled["resources_left"]).round(6).unique().to_list() == [0.01]
    assert old["resources_left"].equals(pooled["resources_left"])


def test_trimmed_summary_fixture_round_trip():
    # a gzipped summary from the ESPN fixtures tree, if any, parses with a date or a null date
    hits = sorted(FIX.glob("espn/cricket/**/summary.json.gz"))
    for path in hits:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            st = cricket_match_state(json.load(fh), fmt="t20")
        assert st.schema == dict(STATE_SCHEMA)
