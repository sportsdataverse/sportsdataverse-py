"""Offline checks of ``tools/fit_nhl_replacement_level.py`` (the fit itself needs the network)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_rep", _ROOT / "tools" / "fit_nhl_replacement_level.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)


def test_ev_rate_pools_5v5_xg_per_team():
    stints = pl.DataFrame(
        {
            "strength_state": ["5v5", "5v5", "5v4", "5v5"],
            "duration": [1800, 1800, 600, 0],
            "xgf_home": [1.0, 0.5, 9.0, 9.0],
            "xgf_away": [0.5, None, 9.0, 9.0],
        }
    )
    rate, secs = fit.ev_rate(stints)
    assert secs == 3600  # the 5v4 and zero-length stints are dropped
    assert rate == pytest.approx(2.0 * 3600.0 / (2 * 3600))  # two teams' xG over one hour each


def test_replacement_pool_is_below_the_regulars_and_toi_weighted():
    rapm = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4],
            "xg_rapm_off": [0.5, 0.4, -0.1, -0.4],
            "xg_rapm_def": [0.2, 0.1, 0.0, -0.3],
            "toi_minutes": [900.0, 800.0, 300.0, 100.0],
        }
    )
    off, dfn, n = fit.replacement_pool(rapm, 2)  # players 1 and 2 are the regulars
    assert n == 2
    assert off == pytest.approx((-0.1 * 300 - 0.4 * 100) / 400)
    assert dfn == pytest.approx((0.0 * 300 - 0.3 * 100) / 400)
    # charged against its own level, the pool's TOI-weighted EV GAR is zero
    pool = rapm.filter(pl.col("player_id") > 2)
    gar = ((pool["xg_rapm_off"] - off + pool["xg_rapm_def"] - dfn) * pool["toi_minutes"] / 60.0).sum()
    assert gar == pytest.approx(0.0)


def test_a_season_without_5v5_time_fails_the_fit(monkeypatch, capsys):
    empty = pl.DataFrame(
        schema={"strength_state": pl.Utf8, "duration": pl.Int64, "xgf_home": pl.Float64, "xgf_away": pl.Float64}
    )
    monkeypatch.setattr(fit, "load_nhl_shifts", lambda seasons: pl.DataFrame())
    monkeypatch.setattr(fit, "load_nhl_pbp_full", lambda seasons: pl.DataFrame())
    monkeypatch.setattr(fit, "nhl_xg", lambda pbp: pbp)
    monkeypatch.setattr(fit, "build_stints", lambda shifts, scored: empty)
    assert fit.main(["--seasons", "2025"]) == 1
    assert "2025: no 5v5 stints" in capsys.readouterr().err


def _fake_season(monkeypatch, stints, rapm):
    monkeypatch.setattr(fit, "load_nhl_shifts", lambda seasons: pl.DataFrame())
    monkeypatch.setattr(fit, "load_nhl_pbp_full", lambda seasons: pl.DataFrame({"home_abbr": ["BUF"]}))
    monkeypatch.setattr(fit, "nhl_xg", lambda pbp: pbp)
    monkeypatch.setattr(fit, "build_stints", lambda shifts, scored: stints)
    monkeypatch.setattr(fit, "nhl_skater_rapm", lambda *a, **k: rapm)


_STINTS = pl.DataFrame({"strength_state": ["5v5"], "duration": [3600], "xgf_home": [1.0], "xgf_away": [1.0]})
_RAPM = pl.DataFrame(
    {"player_id": [1], "xg_rapm_off": [0.1], "xg_rapm_def": [0.1], "toi_minutes": [60.0]},
)


def test_a_season_without_5v5_xg_fails_the_fit(monkeypatch, capsys):
    _fake_season(monkeypatch, _STINTS.with_columns(xgf_home=pl.lit(None, pl.Float64), xgf_away=pl.lit(0.0)), _RAPM)
    assert fit.main(["--seasons", "2025"]) == 1
    assert "2025: no 5v5 xG" in capsys.readouterr().err


def test_an_empty_replacement_pool_fails_the_fit(monkeypatch, capsys):
    _fake_season(monkeypatch, _STINTS, _RAPM)  # one skater, 18 regulars for the one team: no pool
    assert fit.main(["--seasons", "2025"]) == 1
    assert "2025: no replacement pool" in capsys.readouterr().err
