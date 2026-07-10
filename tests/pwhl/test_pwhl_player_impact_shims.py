"""Offline tests for the PWHL by-reference player-impact shims."""

from __future__ import annotations

import functools
from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
from sportsdataverse.nhl.nhl_war import nhl_skater_war
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.pwhl.pwhl_player_impact import (
    pwhl_goalie_gsax,
    pwhl_skater_rapm,
    pwhl_skater_war,
    pwhl_special_teams_value,
    pwhl_unit_ratings,
    pwhl_xg,
)

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def test_pwhl_xg_is_a_league_pwhl_partial_of_nhl_xg():
    assert isinstance(pwhl_xg, functools.partial)
    assert pwhl_xg.func is nhl_xg
    assert pwhl_xg.keywords.get("league") == "pwhl"


def test_pwhl_goalie_gsax_is_a_league_pwhl_partial_of_nhl_goalie_gsax():
    assert isinstance(pwhl_goalie_gsax, functools.partial)
    assert pwhl_goalie_gsax.func is nhl_goalie_gsax
    assert pwhl_goalie_gsax.keywords.get("league") == "pwhl"


def test_pwhl_xg_runs_on_a_pwhl_shaped_synthetic_frame_via_borrowed_nhl_boosters():
    pbp = pl.read_parquet(FIX / "pbp_sample.parquet")
    out = pwhl_xg(pbp, model_dir=MODELS)
    assert out.filter(pl.col("xg").is_not_null()).height > 0


def test_pwhl_rapm_family_returns_documented_empty_frame_on_empty_shifts(capsys):
    empty_pbp, empty_shifts = pl.DataFrame(), pl.DataFrame()
    rapm = pwhl_skater_rapm(empty_pbp, empty_shifts)
    assert rapm.height == 0
    assert set(rapm.columns) == set(nhl_skater_rapm(pl.DataFrame(), pl.DataFrame()).columns)

    units = pwhl_unit_ratings(empty_pbp, empty_shifts)
    assert units.height == 0
    assert set(units.columns) == set(nhl_unit_ratings(pl.DataFrame(), pl.DataFrame()).columns)

    st = pwhl_special_teams_value(empty_pbp, empty_shifts)
    assert st.height == 0
    assert set(st.columns) == set(nhl_special_teams_value(pl.DataFrame(), pl.DataFrame()).columns)

    war = pwhl_skater_war(empty_pbp, empty_shifts)
    assert war.height == 0
    assert set(war.columns) == set(nhl_skater_war(pl.DataFrame(), pl.DataFrame()).columns)


def test_pwhl_rapm_family_runs_when_shifts_coverage_is_sufficient():
    pbp = pl.read_parquet(FIX / "pbp_sample.parquet")
    shifts = pl.read_parquet(FIX / "shifts_sample.parquet")
    rapm = pwhl_skater_rapm(pbp, shifts, model_dir=MODELS)
    assert rapm.height > 0
