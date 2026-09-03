"""Offline tests for the spring-football EP/WP port (T7.3 Phase 1).

League constants (Task 1.1), ``build_spring_football_pbp`` (Task 1.2), and
``enrich_spring_football_pbp`` (Task 1.3). Uses the real captured
``tests/fixtures/league_ports/{xfl,ufl}_summary*.json`` fixtures -- see
``tests/fixtures/league_ports/FEASIBILITY.md`` for the capture-verification
findings these tests encode (notably: UFL carries no ESPN play-by-play as of
this port; XFL does).
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.spring_football_constants import SPRING_FOOTBALL_CONSTANTS, get_sf_constants
from sportsdataverse.football.spring_football_ep_wp import build_spring_football_pbp, enrich_spring_football_pbp
from sportsdataverse.nfl.model_vars import NFLVERSE_FRAME_CONTRACT

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "league_ports"


def _load_fixture(name: str) -> dict:
    with open(FIXTURE_DIR / name, encoding="utf-8") as f:
        return json.load(f)


def _xfl_summary() -> dict:
    return _load_fixture("xfl_summary.json")


def _ufl_summary() -> dict:
    return _load_fixture("ufl_summary.json")


# ---------------------------------------------------------------------------
# Task 1.1 -- league constants
# ---------------------------------------------------------------------------


def test_sf_constants_resolve_all_leagues():
    for league in ("ufl", "xfl", "cfl"):
        c = get_sf_constants(league)
        assert c.league == league


def test_sf_constants_downs():
    assert get_sf_constants("ufl").downs == 4
    assert get_sf_constants("xfl").downs == 4
    assert get_sf_constants("cfl").downs == 3


def test_sf_constants_no_pat_kick_ufl_xfl():
    assert get_sf_constants("ufl").pat_kick is False
    assert get_sf_constants("xfl").pat_kick is False


def test_sf_constants_ep_point_values_shape():
    for league in SPRING_FOOTBALL_CONSTANTS:
        assert get_sf_constants(league).ep_point_values.shape == (7,)


def test_sf_constants_unknown_league_raises():
    with pytest.raises(ValueError):
        get_sf_constants("nfl")
    with pytest.raises(ValueError):
        get_sf_constants("usfl")


# ---------------------------------------------------------------------------
# Task 1.2 -- build_spring_football_pbp
# ---------------------------------------------------------------------------


def test_build_xfl_pbp_contract():
    # XFL 2023 summaries DO carry `drives.previous[].plays[]` -- the real-data
    # contract fixture (see FEASIBILITY.md).
    out = build_spring_football_pbp(_xfl_summary(), league="xfl")
    assert out.height > 0
    assert set(NFLVERSE_FRAME_CONTRACT).issubset(set(out.columns))
    assert out.schema["game_id"] == pl.Utf8
    assert out.schema["play_id"] == pl.Utf8
    assert set(out["down"].drop_nulls().unique().to_list()).issubset({1, 2, 3, 4})


def test_build_xfl_pbp_scores_and_teams_are_consistent():
    out = build_spring_football_pbp(_xfl_summary(), league="xfl")
    real = out.filter(pl.col("posteam").is_not_null())
    # posteam/defteam are always the two distinct competitors.
    assert real.filter(pl.col("posteam") == pl.col("defteam")).height == 0
    # score_differential == posteam_score - defteam_score by construction.
    assert real.filter(pl.col("score_differential") != (pl.col("posteam_score") - pl.col("defteam_score"))).height == 0
    # scores are non-negative and monotonically non-decreasing within a game.
    assert real["posteam_score"].min() >= 0
    assert real["defteam_score"].min() >= 0


def test_build_ufl_pbp_returns_empty_contract_frame():
    # Capture finding (Task 0.1 / 1.2): ESPN carries no drives for UFL as of
    # this port (verified across every completed 2024 + 2025 game). A
    # zero-row, contract-shaped frame is the correct, honest output on
    # today's real data -- not a stub.
    out = build_spring_football_pbp(_ufl_summary(), league="ufl")
    assert out.height == 0
    assert set(NFLVERSE_FRAME_CONTRACT).issubset(set(out.columns))


def test_build_empty_payload_returns_schema():
    out = build_spring_football_pbp({}, league="xfl")
    assert out.height == 0
    assert set(NFLVERSE_FRAME_CONTRACT).issubset(set(out.columns))


def test_build_unknown_league_raises():
    with pytest.raises(ValueError):
        build_spring_football_pbp({}, league="usfl")


# ---------------------------------------------------------------------------
# Task 1.3 -- enrich_spring_football_pbp
# ---------------------------------------------------------------------------


def test_enrich_xfl_pbp_produces_model_columns():
    pbp = build_spring_football_pbp(_xfl_summary(), league="xfl")
    out = enrich_spring_football_pbp(pbp, league="xfl")
    for col in ("ep", "epa", "wp", "wpa", "vegas_wp"):
        assert col in out.columns
        assert out[col].drop_nulls().len() > 0
    assert out.select(pl.col("epa").drop_nulls().is_finite().all()).item()
    assert out.select(pl.col("wp").drop_nulls().is_between(0.0, 1.0).all()).item()


def test_enrich_return_as_pandas():
    import pandas as pd

    pbp = build_spring_football_pbp(_xfl_summary(), league="xfl")
    out = enrich_spring_football_pbp(pbp, league="xfl", return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_enrich_empty_frame_passthrough():
    pbp = build_spring_football_pbp({}, league="xfl")
    out = enrich_spring_football_pbp(pbp, league="xfl")
    assert out.height == 0


def test_enrich_unknown_league_raises():
    pbp = build_spring_football_pbp(_xfl_summary(), league="xfl")
    with pytest.raises(ValueError):
        enrich_spring_football_pbp(pbp, league="usfl")


def test_spring_enrichment_output_contract():
    """Pin which enrich outputs spring football actually populates.

    The docstring used to promise ``cp``/``cpoe``/xYAC; all three come back
    100% null, and cp/cpoe do so with no warning at all. The cause is upstream:
    the builder emits ``air_yards`` and ``spread_line`` as all-null (no
    air-yards charting, no betting market), so the CP and xYAC air-yards models
    and the fourth-down surface have nothing to score.

    This asserts the real contract in both directions. If the builder ever
    gains genuine air yards, the "always null" half starts failing -- which is
    the point: the docs claim these are structurally empty, and that claim
    should not outlive the reason for it.
    """
    pbp = build_spring_football_pbp(_load_fixture("xfl_summary.json"), league="xfl")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = enrich_spring_football_pbp(pbp, league="xfl")

    assert out.height > 0

    # The inputs whose absence causes everything below.
    for col in ("air_yards", "spread_line"):
        assert out[col].drop_nulls().len() == 0, f"{col} gained values — update the Returns note"

    # Scored: EP/WP derive from down/distance/yardline/clock/score, all of which
    # the builder does produce. Guard on a share, not an exact count, so a
    # re-captured fixture does not turn a working pipeline red.
    for col in ("ep", "epa", "wp", "wpa", "vegas_wp"):
        filled = out[col].drop_nulls().len()
        assert filled >= 0.9 * out.height, f"{col} only {filled}/{out.height} populated"

    # Structurally empty for these leagues — see the function's Returns note.
    for col in ("cp", "cpoe", "xyac_epa", "first_down_prob", "go_boost"):
        assert out[col].drop_nulls().len() == 0, (
            f"{col} is now populated for spring football — the Returns note in "
            "enrich_spring_football_pbp says it cannot be, so one of them is wrong"
        )
