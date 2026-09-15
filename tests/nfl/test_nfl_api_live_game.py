"""Parsers for the api.nfl.com live game-statistics and game-detail routes.

Offline tests run the parsers on trimmed real bodies captured during a live
game (``tests/fixtures/nfl_api/``). One live smoke test per wrapper runs under
``SDV_PY_LIVE_TESTS=1`` against a completed game.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import nfl_api_parsers as P
from tests.conftest import skip_if_no_live

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_api"
GAME = "a9a890ed-4feb-11f1-abca-2c54536568a9"


def _load(name: str) -> dict:
    return json.loads((FIX / f"{name}.json").read_text(encoding="utf-8"))


def test_team_statistics_one_row_per_side():
    df = P.parse_nfl_live_team_statistics(_load("live_team_statistics"))
    assert df.height == 2 and df["side"].to_list() == ["away", "home"]
    assert df["game_id"].unique().to_list() == [GAME]
    assert {"offset", "team_id", "passing_attempts", "rushing_plays", "defensive_sacks"} <= set(df.columns)
    assert df.schema["offset"] == pl.Int64


def test_player_statistics_rows_carry_side_team_and_gsis_id():
    raw = _load("live_player_statistics")
    df = P.parse_nfl_live_player_statistics(raw)
    assert df.height == 6  # fixture keeps 3 players per side
    assert df.filter(pl.col("side") == "home")["team_id"].unique().to_list() == [raw["homeTeam"]["teamId"]]
    assert df["gsis_player_id"].str.starts_with("00-").all()
    assert {"person_id", "gsis_player_name", "offset"} <= set(df.columns)


def test_game_details_v2_is_one_flat_row():
    df = P.parse_nfl_game_details_v2(_load("game_details_v2"))
    assert df.height == 1 and df["id"][0] == GAME
    assert {"status", "season", "week", "venue_name"} <= set(df.columns)
    # optional sections flatten into prefixed columns
    assert any(c.startswith("home_team_standings_") for c in df.columns)
    assert any(c.startswith("drive_chart_") for c in df.columns)


def test_by_slug_matches_v2_shape():
    raw = _load("game_details_v2")
    a = P.parse_nfl_game_details_by_slug(raw)
    b = P.parse_nfl_game_details_v2(raw)
    assert a.columns == b.columns


def test_by_slug_v1_body_is_flat_not_data_wrapped():
    # /experience/v1/gamedetailsbyslug returns the game at the top level (only
    # /experience/v1/gamedetails/{game_id} wraps it under ``data``).
    raw = _load("game_details_by_slug")
    assert "data" not in raw and raw["id"] == GAME
    df = P.parse_nfl_game_details_by_slug(raw)
    assert df.height == 1 and df["id"][0] == GAME and df["status"][0] == raw["status"]
    assert not any(c.startswith("data_") for c in df.columns)
    assert {"season", "week", "venue_name", "summary_game_id"} <= set(df.columns)


@pytest.mark.parametrize(
    "fn",
    [P.parse_nfl_live_team_statistics, P.parse_nfl_live_player_statistics, P.parse_nfl_game_details_v2],
)
def test_empty_or_malformed_payload_is_zero_rows(fn):
    assert fn({}).height == 0
    assert fn(None).height == 0
    assert fn({}, return_as_pandas=True).shape[0] == 0


def test_pandas_round_trip():
    out = P.parse_nfl_live_team_statistics(_load("live_team_statistics"), return_as_pandas=True)
    assert out.__class__.__module__.startswith("pandas") and len(out) == 2


@skip_if_no_live
def test_live_wrappers_against_a_completed_game():
    from sportsdataverse.nfl import (
        nfl_game_details_by_slug,
        nfl_game_details_v2,
        nfl_live_player_statistics,
        nfl_live_team_statistics,
    )

    assert nfl_live_team_statistics(game_id=GAME).height == 2
    assert nfl_live_player_statistics(game_id=GAME).height > 20
    assert nfl_game_details_v2(game_id=GAME).height == 1
    assert nfl_game_details_by_slug(slug="broncos-at-chiefs-2026-reg-1").height == 1
