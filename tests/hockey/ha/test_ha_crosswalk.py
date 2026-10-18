"""Offline tests for the HockeyAllsvenskan <-> SHL player-id alias, on committed real captures."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.hockey.ha import ha_player_crosswalk, parse_ha_leaders
from sportsdataverse.hockey.ha import ha_crosswalk_runtime as X
from sportsdataverse.hockey.shl.shl_parsers import parse_shl_player_crosswalk

FIXTURES = Path(__file__).parents[3] / "tests" / "fixtures"


@pytest.fixture(scope="module")
def shl() -> pl.DataFrame:
    body = json.loads((FIXTURES / "shl" / "statistics-v2__stats-info__module__players_summary.json").read_text("utf-8"))
    return parse_shl_player_crosswalk(body)


def test_ids_alias_and_unknown_player_is_null(shl: pl.DataFrame) -> None:
    xw = ha_player_crosswalk([214, 524, 4204, 214], shl_crosswalk=shl)
    assert xw["player_statnet_id"].to_list() == [214, 524, 4204]
    assert xw["shl_player_id"].to_list() == [214, 524, None]
    assert (
        xw["athlete_uuid"].to_list()[:2] == shl.filter(pl.col("player_id").is_in([214, 524]))["athlete_uuid"].to_list()
    )
    assert xw.schema["shl_player_id"] == pl.Int64


def test_ha_frame_in_pandas_out(shl: pl.DataFrame) -> None:
    body = json.loads((FIXTURES / "ha" / "api__player-leaderboard.json").read_text("utf-8"))
    leaders = parse_ha_leaders(body)
    xw = ha_player_crosswalk(leaders, shl_crosswalk=shl, return_as_pandas=True)
    assert len(xw) == leaders["player_statnet_id"].n_unique()


def test_needs_season_or_crosswalk_and_the_id_column() -> None:
    with pytest.raises(ValueError, match="season"):
        ha_player_crosswalk([1])
    with pytest.raises(ValueError, match="player_statnet_id"):
        ha_player_crosswalk(pl.DataFrame({"x": [1]}), shl_crosswalk=pl.DataFrame())


def test_season_path_calls_the_shl_crosswalk_once(monkeypatch: pytest.MonkeyPatch, shl: pl.DataFrame) -> None:
    calls = []
    monkeypatch.setattr(X, "shl_player_crosswalk", lambda season, game_type: calls.append((season, game_type)) or shl)
    ha_player_crosswalk([214], season=2026, game_type="playoff")
    assert calls == [(2026, "playoff")]
