"""Offline tests for the ESPN golf parsers (``sportsdataverse.golf.golf_espn_parsers``)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.golf.golf_espn_parsers import (
    GOLF_LEADERBOARD_SCHEMA,
    parse_golf_leaderboard,
    parse_golf_scoreboard,
)

FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "golf"


def _load(league: str, endpoint: str) -> dict:
    return json.loads((FIXTURES / league / "site-v2" / f"{endpoint}.json").read_text(encoding="utf-8"))


def test_leaderboard_one_row_per_player_with_string_ids():
    df = parse_golf_leaderboard(_load("pga", "leaderboard"))  # 2026 Masters, 91 players
    assert df.height == 91
    assert df.schema == pl.Schema(GOLF_LEADERBOARD_SCHEMA)
    assert df["event_id"].unique().to_list() == ["401811941"]
    assert df["player_id"].n_unique() == 91
    assert df["sort_order"].is_sorted()


def test_leaderboard_winner_row():
    df = parse_golf_leaderboard(_load("pga", "leaderboard"))
    top = df.row(0, named=True)
    assert top["player_id"] == "3470"
    assert top["position"] == "1"
    assert top["player_status"] == "STATUS_FINISH"
    assert top["to_par"] == -12
    assert top["to_par_display"] == "-12"
    assert top["total_strokes"] == 276
    assert top["total_strokes"] == sum(top[f"round_{i}"] for i in range(1, 5))


def test_leaderboard_missed_cut_has_null_weekend_rounds():
    df = parse_golf_leaderboard(_load("pga", "leaderboard"))
    cut = df.filter(pl.col("player_status") == "STATUS_CUT")
    assert cut.height == 37
    assert cut["round_3"].null_count() == cut.height
    assert cut["round_4"].null_count() == cut.height
    assert cut["round_2"].null_count() == 0
    assert cut["position"].null_count() == cut.height  # ESPN shows "-" for a player off the board
    assert cut["position_is_tie"].null_count() == cut.height
    tied = df.filter(pl.col("position").str.starts_with("T"))
    assert tied.height > 0
    assert tied["position_is_tie"].all()


@pytest.mark.parametrize("league, players", [("pga", 91), ("lpga", 60)])
def test_scoreboard_field(league, players):
    df = parse_golf_scoreboard(_load(league, "scoreboard"))
    assert df.height == players
    assert df.schema == pl.Schema(GOLF_LEADERBOARD_SCHEMA)
    assert df["player_id"].null_count() == 0
    # the scoreboard carries no position or player status; strokes are summed from the rounds
    assert df["position"].null_count() == players
    first = df.row(0, named=True)
    played = [first[f"round_{i}"] for i in range(1, 5) if first[f"round_{i}"] is not None]
    assert first["total_strokes"] == sum(played)


def test_scoreboard_and_leaderboard_agree_on_scores():
    sb = parse_golf_scoreboard(_load("pga", "scoreboard")).select("player_id", "to_par", "round_1")
    lb = parse_golf_leaderboard(_load("pga", "leaderboard")).select("player_id", "to_par", "round_1")
    assert sb.schema["player_id"] == lb.schema["player_id"]
    joined = sb.join(lb, on="player_id", suffix="_lb")
    assert joined.height == 91
    assert (joined["round_1"] == joined["round_1_lb"]).all()
    assert (joined["to_par"] == joined["to_par_lb"]).all()


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"events": []}, {"events": [{"id": "1", "competitions": []}]}, "x"],
)
def test_empty_payload_is_zero_rows_with_schema(payload):
    df = parse_golf_leaderboard(payload)
    assert df.height == 0
    assert df.schema == pl.Schema(GOLF_LEADERBOARD_SCHEMA)


def test_return_as_pandas():
    pdf = parse_golf_leaderboard(_load("pga", "leaderboard"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == 91
    assert list(pdf.columns) == list(GOLF_LEADERBOARD_SCHEMA)


def test_to_par_display_values():
    from sportsdataverse.golf.golf_espn_parsers import _to_par

    assert [_to_par(v) for v in ("-12", "E", "+3", "-", "--", None, "WD")] == [
        -12,
        0,
        3,
        None,
        None,
        None,
        None,
    ]


def _player(**fields) -> dict:
    return {"events": [{"id": 9, "competitions": [{"competitors": [{"id": 1, **fields}]}]}]}


def test_playoff_line_is_not_a_round():
    # a 54-hole event whose period-4 line is the playoff
    lines = [{"period": i, "value": 70.0, "displayValue": "E"} for i in (1, 2, 3)]
    lines.append({"period": 4, "value": 4.0, "displayValue": "E", "isPlayoff": True})
    row = parse_golf_leaderboard(_player(score="-6", linescores=lines)).row(0, named=True)
    assert row["round_4"] is None
    assert row["total_strokes"] == 210
    assert (row["event_id"], row["player_id"]) == ("9", "1")


@pytest.mark.parametrize(
    "payload",
    [
        {"events": [{"id": "1", "competitions": {"a": 1}}]},
        {"events": [{"id": "1", "competitions": ["x"]}]},
        {"events": [{"id": "1", "competitions": [{"competitors": "x"}]}]},
    ],
)
def test_malformed_nodes_are_zero_rows(payload):
    df = parse_golf_leaderboard(payload)
    assert df.height == 0
    assert df.schema == pl.Schema(GOLF_LEADERBOARD_SCHEMA)


def test_malformed_player_fields_never_raise():
    row = parse_golf_leaderboard(_player(athlete="x", status="x", score=None, linescores="x")).row(0, named=True)
    assert row["player_id"] == "1"
    assert row["player_name"] is None
    assert row["total_strokes"] is None


def test_leaderboard_wrapper_sends_league_as_a_query_param(monkeypatch):
    from sportsdataverse.golf import golf_espn_ext as ext

    seen: dict = {}

    def fake_get(url, params=None, **kwargs):
        seen.update(url=url, params=params)
        return _load("pga", "leaderboard")

    monkeypatch.setattr(ext, "_get", fake_get)
    df = ext.espn_golf_leaderboard(league="lpga", event_id="401835177")
    assert seen["url"] == "https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard"
    assert seen["params"]["league"] == "lpga"
    assert seen["params"]["event"] == "401835177"
    assert df.height == 91  # parsed through parse_golf_leaderboard
