"""pwhl_game_info / pwhl_player_box / pwhl_player_info on real ``statviewfeed`` captures (B-M9).

The ``statviewfeed`` ``gameSummary`` and ``player`` replies are bare JSON objects, not ``SiteKit``
envelopes; the wrappers used to read them as ``SiteKit`` and returned (0, 0) for every game. The
captures are live replies (``tests/fixtures/hockeytech/README.md``): game 164 (TOR 2, NY 1,
2025-03-19) and player 12 (Megan Keller).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.pwhl import pwhl_api as api

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "hockeytech"


def _load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def calls(monkeypatch):
    """Serve the committed capture for the requested statviewfeed view; record each call."""
    seen: list = []
    files = {"gameSummary": "pwhl_svf_gamesummary_164.json", "player": "pwhl_svf_player_12.json"}

    def fake(league, feed, view, params=None):
        seen.append((league, feed, view, dict(params or {})))
        return _load(files[view])

    monkeypatch.setattr(api, "hockeytech_api", fake)
    return seen


def test_pwhl_game_info_rows_on_statviewfeed_capture(calls):
    df = api.pwhl_game_info(164)
    assert calls == [("pwhl", "statviewfeed", "gameSummary", {"game_id": 164})]
    assert dict(df.schema) == P.GAME_INFO_SCHEMA
    row = df.row(0, named=True)
    assert df.height == 1
    assert (row["game_id"], row["game_season_id"], row["game_status"]) == (164, 5, "Final")
    assert (row["home_team_id"], row["home_team"], row["home_score"]) == (6, "Toronto Sceptres", 2)
    assert (row["away_team_id"], row["away_team_abbr"], row["away_score"]) == (4, "NY", 1)
    assert (row["attendance"], row["started"], row["final"], row["has_shootout"]) == (8351, 1, 1, 0)


def test_pwhl_player_box_rows_on_statviewfeed_capture(calls):
    box = api.pwhl_player_box(164)
    assert calls == [("pwhl", "statviewfeed", "gameSummary", {"game_id": 164})]
    sk, gl = box["skaters"], box["goalies"]
    assert dict(sk.schema) == P.SKATER_BOX_SCHEMA and dict(gl.schema) == P.GOALIE_BOX_SCHEMA
    assert (sk.height, gl.height) == (38, 4)  # 19 skaters and 2 goalies per team
    assert sk["league"].unique().to_list() == ["pwhl"] and sk["game_id"].unique().to_list() == [164]

    # Every skater total reconciles with the team totals in the same reply.
    raw = _load("pwhl_svf_gamesummary_164.json")
    for side in ("homeTeam", "visitingTeam"):
        team = raw[side]
        mine = sk.filter(pl.col("team_id") == team["info"]["id"])
        assert mine.height == len(team["skaters"])
        for col, key in (("goals", "goals"), ("shots", "shots"), ("hits", "hits"), ("faceoff_wins", "faceoffWins")):
            assert mine[col].sum() == team["stats"][key], (side, col)

    # Goalies: each starter faced the other team's shots and allowed its goals; backups have no toi.
    info = api.pwhl_game_info(164)
    assert gl.schema["team_id"] == info.schema["home_team_id"]  # one id dtype on the join key
    g = gl.join(info.select("home_team_id", "home_score", "away_score"), how="cross")
    started = g.filter(pl.col("starting") == 1).sort("team_id")
    assert started["player_id"].to_list() == [155, 64]
    assert started["toi"].to_list() == ["58:59", "60:00"]
    assert started["time_on_ice"].to_list() == [59.0, 60.0]
    assert started.select(
        (
            pl.col("goals_against")
            == pl.when(pl.col("team_id") == pl.col("home_team_id")).then("away_score").otherwise("home_score")
        ).all()
    ).item()
    assert started["shots_against"].to_list() == [29, 28]
    assert gl.filter(pl.col("starting") == 0)["time_on_ice"].null_count() == 2

    # Derived faceoff columns; a skater with no attempts has a null percentage, never NaN.
    fo = sk.filter(pl.col("faceoff_attempts") > 0)
    assert (fo["faceoff_losses"] == fo["faceoff_attempts"] - fo["faceoff_wins"]).all()
    assert (
        sk.filter(pl.col("faceoff_attempts") == 0)["faceoff_pct"].null_count()
        == sk.filter(pl.col("faceoff_attempts") == 0).height
    )
    flanagan = sk.filter(pl.col("player_id") == 68).row(0, named=True)
    assert (flanagan["toi"], flanagan["time_on_ice"], flanagan["plus_minus"]) == ("21:54", 21.9, 1)


def test_pwhl_player_box_is_the_dict_the_pwhl_raw_producer_reads(calls):
    """fastRhockey-pwhl-raw ``final.py`` does ``box.get("skaters")`` / ``box.get("goalies")`` on a dict."""
    box = api.pwhl_player_box(164)
    assert isinstance(box, dict)
    assert len(box.get("skaters").to_dicts()) == 38 and len(box.get("goalies").to_dicts()) == 4
    pdf = api.pwhl_player_box(164, return_as_pandas=True)
    assert set(pdf) == {"skaters", "goalies"} and len(pdf["skaters"]) == 38


def test_pwhl_player_info_rows_on_statviewfeed_capture(calls):
    df = api.pwhl_player_info(12)
    assert calls == [("pwhl", "statviewfeed", "player", {"player_id": 12})]
    assert dict(df.schema) == P.PLAYER_INFO_SCHEMA
    row = df.row(0, named=True)
    assert df.height == 1
    assert (row["player_id"], row["person_id"], row["name"]) == (12, 14, "Megan Keller")
    assert (row["position"], row["shoots"], row["birthdate"]) == ("LD", "L", "1996-05-01")
    assert (row["nationality"], row["player_type"], row["team_name"]) == ("American", "skater", "Boston Fleet")


@pytest.mark.parametrize("payload", [None, {}, [], "x", {"details": []}, {"homeTeam": {"skaters": [1, None]}}])
def test_pwhl_game_info_player_box_player_info_empty_payloads_keep_schema(payload):
    assert dict(P.parse_game_info(payload).schema) == P.GAME_INFO_SCHEMA
    assert P.parse_game_info(payload).height == 0
    box = P.parse_player_box(payload)
    assert {k: v.height for k, v in box.items()} == {"skaters": 0, "goalies": 0}
    assert dict(box["goalies"].schema) == P.GOALIE_BOX_SCHEMA
    info = P.parse_player_info(payload)
    assert info.height == 0 and dict(info.schema) == P.PLAYER_INFO_SCHEMA


def test_pwhl_game_info_non_scalar_fields_become_null():
    """A list / dict where the feed normally ships a scalar never reaches pl.from_dicts (review)."""
    raw = _load("pwhl_svf_gamesummary_164.json")
    raw["details"]["venue"] = ["Coca-Cola Coliseum"]
    raw["details"]["attendance"] = {"n": 8351}
    raw["homeTeam"]["info"]["name"] = {"en": "Toronto Sceptres"}
    row = P.parse_game_info(raw).row(0, named=True)
    assert (row["game_id"], row["game_venue"], row["attendance"], row["home_team"]) == (164, None, None, None)
    assert row["away_team"] == "New York Sirens"


def test_pwhl_player_box_non_scalar_fields_become_null():
    raw = _load("pwhl_svf_gamesummary_164.json")
    raw["homeTeam"]["skaters"][0]["info"]["firstName"] = ["Kali"]
    raw["homeTeam"]["skaters"][0]["stats"]["toi"] = ["21:54"]
    sk = P.parse_player_box(raw)["skaters"]
    first = sk.filter(pl.col("player_id") == 68).row(0, named=True)
    assert (first["first_name"], first["toi"], first["time_on_ice"], first["last_name"]) == (
        None,
        None,
        None,
        "Flanagan",
    )
    assert sk.height == 38


def test_pwhl_player_box_without_game_id_is_empty():
    """No details.id -> both frames empty, never rows with a null game_id (review)."""
    raw = _load("pwhl_svf_gamesummary_164.json")
    del raw["details"]["id"]
    box = P.parse_player_box(raw)
    assert {k: v.height for k, v in box.items()} == {"skaters": 0, "goalies": 0}
    assert dict(box["skaters"].schema) == P.SKATER_BOX_SCHEMA


def test_pwhl_player_info_non_scalar_fields_become_null():
    raw = _load("pwhl_svf_player_12.json")
    raw["info"]["firstName"] = ["Megan"]
    raw["info"]["jerseyNumber"] = 5
    row = P.parse_player_info(raw).row(0, named=True)
    assert (row["player_id"], row["first_name"], row["jersey_number"], row["last_name"]) == (12, None, "5", "Keller")
    assert row["name"] == "Keller"
