"""Offline tests for the ESPN soccer parsers (payload-agnostic against captured fixtures)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

FIX = Path(__file__).parent / "fixtures" / "espn" / "soccer"


def _load(slug: str, host: str, name: str) -> dict:
    return json.loads((FIX / slug / host / f"{name}.json").read_text(encoding="utf-8"))


def test_parse_soccer_scoreboard_returns_one_row_per_match():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    payload = _load("eng.1", "site-v2", "scoreboard")
    df = parse_soccer_scoreboard(payload)
    assert isinstance(df, pl.DataFrame)
    expected = {"event_id", "date", "home_team", "away_team", "home_score", "away_score", "status"}
    assert expected <= set(df.columns), f"missing {expected - set(df.columns)}"
    assert df.height == len(payload.get("events", []))


def test_parse_soccer_scoreboard_empty_payload_zero_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    df = parse_soccer_scoreboard({})
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_soccer_scoreboard_pandas_flag():
    import pandas as pd
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    out = parse_soccer_scoreboard(_load("eng.1", "site-v2", "scoreboard"), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_parse_soccer_standings_flattens_table_with_group_column():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    df = parse_soccer_standings(_load("eng.1", "site-v2", "standings"))
    assert isinstance(df, pl.DataFrame)
    assert {"team", "group"} <= set(df.columns)
    assert df.height >= 18  # 20 EPL clubs (>=18 tolerates a mid-season capture)
    assert any(c in df.columns for c in ("points", "rank", "games_played", "overall"))


def test_parse_soccer_standings_multi_group_mls_has_two_groups():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    df = parse_soccer_standings(_load("usa.1", "site-v2", "standings"))
    assert df["group"].n_unique() >= 2  # Eastern + Western conferences


def test_parse_soccer_standings_empty_zero_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    assert parse_soccer_standings({}).height == 0


def test_parse_soccer_summary_dispatch_all_sections_returns_dict():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary(_load("eng.1", "site-v2", "summary"))
    assert isinstance(out, dict)
    for sec in ("header", "lineups", "key_events", "team_stats"):
        assert sec in out and isinstance(out[sec], pl.DataFrame), sec


def test_parse_soccer_summary_key_events_has_rows_and_columns():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    ke = parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="key_events")
    assert isinstance(ke, pl.DataFrame)
    assert {"type", "clock", "team_id"} <= set(ke.columns)
    assert ke.height >= 1


def test_parse_soccer_summary_lineups_one_row_per_player():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    lu = parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="lineups")
    assert {"athlete", "team_id", "starter"} <= set(lu.columns)
    assert lu.height >= 22


def test_parse_soccer_summary_unknown_section_returns_empty_frame():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    assert parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="nope").height == 0


def test_parse_soccer_summary_empty_payload():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary({})
    assert isinstance(out, dict) and out["key_events"].height == 0


def test_parse_soccer_summary_remaining_sections_present():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary(_load("eng.1", "site-v2", "summary"))
    for sec in ("commentary", "leaders", "standings", "head_to_head", "last_five", "game_info"):
        assert sec in out and isinstance(out[sec], pl.DataFrame), sec


def test_parse_soccer_summary_commentary_has_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    c = parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="commentary")
    assert c.height >= 1 and "text" in c.columns


def test_parse_soccer_summary_shootout_section_for_knockout():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary(_load("uefa.champions", "site-v2", "summary"))
    assert "shootout" in out and isinstance(out["shootout"], pl.DataFrame)


def test_parse_soccer_teams_one_row_per_team():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_teams

    df = parse_soccer_teams(_load("eng.1", "site-v2", "teams"))
    assert {"team_id", "display_name", "abbreviation"} <= set(df.columns)
    assert df.height >= 18


def test_parse_soccer_team_roster_one_row_per_player():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_team_roster

    df = parse_soccer_team_roster(_load("eng.1", "site-v2", "team_roster"))
    assert isinstance(df, pl.DataFrame)
    assert "athlete_id" in df.columns or "id" in df.columns


def test_parse_soccer_teams_empty_zero_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_teams

    assert parse_soccer_teams({}).height == 0


# --- W1: event coordinates + play details ----------------------------------

_COORD_COLS = [
    "field_position_x",
    "field_position_y",
    "field_position2_x",
    "field_position2_y",
    "goal_position_x",
    "goal_position_y",
]


def _summary(league: str) -> dict:
    return json.loads((FIX / league / "site-v2" / "summary.json").read_text(encoding="utf-8"))


def test_key_events_keep_coordinates_float64():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    ke = parse_soccer_summary(_summary("eng.1"), section="key_events")
    for c in _COORD_COLS + ["source_id", "source_description"]:
        assert c in ke.columns
    for c in _COORD_COLS:
        assert ke.schema[c] == pl.Float64
    # end location sits on the goal line (x = 0) with y > 0
    assert ke.filter(pl.col("field_position2_y") > 0).height >= 1


def test_key_events_penalty_spot_coordinates():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    found = 0
    for lg in ("eng.1", "uefa.champions"):
        ke = parse_soccer_summary(_summary(lg), section="key_events")
        pen = ke.filter(pl.col("type").str.starts_with("Penalty"))
        for r in pen.iter_rows(named=True):
            # ESPN's (0, 0) placeholder is kept faithfully; located ones sit on the spot.
            if r["field_position_x"] > 0:
                assert r["field_position_x"] == 0.23
                assert r["field_position_y"] == 0.5
                found += 1
    assert found >= 1


def test_commentary_play_block_columns():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    raw = _summary("eng.1")
    cm = parse_soccer_summary(raw, section="commentary")
    for c in ["play_id", "play_type", "team_name", "field_position_x", "athlete_id", "athlete_name"]:
        assert c in cm.columns
    assert cm.columns[:4] == ["sequence", "time_display", "time_value", "text"]
    with_play = [i for i in raw["commentary"] if i.get("play")]
    assert len(with_play) >= 70
    assert cm.filter(pl.col("play_id").is_not_null()).height == len(with_play)
    assert cm.filter(pl.col("play_id").is_null()).height == len(raw["commentary"]) - len(with_play)
    expected = sum(
        1 for i in with_play if (i["play"].get("fieldPositionX") or 0) > 0 or (i["play"].get("fieldPositionY") or 0) > 0
    )
    got = cm.filter((pl.col("field_position_x") > 0) | (pl.col("field_position_y") > 0)).height
    assert got == expected > 0


def test_key_events_and_commentary_coordinates_agree():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    raw = _summary("eng.1")
    ke = parse_soccer_summary(raw, section="key_events")
    cm = parse_soccer_summary(raw, section="commentary")
    j = ke.join(cm.drop_nulls("play_id"), left_on="id", right_on="play_id", suffix="_cm")
    assert j.height > 0
    for c in ("field_position_x", "field_position_y"):
        # ESPN's (0.0, 0.0) "no location" pairs are null in BOTH frames (rules/epl.yaml
        # epl-2009-espn-commentary-coverage-start); located values must agree exactly.
        assert (j[c].is_null() == j[c + "_cm"].is_null()).all()
        located = j.filter(pl.col(c).is_not_null())
        assert located.height > 0
        assert (located[c] == located[c + "_cm"]).all()


# -- era-aware fixes (sdv-internal-refs rules/epl.yaml) -----------------------------
def _commentary_payload() -> dict:
    return {
        "commentary": [
            {
                "sequence": 1,
                "text": "unlocated era row",
                "play": {
                    "id": "1",
                    "fieldPositionX": 0.0,
                    "fieldPositionY": 0.0,
                    "goalPositionX": 0.0,
                    "goalPositionY": 0.0,
                    "team": {"displayName": "Augsburg "},
                    "participants": [{"athlete": {"id": "9", "displayName": "Caiuby "}}],
                },
            },
            {
                "sequence": 2,
                "text": "located row",
                "play": {
                    "id": "2",
                    "fieldPositionX": 0.42,
                    "fieldPositionY": 0.0,
                    "goalPositionX": 0.51,
                    "goalPositionY": 0.33,
                    "team": {"displayName": "Augsburg"},
                    "participants": [{"athlete": {"id": "9", "displayName": "Caiuby"}}],
                },
            },
        ]
    }


def test_commentary_zero_pair_is_unlocated_not_a_position() -> None:
    """epl-2009-espn-commentary-coverage-start: ESPN's (0.0, 0.0) means 'no location'."""
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    df = parse_soccer_summary(_commentary_payload(), section="commentary").sort("sequence")
    assert df["field_position_x"].to_list() == [None, 0.42]
    assert df["field_position_y"].to_list() == [None, 0.0]  # a lone 0.0 beside a real x survives
    assert df["goal_position_x"].to_list() == [None, 0.51]
    assert df["goal_position_y"].to_list() == [None, 0.33]


def test_summary_names_are_stripped() -> None:
    """epl-2002-espn-athlete-names-trailing-space: 'Caiuby ' and 'Caiuby' are one player."""
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    df = parse_soccer_summary(_commentary_payload(), section="commentary")
    assert set(df["athlete_name"].to_list()) == {"Caiuby"}
    assert set(df["team_name"].to_list()) == {"Augsburg"}
    frames = parse_soccer_summary(_commentary_payload())
    assert set(frames["commentary"]["athlete_name"].to_list()) == {"Caiuby"}
