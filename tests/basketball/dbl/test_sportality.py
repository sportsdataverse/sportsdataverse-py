"""Offline tests for the Basketligaen (``sportality``) parsers, endpoint YAML and returns-schemas.

Every parser runs on the real captures in ``tests/fixtures/sportality/`` (3-row samples copied from
sdv-internal-refs) and, for the dtypes, on the untruncated bodies in ``tests/fixtures/sportality/full/``.
No network and no refs checkout.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.basketball.dbl import sportality_parsers as P

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "sportality"
FULL = FIXTURES / "full"
YAML = ROOT / "tools" / "codegen" / "endpoints" / "sportality.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "sportality"
NOTES = ROOT / "tools" / "codegen" / "notes" / "dbl_descriptions.yaml"

_SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
# Genius person / match / team / event ids are Int64 in every frame; platform uuids are Utf8.
_INT_ID = re.compile(r"(^|_)(player_id|game_id|game_source_id|event_id|ext_id|team_id)$")
_PERSONAL = re.compile(r"(birth|dob|e_?mail|phone|mobile|address|contact)")


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _endpoints() -> list:
    return yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"]


def _capture_for(ep: dict) -> list:
    slug = ep["path"].strip("/").replace("/", "__").replace("{", "").replace("}", "")
    if ep["short"] == "stats_info":
        return sorted(FIXTURES.glob("statistics-v2__stats-info__module__*.json"))
    return [FIXTURES / f"{slug}.json"]


_CASES = [(ep["short"], ep["parser"], f) for ep in _endpoints() for f in _capture_for(ep)]


@pytest.mark.parametrize(("short", "parser", "fixture"), _CASES, ids=[f"{s}:{f.stem[-30:]}" for s, _, f in _CASES])
def test_every_capture_parses_to_a_typed_frame(short, parser, fixture):
    df = getattr(P, parser)(_load(fixture))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, short
    assert all(_SNAKE.match(c) for c in df.columns), [c for c in df.columns if not _SNAKE.match(c)]
    for col in df.columns:
        if _INT_ID.search(col):
            assert df.schema[col] == pl.Int64, (short, col, df.schema[col])
        if col.endswith("_uuid"):
            assert df.schema[col] == pl.Utf8, (short, col)
        assert not _PERSONAL.search(col), (short, col)


@pytest.mark.parametrize("payload", [{}, [], None, "<html>", 3, [None], {"stats": None}])
@pytest.mark.parametrize("parser", P.__all__)
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = getattr(P, parser)(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_zero_row_frame_carries_the_documented_columns():
    df = P.parse_sportality_pbp([], columns=["game_uuid", "player_id", "coordinates_x", "event_id"])
    assert df.schema == {
        "game_uuid": pl.Utf8,
        "player_id": pl.Int64,
        "coordinates_x": pl.Utf8,
        "event_id": pl.Int64,
    }


@pytest.mark.parametrize("parser", P.__all__)
def test_return_as_pandas(parser):
    ep = next((e for e in _endpoints() if e["parser"] == parser), None)
    fixture = _capture_for(ep)[0] if ep else FIXTURES / "statistics-v2__stats-info__module__players_summary.json"
    out = getattr(P, parser)(_load(fixture), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


# --------------------------------------------------------------------------- personal data


def test_scrub_drops_personal_keys_at_every_depth():
    body = {"a": {"birthDate": "2007-01-01", "dateOfBirth": "x", "email": "e", "tel": "1", "keep": 1}}
    assert P._scrub([body]) == [{"a": {"keep": 1}}]


def test_personal_data_never_reaches_a_frame_even_when_the_body_is_not_scrubbed():
    """Birth dates sit in the stats-info row info, the players map, rosters, profiles and head-to-head."""
    players = P.parse_sportality_stats_info(_load(FULL / "statistics-v2__stats-info__players_summary__qzw34da6jg.json"))
    crosswalk = P.parse_sportality_player_crosswalk(
        _load(FULL / "statistics-v2__stats-info__players_summary__qzw34da6jg.json")
    )
    profile = P.parse_sportality_page(_load(FIXTURES / "statistics-v2__athlete__profile-page.json"))
    athlete = P.parse_sportality_athlete(_load(FIXTURES / "sports-v2__athlete-details__athlete_uuid.json"))
    team = P.parse_sportality_page(_load(FIXTURES / "sports-v2__teams__team_uuid.json"))
    for df in (players, crosswalk, profile, athlete, team):
        assert not [c for c in df.columns if _PERSONAL.search(c)], df.columns
    assert "<redacted>" not in "".join(str(v) for df in (profile, athlete) for v in df.row(0))


# --------------------------------------------------------------------------- dtypes from FULL bodies


def test_pbp_full_body_ids_coordinates_and_event_codes():
    df = P.parse_sportality_pbp(_load(FULL / "gameday__play-by-play__c4hh5nig6t.json"))
    assert df.height == 129
    for col in ("game_id", "game_source_id", "event_id", "player_id", "home_team_id", "event_team_id"):
        assert df.schema[col] == pl.Int64, col  # strings on the wire for the Genius ids
    assert df.schema["coordinates_x"] == pl.Float64 and df.schema["coordinates_y"] == pl.Float64
    assert df["game_id"].unique().to_list() == [2868126] == df["game_source_id"].unique().to_list()
    assert df["game_uuid"].unique().to_list() == ["c4hh5nig6t"]
    shots = df.filter(pl.col("type").is_in(["2pt", "3pt"]))
    assert shots.height > 0 and shots["coordinates_x"].null_count() == 0
    # The newest event is the game-end line at the final score.
    last = df.sort("event_id").tail(1)
    assert (last["type"].item(), last["sub_type"].item(), last["game_state"].item()) == ("game", "end", "GameEnded")
    assert (last["home_score"].item(), last["away_score"].item()) == (84, 100)
    # qualifiers is one ';'-joined string whatever the era's wire type.
    assert df.schema["qualifiers"] == pl.Utf8
    assert {c for c in df.columns if "player" in c} >= {"player_id", "player_pno", "player_first_name"}


def test_pbp_qualifiers_list_and_string_forms_agree():
    a = P.parse_sportality_pbp([{"type": "2pt", "qualifiers": ["fastbreak", "pointsinthepaint"]}])
    b = P.parse_sportality_pbp([{"type": "2pt", "qualifiers": "fastbreak;pointsinthepaint"}])
    assert a["qualifiers"].item() == b["qualifiers"].item() == "fastbreak;pointsinthepaint"


def test_player_box_full_body_rows_sides_and_types():
    df = P.parse_sportality_player_box(_load(FULL / "gameday__boxscore__c4hh5nig6t.json"))
    assert set(df["team_side"]) == {"home", "away"}
    assert set(df["row_type"]) == {"player", "team_coach", "team_totals"}
    assert df.schema["player_id"] == pl.Int64 and df.schema["team_id"] == pl.Int64
    players = df.filter(pl.col("row_type") == "player")
    assert players["full_name"].null_count() == 0  # every row found its Genius id in the name map
    assert players["player_id"].null_count() == 0
    assert df.filter(pl.col("row_type") != "player")["player_id"].null_count() == 4
    # counts are Int64, rates Float64, minutes and made-attempted stay text
    assert df.schema["pts"] == pl.Int64 and df.schema["fg_perc"] == pl.Float64
    assert df.schema["mins"] == pl.Utf8 and df.schema["fg"] == pl.Utf8
    totals = df.filter(pl.col("row_type") == "team_totals").sort("team_side")
    assert totals["pts"].to_list() == [100, 84]
    assert totals["team_id"].to_list() == [22250, 153747]


def test_box_player_rows_take_their_sides_team_id_not_the_wire_player_team_id():
    """The wire's info.teamId on player rows was the AWAY team's id for both sides in every game checked."""
    body = _load(FULL / "gameday__boxscore__c4hh5nig6t.json")
    wire = {r["info"]["teamId"] for r in body["stats"]["homeTeamValue"] if "label" not in r["info"]}
    assert wire == {"22250"}  # the trap: home players carry the away id
    df = P.parse_sportality_player_box(body)
    home = df.filter((pl.col("team_side") == "home") & (pl.col("row_type") == "player"))
    assert home["team_id"].unique().to_list() == [153747]
    assert home["team_name"].unique().to_list() == ["Holbæk-Stenhus"]


def test_player_stats_rows_carry_jersey_numbers_and_no_team_id():
    df = P.parse_sportality_player_box(_load(FIXTURES / "gameday__player-stats__game_uuid.json"))
    assert {"team_number", "pno"} <= set(df.columns)
    assert df["team_id"].null_count() == df.height


def test_stats_info_full_bodies_are_typed_and_the_genius_id_is_joined():
    players = P.parse_sportality_stats_info(_load(FULL / "statistics-v2__stats-info__players_summary__qzw34da6jg.json"))
    assert players.height == 208
    assert players.schema["ppg"] == pl.Float64 and players.schema["eff"] == pl.Float64
    assert players.schema["gp"] == pl.Int64
    assert players.schema["player_id"] == pl.Int64 and players["player_id"].null_count() == 0
    assert "rank" not in players.columns and "player" not in players.columns  # constant-0/null display slots
    # the wire's teamCode (an integer) is the Genius team id; its teamId (a uuid) repeats team.uuid
    assert players.schema["info_team_id"] == pl.Int64 and "info_team_code" in players.columns
    assert players["info_team_code"].str.len_chars().max() == 3
    teams = P.parse_sportality_stats_info(_load(FULL / "statistics-v2__stats-info__teams_summary__qzw34da6jg.json"))
    assert teams.height == 13 or teams.height == 12
    assert teams.schema["info_team_id"] == pl.Int64 and teams.schema["info_uuid"] == pl.Utf8
    assert teams.schema["p_avg"] == pl.Float64


def test_games_games_splits_the_score_and_keeps_genius_ids():
    games = P.parse_sportality_stats_info(_load(FULL / "statistics-v2__stats-info__games_games__qzw34da6jg.json"))
    assert games.height == 192
    for col in ("home_score", "away_score", "info_game_id", "info_home_team_id", "info_away_team_id", "attendance"):
        assert games.schema[col] == pl.Int64, col
    assert {"home_team", "away_team"} <= set(games.columns) and "guest_team" not in games.columns
    assert games.select(pl.col("info_game_id").is_unique().all()).item()


def test_schema_types_come_from_the_full_bodies():
    """A 3-row sample can type a rate as an integer; the committed schema must not."""
    for short, body, parser in [
        ("stats_info", "statistics-v2__stats-info__players_summary__qzw34da6jg.json", P.parse_sportality_stats_info),
        ("stats_info", "statistics-v2__stats-info__teams_summary__qzw34da6jg.json", P.parse_sportality_stats_info),
        ("boxscore", "gameday__boxscore__c4hh5nig6t.json", P.parse_sportality_player_box),
        ("pbp", "gameday__play-by-play__c4hh5nig6t.json", P.parse_sportality_pbp),
        ("schedule", "sports-v2__game-schedule__qzw34da6jg.json", P.parse_sportality_schedule),
    ]:
        schema = {c["name"]: c["type"] for c in yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text())["columns"]}
        df = parser(_load(FULL / body))
        for col, dtype in df.schema.items():
            want = "numeric" if dtype == pl.Float64 else "integer" if dtype.is_integer() else None
            if want:
                assert schema[col] == want, (short, col)


def test_a_rate_that_is_whole_numbers_in_one_season_is_numeric_in_the_schema():
    """2002/03 tables send whole-number averages where later seasons send decimals; the schema takes the wider type."""
    cols = {c["name"]: c["type"] for c in yaml.safe_load((SCHEMAS / "stats_info.yaml").read_text())["columns"]}
    for col in ("tr_avg", "f_on", "eff", "o_rat"):
        assert cols[col] == "numeric", col


# --------------------------------------------------------------------------- dedicated shapers


def test_standings_rows_carry_genius_and_platform_team_ids():
    df = P.parse_sportality_standings(_load(FIXTURES / "gameday__team-standings__ssgt_uuid.json"))
    assert df.schema["team_id"] == pl.Int64 and df.schema["team_uuid"] == pl.Utf8
    assert df.schema["strk"] == pl.Int64  # a signed-number string on the wire
    assert df.select((pl.col("w") + pl.col("l") == pl.col("gp")).all()).item()
    assert df["team_code"].to_list()[0] == "TFN" and df["team_id"].to_list()[0] == 22255
    assert df.select((pl.col("for") - pl.col("ga") == pl.col("plus_minus")).all()).item()
    assert "season" not in df.columns  # the body names neither its season nor its ssgt


def test_standings_streak_sign():
    body = {
        "leagueStandings": [
            {"Rank": 1, "Strk": "-2", "info": {}},
            {"Rank": 2, "Strk": "3", "info": {}},
            {"Rank": 3, "Strk": "x", "info": {}},
        ]
    }
    assert P.parse_sportality_standings(body)["strk"].to_list() == [-2, 3, None]


def test_schedule_rows_are_games_with_side_prefixes():
    df = P.parse_sportality_schedule(_load(FIXTURES / "sports-v2__game-schedule.json"))
    assert df["game_uuid"][0] == "c4hh5nig6t"
    assert {"home_team_uuid", "away_team_code", "home_team_score", "venue_name", "raw_start_date_time"} <= set(
        df.columns
    )
    full = P.parse_sportality_schedule(_load(FULL / "sports-v2__game-schedule__qzw34da6jg.json"))
    assert full.height == 192 and full["state"].unique().to_list() == ["post-game"]


def test_team_stats_one_row_per_side_and_period():
    df = P.parse_sportality_team_stats(_load(FULL / "gameday__team-stats__c4hh5nig6t.json"))
    assert df.group_by("team_side").len().sort("team_side")["len"].to_list() == [5, 5]  # whole game + 4 quarters
    whole = df.filter((pl.col("team_side") == "home") & (pl.col("period") == 0))
    assert whole["tp"].item() == 84 and whole["team_id"].item() == 153747 and whole["game_id"].item() == 2868126
    assert whole["period_type"].item() is None  # absent on period 0
    assert df.schema["tp"] == pl.Int64 and {"fgm", "reb", "stl", "blk", "pf"} <= set(df.columns)
    assert "place" not in df.columns


def test_layout_maps_start_year_codes_to_end_year_seasons():
    df = P.parse_sportality_layout(_load(FULL / "statistics-v2__layout-info__directory.json"))
    assert df.height == 49
    assert df.select(pl.struct("season", "game_type").is_unique().all()).item()
    assert set(df["game_type"]) == {"regular", "finals"}
    row = df.filter((pl.col("season") == 2026) & (pl.col("game_type") == "regular"))
    assert row["ssgt_uuid"].item() == "qzw34da6jg" and row["season_code"].item() == "2025"
    assert df["season"].min() == 2003 and df["season"].max() == 2027


def test_player_crosswalk_genius_id_to_athlete_uuid():
    df = P.parse_sportality_player_crosswalk(
        _load(FULL / "statistics-v2__stats-info__players_summary__qzw34da6jg.json")
    )
    assert df.schema["player_id"] == pl.Int64 and df.schema["athlete_uuid"] == pl.Utf8
    assert df.height == 206 and df["athlete_uuid"].null_count() == 0  # the map holds one entry per Genius id
    assert df["player_id"].is_unique().all()
    assert {"first_name", "last_name", "height", "weight", "jersey_number"} <= set(df.columns)


def test_athlete_details_carries_the_genius_person_id():
    df = P.parse_sportality_athlete(_load(FIXTURES / "sports-v2__athlete-details__athlete_uuid.json"))
    assert df.height == 1 and df.schema["player_id"] == pl.Int64
    assert df["player_id"].item() == 1917467 and df["athlete_uuid"].item() == "dpu0901c0t"
    assert "player_ext_ids" not in df.columns


def test_athlete_without_a_genius_id_has_a_null_player_id():
    df = P.parse_sportality_athlete({"athleteData": {"uuid": "u1", "playerExtIds": []}})
    assert df["player_id"].null_count() == 1 and df["athlete_uuid"].item() == "u1"


def test_page_object_is_one_row():
    """A team page holds one incidental list (contact); it must not become the rows."""
    team = _load(FIXTURES / "sports-v2__teams__team_uuid.json")
    df = P.parse_sportality_page(team)
    assert df.height == 1 and df["uuid"].item() == "rfo82vr830"
    header = P.parse_sportality_page(_load(FIXTURES / "sports-v2__game-info__game_uuid.json"))
    assert header.height == 1 and header.schema["game_info_ext_id"] == pl.Int64
    assert header["game_info_ext_id"].item() == 2868126


def test_game_overview_ids_are_int64():
    df = P.parse_sportality_page(_load(FIXTURES / "gameday__game-overview__game_uuid.json"))
    assert df["home_team_game_source_id"].item() == 2868126 == df["time_game_id"].item()
    assert df["home_team_team_id"].item() == 153747 and df["away_team_team_id"].item() == 22250


def test_provider_bare_word_is_one_row():
    assert P.parse_sportality("genius-basketball").to_dicts() == [{"provider": "genius-basketball"}]


def test_filter_lists_series_seasons_and_game_types():
    df = P.parse_sportality_filter(_load(FIXTURES / "sports-v2__season-series-game-types-filter.json"))
    assert set(df["kind"]) <= {"series", "season", "game_type"} and "season" in set(df["kind"])


# --------------------------------------------------------------------------- YAML + schemas


def test_yaml_lists_every_spec_route():
    spec = _load(FIXTURES / "spec_paths.json")
    assert len(spec["paths"]) == 42 and len(spec["modules"]) == 18
    shorts = [e["short"] for e in _endpoints()]
    assert len(shorts) == len(set(shorts)) == 25
    module_paths = {f"/statistics-v2/stats-info/{m}" for m in spec["modules"]}
    routed = {e["path"] for e in _endpoints()} - {"/statistics-v2/stats-info/{module}"}
    assert routed | module_paths == set(spec["paths"])
    stats = next(e for e in _endpoints() if e["short"] == "stats_info")
    assert next(p for p in stats["path_params"] if p["name"] == "module")["choices"] == spec["modules"]
    for ep in _endpoints():
        assert all(f.exists() for f in _capture_for(ep)), ep["short"]
        assert (SCHEMAS / f"{ep['short']}.yaml").exists()
    modules_captured = {re.search(r"module__([A-Za-z_]+)", f.name).group(1) for f in _capture_for(stats)}
    assert modules_captured == set(spec["modules"])


def test_no_path_token_named_league_or_sport():
    for ep in _endpoints():
        assert not {"league", "sport"} & set(re.findall(r"\{(\w+)\}", ep["path"]))


def test_season_routes_take_the_end_year_and_stats_info_always_sends_count_1000():
    by = {e["short"]: e for e in _endpoints()}
    for short in ("stats_info", "schedule", "last_game_day", "next_game_day"):
        args = {p["name"]: p for p in by[short]["extra_params"]}
        assert args["season"]["type"] == "int" and "END year" in args["season"]["description"]
        assert args["game_type"]["choices"] == ["regular", "finals"]
        assert not {"ssgt_uuid", "season_uuid", "count", "series_uuid"} & set(args)
    assert by["stats_info"]["fixed_params"] == {"count": 1000}
    assert by["standings"]["path_params"][0]["name"] == "ssgt_uuid"


def test_family_docstring_carries_the_owner_flags():
    doc = yaml.safe_load(YAML.read_text(encoding="utf-8"))["docstring"]
    notes = " ".join(doc["notes"])
    for needle in ("Genius", "END year", "2023/24", "Personal data", "Dameligaen", "unofficial".capitalize()):
        assert needle in notes, needle


def test_schemas_match_the_parsers_on_the_captures():
    for ep in _endpoints():
        cols = [c["name"] for c in yaml.safe_load((SCHEMAS / f"{ep['short']}.yaml").read_text())["columns"]]
        for f in _capture_for(ep):
            parsed = getattr(P, ep["parser"])(_load(f)).columns
            assert set(parsed) <= set(cols), (ep["short"], f.name, sorted(set(parsed) - set(cols)))


# --------------------------------------------------------------------------- empty frames = populated dtypes


def _documented(short: str) -> list:
    return [c["name"] for c in yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text())["columns"]]


@pytest.mark.parametrize(("short", "parser", "fixture"), _CASES, ids=[f"{s}:{f.stem[-30:]}" for s, _, f in _CASES])
def test_empty_frame_concatenates_with_a_populated_one(short, parser, fixture):
    """What the generated wrapper does: ``columns=`` -> documented columns and dtypes, empty or not."""
    cols = _documented(short)
    empty = getattr(P, parser)({}, columns=cols)
    full = getattr(P, parser)(_load(fixture), columns=cols)
    both = pl.concat([empty, full], how="vertical")
    assert both.columns[: len(cols)] == cols and both.height == full.height > 0
    assert empty.schema == {c: full.schema[c] for c in cols}


@pytest.mark.parametrize(
    ("short", "parser", "body"),
    [
        ("pbp", "parse_sportality_pbp", "gameday__play-by-play__c4hh5nig6t.json"),
        ("boxscore", "parse_sportality_player_box", "gameday__boxscore__c4hh5nig6t.json"),
        ("team_stats", "parse_sportality_team_stats", "gameday__team-stats__c4hh5nig6t.json"),
        ("schedule", "parse_sportality_schedule", "sports-v2__game-schedule__qzw34da6jg.json"),
        ("stats_info", "parse_sportality_stats_info", "statistics-v2__stats-info__players_summary__qzw34da6jg.json"),
        ("stats_info", "parse_sportality_stats_info", "statistics-v2__stats-info__teams_summary__qzw34da6jg.json"),
        ("stats_info", "parse_sportality_stats_info", "statistics-v2__stats-info__games_games__qzw34da6jg.json"),
        ("layout_info", "parse_sportality_layout", "statistics-v2__layout-info__directory.json"),
    ],
)
def test_empty_frame_matches_the_full_bodies(short, parser, body):
    cols = _documented(short)
    empty = getattr(P, parser)([], columns=cols)
    populated = getattr(P, parser)(_load(FULL / body), columns=cols)
    pl.concat([empty, populated], how="vertical")
    assert empty.schema == {c: populated.schema[c] for c in cols}


def test_full_bodies_lose_nothing_to_the_documented_dtypes():
    """strict=False casts a value the dtype cannot hold to null: no full body may lose one."""
    for short, parser, body in [
        ("pbp", P.parse_sportality_pbp, "gameday__play-by-play__c4hh5nig6t.json"),
        ("boxscore", P.parse_sportality_player_box, "gameday__boxscore__c4hh5nig6t.json"),
        ("stats_info", P.parse_sportality_stats_info, "statistics-v2__stats-info__players_summary__qzw34da6jg.json"),
        ("stats_info", P.parse_sportality_stats_info, "statistics-v2__stats-info__games_games__qzw34da6jg.json"),
        ("schedule", P.parse_sportality_schedule, "sports-v2__game-schedule__qzw34da6jg.json"),
    ]:
        raw = parser(_load(FULL / body))
        conformed = parser(_load(FULL / body), columns=_documented(short))
        for col in raw.columns:
            assert conformed[col].null_count() == raw[col].null_count(), (short, col)


def test_schema_module_lists_the_returns_schema_columns():
    """``sportality_dtypes.SCHEMAS`` is written by gen_sportality.py with the schemas; they must not drift apart."""
    from sportsdataverse.basketball.dbl.sportality_dtypes import SCHEMAS as DTYPES

    assert set(DTYPES) == {e["short"] for e in _endpoints()}
    r_type = {"str": "character", "int": "integer", "float": "numeric", "bool": "logical", "list[int]": "character"}
    for short, types in DTYPES.items():
        doc = yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text())["columns"]
        assert [c["name"] for c in doc] == list(types), short
        assert [c["type"] for c in doc] == [r_type[t] for t in types.values()], short
        assert all(c["description"] == "" for c in doc), short  # text lives in notes/dbl_descriptions.yaml


def test_descriptions_cover_every_column_and_are_not_filler():
    notes = yaml.safe_load(NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"sportality_{e['short']}" for e in _endpoints()}
    for short in (e["short"] for e in _endpoints()):
        texts = notes[f"sportality_{short}"]
        assert set(texts) == set(_documented(short)), short
        for col, text in texts.items():
            assert len(text.strip()) >= 15 and text.strip().lower() != col.lower(), (short, col, text)


def test_unplayed_games_type_the_score_like_played_ones():
    cols = _documented("team_upcoming_games")
    df = P.parse_sportality_schedule(_load(FIXTURES / "sports-v2__upcoming-games__team_uuid.json"), columns=cols)
    assert df.schema["home_team_score"] == pl.Int64 == df.schema["away_team_score"]


def test_missing_documented_columns_are_backfilled_with_their_dtype():
    cols = _documented("standings")
    df = P.parse_sportality_standings({"leagueStandings": [{"Rank": 1, "info": {"teamId": 7}}]}, columns=cols)
    assert df.columns == cols and df.height == 1
    assert df.schema["gp"] == pl.Int64 and df["gp"].null_count() == 1


@pytest.mark.parametrize(
    ("parser", "payload"),
    [
        ("parse_sportality_team_stats", {"home": {"teamId": "1", "statistics": 5}}),
        ("parse_sportality_team_stats", {"home": {"statistics": [{"period": 0, "parsedTotalStatistics": "x"}]}}),
        ("parse_sportality_standings", {"leagueStandings": "x"}),
        ("parse_sportality_stats_info", [{"stats": 7}]),
        ("parse_sportality_filter", {"season": [{"code": "2025", "names": 3}], "series": True}),
        ("parse_sportality_roster", [{"positionCode": "G", "players": {"a": 1}}]),
        ("parse_sportality_h2h", {"players": "x"}),
        ("parse_sportality_player_box", {"stats": {"homeTeamValue": "x"}, "players": 3}),
        ("parse_sportality_athlete", {"athleteData": {"playerExtIds": "x"}}),
    ],
)
def test_wrong_typed_containers_read_as_absent(parser, payload):
    assert isinstance(getattr(P, parser)(payload), pl.DataFrame)


def test_ids_never_stringify_a_float():
    """The Genius ids arrive as strings and ints; none may become '123.0' or a Float64."""
    df = P.parse_sportality_pbp(
        [{"gameSourceId": "2868126", "player": {"playerId": "1887576"}}, {"gameSourceId": 2868126}]
    )
    assert df.schema["game_source_id"] == pl.Int64 and df["game_source_id"].to_list() == [2868126, 2868126]
    assert df["player_id"].to_list() == [1887576, None]
    junk = P.parse_sportality_pbp([{"gameSourceId": "123.0"}])
    assert junk["game_source_id"].to_list() == [None]  # a float-looking id is not an id


@pytest.mark.parametrize(
    ("short", "parser"),
    [
        ("pbp", "parse_sportality_pbp"),
        ("player_box", "parse_sportality_player_box"),
        ("team_stats", "parse_sportality_team_stats"),
        ("schedule", "parse_sportality_schedule"),
    ],
)
def test_an_unusable_body_without_columns_has_the_documented_schema(short, parser):
    """A parser that serves one schema answers ``None`` with that schema's zero-row frame (generic ones need columns=)."""
    cols = _documented(short)
    none = getattr(P, parser)(None)
    assert none.height == 0 and none.columns == cols
    assert getattr(P, parser)(None, return_as_pandas=True).empty
