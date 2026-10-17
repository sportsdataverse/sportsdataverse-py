"""Atrium embed API parsers, codegen output and fixtures (offline: committed fixtures only, never the refs checkout)."""

from __future__ import annotations

import base64
import copy
import json
import re
import zlib
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.basketball.atrium import atrium_parsers as P

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "atrium"
FULL = FIX / "full"
YAML = ROOT / "tools" / "codegen" / "endpoints" / "atrium.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "atrium"
NOTES = ROOT / "tools" / "codegen" / "notes" / "atrium_descriptions.yaml"

#: Fixture type (the part of the file name after the website id) -> the parsers that read it.
PARSERS = {
    "fixtures": [P.parse_atrium_fixtures, P.parse_atrium_seasons],
    "fixtures_ribbon": [P.parse_atrium_fixtures_ribbon],
    "fixture_detail__statistics": [
        P.parse_atrium_fixture,
        P.parse_atrium_fixture_periods,
        P.parse_atrium_box_score,
        P.parse_atrium_team_box_score,
    ],
    "fixture_detail__pbp": [P.parse_atrium_pbp, P.parse_atrium_fixture_periods],
    "fixture_detail__shot_chart": [P.parse_atrium_shots],
    "standings": [P.parse_atrium_standings],
    "standings_fragment": [P.parse_atrium_standings],
    "entities": [P.parse_atrium_teams],
    "entity_detail": [P.parse_atrium_team],
    "persons": [P.parse_atrium_players],
    "person_detail": [P.parse_atrium_player, P.parse_atrium_player_seasons],
    "person_detail__log": [P.parse_atrium_player_game_log],
    "statistics_leaders": [P.parse_atrium_leaders],
    "statistics_persons": [P.parse_atrium_stat_tables],
    "statistics_entities": [P.parse_atrium_stat_tables],
    "statistics_competition_persons": [P.parse_atrium_stat_tables],
    "statistics_competition_entities": [P.parse_atrium_stat_tables],
}
CAPTURES = sorted(p for p in FIX.glob("*__*.json"))
FULLS = sorted(FULL.glob("*.json"))
#: Every parser, once, with a body it fills (the schema name -> (parser, fixture)).
EVERY = {
    "seasons": (P.parse_atrium_seasons, "12__fixtures.json"),
    "fixtures": (P.parse_atrium_fixtures, "12__fixtures.json"),
    "fixtures_ribbon": (P.parse_atrium_fixtures_ribbon, "14__fixtures_ribbon.json"),
    "fixture": (P.parse_atrium_fixture, "14__fixture_detail__statistics.json"),
    "fixture_periods": (P.parse_atrium_fixture_periods, "14__fixture_detail__statistics.json"),
    "box_score": (P.parse_atrium_box_score, "14__fixture_detail__statistics.json"),
    "team_box_score": (P.parse_atrium_team_box_score, "14__fixture_detail__statistics.json"),
    "pbp": (P.parse_atrium_pbp, "14__fixture_detail__pbp.json"),
    "shots": (P.parse_atrium_shots, "14__fixture_detail__shot_chart.json"),
    "standings": (P.parse_atrium_standings, "12__standings.json"),
    "teams": (P.parse_atrium_teams, "12__entities.json"),
    "team": (P.parse_atrium_team, "12__entity_detail.json"),
    "players": (P.parse_atrium_players, "12__persons.json"),
    "player": (P.parse_atrium_player, "12__person_detail.json"),
    "player_seasons": (P.parse_atrium_player_seasons, "12__person_detail.json"),
    "player_game_log": (P.parse_atrium_player_game_log, "12__person_detail__log.json"),
    "leaders": (P.parse_atrium_leaders, "12__statistics_leaders.json"),
    "stat_tables": (P.parse_atrium_stat_tables, "12__statistics_persons.json"),
}
#: Columns that are ids and so must be String (UUIDs), except website_id / period_id.
_ID = re.compile(r"(^|_)(fixture|entity|person|season|competition|team|event|venue)_id$|(^|_)captain_person_id$")
_SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _body(name: str) -> dict:
    return _load(FULL / name if (FULL / name).exists() else FIX / name)


def _type_of(path: Path) -> str:
    _site, typ, *rest = path.stem.split("__")
    return typ if not rest or rest == ["default"] else f"{typ}__{rest[0]}"


def _frames(parsers, body):
    return [(fn.__name__, fn(body)) for fn in parsers]


# --- every committed body parses ---------------------------------------------------------------------


@pytest.mark.parametrize("path", CAPTURES + FULLS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_every_fixture_parses_to_a_non_empty_snake_case_frame_with_string_ids(path):
    body = _load(path)
    typ = _type_of(path)
    if typ == "fixtures" and path.stem.startswith("236"):  # no season list on this site: the seasons frame is empty
        parsers = [P.parse_atrium_fixtures]
    else:
        parsers = PARSERS[typ]
    for name, df in _frames(parsers, body):
        assert isinstance(df, pl.DataFrame) and df.height > 0, f"{name} on {path.name}"
        assert all(_SNAKE.match(c) for c in df.columns), [c for c in df.columns if not _SNAKE.match(c)]
        for col in df.columns:
            if _ID.search(col):
                assert df.schema[col] == pl.String, f"{name}.{col} is {df.schema[col]}"
        assert df.schema["website_id"] == pl.Int64
        assert set(df["website_id"].to_list()) == {int(path.stem.split("__")[0])}


def test_documented_columns_are_the_frame_prefix_with_no_widened_dtype_on_full_bodies():
    for path in FULLS:
        for name, df in _frames(PARSERS[_type_of(path)], _load(path)):
            schema = P.SCHEMAS[name.removeprefix("parse_atrium_")]
            assert df.columns[: len(schema)] == list(schema), name
            assert {c: df.schema[c] for c in schema} == schema, f"{name} on {path.name}"


def test_every_parser_has_a_schema_and_a_populated_fixture():
    assert set(EVERY) == set(P.SCHEMAS)
    for schema, (fn, fixture) in EVERY.items():
        df = fn(_body(fixture))
        assert df.height > 0 and df.schema == pl.Schema(P.SCHEMAS[schema]), schema


# --- empty / malformed bodies ------------------------------------------------------------------------


@pytest.mark.parametrize("schema", sorted(EVERY))
@pytest.mark.parametrize("payload", [{}, [], None, "x", {"data": None}, {"data": []}, {"data": {}, "links": 3}])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(schema, payload):
    fn, _ = EVERY[schema]
    df = fn(payload)
    assert df.height == 0
    assert df.schema == pl.Schema(P.SCHEMAS[schema])


@pytest.mark.parametrize("schema", sorted(EVERY))
def test_two_empty_frames_stack_and_stack_onto_a_populated_one(schema):
    fn, fixture = EVERY[schema]
    empty, full = fn({}), fn(_body(fixture))
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([full, empty]).schema == full.schema


@pytest.mark.parametrize("schema", ["fixtures", "box_score", "pbp", "stat_tables", "player"])
def test_return_as_pandas(schema):
    fn, fixture = EVERY[schema]
    body = _body(fixture)
    pdf = fn(body, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and list(pdf.columns) == fn(body).columns
    assert isinstance(fn({}, return_as_pandas=True), pd.DataFrame)


# --- real-body checks --------------------------------------------------------------------------------


def test_box_score_adds_up_to_the_team_line_and_the_final_score():
    body = _body("14__fixture_detail__statistics.json")
    players, teams = P.parse_atrium_box_score(body), P.parse_atrium_team_box_score(body)
    head = P.parse_atrium_fixture(body).row(0, named=True)
    assert players.height == 24 and teams.height == 2
    for side in ("home", "away"):
        mine = players.filter((pl.col("side") == side) & (pl.col("participated") == True))  # noqa: E712
        line = teams.filter(pl.col("side") == side).row(0, named=True)
        assert mine["points"].sum() == line["points"] == head[f"{side}_score"]
        assert line["entity_id"] == head[f"{side}_entity_id"]
        assert set(mine["entity_id"]) == {line["entity_id"]}
        assert line["seconds_played"] == 200 * 60  # PT200M: the sum over players
    dnp = players.filter(pl.col("participated") == False)  # noqa: E712
    assert dnp.height == 5 and dnp["points"].sum() == 0 and dnp["minutes"].null_count() == 5
    assert players["person_id"].null_count() == 0


def test_box_score_columns_are_configured_per_site():
    lnbp = P.parse_atrium_box_score(_body("14__fixture_detail__statistics.json"))
    lnb = P.parse_atrium_box_score(_body("12__fixture_detail__statistics.json"))
    assert lnb.columns == lnbp.columns  # one schema; a site's omissions are nulls
    assert lnbp["plus_minus"].null_count() == lnbp.height and lnbp["field_goals_made"].null_count() < lnbp.height
    assert lnb["field_goals_made"].null_count() == lnb.height and lnb["plus_minus"].null_count() < lnb.height
    assert lnb["position"].null_count() < lnb.height  # LNB rosters carry positions
    assert lnb.filter(pl.col("participated") == True).height == lnb.height  # noqa: E712


def test_minutes_stay_iso_and_seconds_are_parsed():
    box = P.parse_atrium_box_score(_body("14__fixture_detail__statistics.json")).filter(pl.col("minutes") == "PT7M23S")
    assert box.height == 1 and box["seconds_played"].item() == 7 * 60 + 23


def test_percentages_are_floats_whole_or_not():
    lnbp = P.parse_atrium_box_score(_body("14__fixture_detail__statistics.json"))
    assert lnbp.schema["field_goals_percentage"] == pl.Float64
    assert lnbp.schema["points_three_percentage"] == pl.Float64 and lnbp["field_goals_percentage"].max() == 100.0
    st = P.parse_atrium_standings(_body("12__standings.json"))
    assert st.schema["win_percentage"] == pl.Float64 and st["win_percentage"].max() <= 1.0  # a 0-1 fraction


def test_pbp_is_chronological_and_ends_on_the_final_score():
    body = _body("14__fixture_detail__pbp.json")
    pbp, head = P.parse_atrium_pbp(body), P.parse_atrium_fixture(body).row(0, named=True)
    assert pbp.height == 497 and pbp["event_id"].n_unique() == 497
    assert pbp["sequence"].to_list() == list(range(1, 498))
    last = pbp.row(-1, named=True)
    assert (last["home_score"], last["away_score"]) == (head["home_score"], head["away_score"]) == (92, 98)
    for _pid, grp in pbp.group_by("period_id", maintain_order=True):
        assert grp["clock_seconds"].to_list() == sorted(grp["clock_seconds"].to_list(), reverse=True)
    assert pbp["clock_seconds"].max() == 600 and set(pbp["side"].drop_nulls()) == {"home", "away"}
    team_events = pbp.filter(pl.col("person_id").is_null())
    assert team_events.height == 19 and team_events["person_name"].null_count() == 19


def test_pbp_x_y_are_floats_although_some_are_whole_numbers():
    pbp = P.parse_atrium_pbp(_body("14__fixture_detail__pbp.json"))
    shots = pbp.filter(pl.col("x").is_not_null())
    assert pbp.schema["x"] == pbp.schema["y"] == pl.Float64
    assert (shots["x"] != shots["x"].round(0)).any() and (shots["x"] == shots["x"].round(0)).any()
    assert shots["x"].is_between(0, 100).all() and shots["y"].is_between(0, 100).all()


def test_shots_are_the_field_goal_attempts_of_the_pbp_and_join_on_event_id():
    shots = P.parse_atrium_shots(_body("14__fixture_detail__shot_chart.json"))
    pbp = P.parse_atrium_pbp(_body("14__fixture_detail__pbp.json"))
    assert shots.height == 129 and set(shots["event_type"]) <= {"2pt", "3pt"}
    assert shots["success"].null_count() == 0
    j = shots.join(pbp.select("event_id", "x", "y", "person_id"), on="event_id", suffix="_pbp")
    assert j.height == shots.height  # every shot is a pbp event (same id dtype both sides)
    assert (j["x"] == j["x_pbp"]).all() and (j["person_id"] == j["person_id_pbp"]).all()


def test_periods_sum_to_the_final_score_and_overtime_keeps_its_id():
    body = _body("14__fixture_detail__statistics.json")
    per = P.parse_atrium_fixture_periods(body)
    assert (
        per["period_id"].to_list() == [1, 2, 3, 4] and per["home_points"].sum() == 92 and per["away_points"].sum() == 98
    )
    assert per["is_overtime"].sum() == 0 and per["duration_minutes"].to_list() == [10] * 4
    ot = copy.deepcopy(body)  # the same body with the first overtime appended: period 11, not 5
    ids = [c["entityId"] for c in ot["data"]["fixture"]["competitors"]]
    ot["data"]["periodData"].append(
        {"periodId": 11, "isOvertime": True, "isShootout": False, "durationMinutes": 5, "elapsedMinutesBeforePeriod": 40,
         "started": True, "ended": True, "labels": {"shortLabel": "OT1"}, "teamScore": {ids[0]: 7, ids[1]: 9}}
    )  # fmt: skip
    ext = P.parse_atrium_fixture_periods(ot)
    assert ext["period_id"].to_list() == [1, 2, 3, 4, 11]
    assert ext.row(-1, named=True)["is_overtime"] is True and ext["elapsed_minutes_before_period"][-1] == 40


def test_fixtures_scores_are_ints_times_are_naive_datetimes_and_home_is_the_isHome_side():
    body = _body("12__fixtures.json")
    df = P.parse_atrium_fixtures(body)
    assert df.height == 60 and df.schema["home_score"] == pl.Int64 and df.schema["start_time_utc"] == pl.Datetime("us")
    first = df.row(0, named=True)
    assert (first["home_name"], first["home_score"], first["away_name"], first["away_score"]) == (
        "Paris", 87, "Lyon-Villeurbanne", 76,
    )  # fmt: skip
    assert str(first["start_time_utc"]) == "2026-05-16 17:00:00" and first["timezone"] == "Europe/Paris"
    assert df["fixture_id"].n_unique() == 60 and df["competition_name"].n_unique() == 1
    assert set(df["season_end_year"]) == {2026} and df["home_result_place"].is_in([1, 2]).all()


def test_scores_that_are_not_digits_become_null():
    body = copy.deepcopy(_body("12__fixtures.json"))
    body["data"]["fixtures"][0]["competitors"][0]["score"] = ""
    body["data"]["fixtures"][1]["competitors"][0]["score"] = "TBD"
    df = P.parse_atrium_fixtures(body)
    assert df["home_score"].head(2).to_list() == [None, None] and df["home_score"][2] is not None


def test_ribbon_scores_are_ints():
    df = P.parse_atrium_fixtures_ribbon(_body("14__fixtures_ribbon.json"))
    assert df.schema["home_score"] == pl.Int64 and df["home_score"].null_count() == 0 and df["is_final"].all()


def test_standings_columns_follow_the_site():
    lnb, lnbp = (
        P.parse_atrium_standings(_body("12__standings.json")),
        P.parse_atrium_standings(_body("14__standings.json")),
    )
    assert lnb.columns == lnbp.columns
    assert lnb["position"].to_list() == list(range(1, 17)) and lnb["wins"][0] == 22 and lnb["losses"][0] == 8
    assert lnb["wins"].min() == -1  # sent as -1 for two teams in the 2025-26 table (Monaco, Le Portel); kept as sent
    assert lnb["played"].null_count() == lnb.height and lnbp["played"].null_count() == 0
    assert lnb["result_string_home"].null_count() == 0 and lnbp["result_string_home"].null_count() == lnbp.height
    assert lnb["season_end_year"][0] == 2026 and lnbp["season_end_year"][0] == 2026
    frag = P.parse_atrium_standings(_body("12__standings_fragment.json"))
    assert frag["played"].null_count() == 0 and frag["win_percentage"].null_count() == frag.height


def test_players_height_is_an_int_where_given_and_dates_are_dates():
    df = P.parse_atrium_players(_body("12__persons.json"))
    assert df.schema["height"] == pl.Int64 and df.schema["date_of_birth"] == pl.Date and df.schema["age"] == pl.Int64
    assert (
        df["person_id"].n_unique() == df.height == 60 and df["position_code"].null_count() == 3
    )  # an empty position object is null


def test_leaders_value_is_numeric_and_durations_land_in_value_text():
    ld = P.parse_atrium_leaders(_body("12__statistics_leaders.json"))
    assert ld.height == 45 and ld["rank"].min() == 1 and ld["rank"].max() == 5 and ld["person_id"].null_count() == 0
    mins = ld.filter(pl.col("stat_key") == "minutesPerGame")
    assert mins["value"].null_count() == 5 and mins["value_text"].str.starts_with("PT").all()
    assert ld.filter(pl.col("stat_key") == "points")["value"].is_not_null().all()


def test_stat_tables_are_long_with_ids_decoded_from_the_widget_links():
    st = P.parse_atrium_stat_tables(_body("12__statistics_persons.json"))
    assert set(st["table_key"]) == {"averages", "shootingstatistics", "totals"}
    assert (
        st["person_id"].null_count() == 0 and st["entity_id"].null_count() == st.height
    )  # player tables name the team only
    ppg = st.filter(pl.col("stat_key") == "pointsPerGame")
    assert ppg.height == 60 and ppg["value"].is_not_null().all() and ppg["value_text"].null_count() == ppg.height
    mpg = st.filter(pl.col("stat_key") == "minutesPerGame")
    assert mpg["value"].null_count() == mpg.height and mpg["value_text"].str.starts_with("PT").all()
    # the decoded ids are the persons-list ids of the same names (57 of the first 60 players are in both bodies)
    players = P.parse_atrium_players(_body("12__persons.json"))
    ids = dict(zip(players["name"], players["person_id"]))
    pairs = st.select("person_name", "person_id").unique()
    common = [(n, i) for n, i in zip(pairs["person_name"], pairs["person_id"]) if n in ids]
    assert len(common) >= 50 and all(ids[n] == i for n, i in common)
    teams = P.parse_atrium_stat_tables(_body("12__statistics_entities.json"))
    assert teams["entity_id"].null_count() == 0 and teams["person_id"].null_count() == teams.height
    comp = P.parse_atrium_stat_tables(_body("12__statistics_competition_persons.json"))
    assert comp["person_id"].null_count() == comp.height and comp["season_id"].null_count() == comp.height


def _token(obj: dict, prefix: str = "p") -> str:
    c = zlib.compressobj()  # the widget's zlib stream has no final flush
    raw = c.compress(json.dumps(obj).encode()) + c.flush(zlib.Z_SYNC_FLUSH)
    return f"{prefix}~" + base64.urlsafe_b64encode(raw).decode().rstrip("=")


def test_link_ids_decode_both_link_shapes_and_survive_garbage():
    tok = _token({"s": "S", "p": "PERSON"})
    assert P._link_ids({"queryParams": {"~w": tok}})["p"] == "PERSON"
    assert P._link_ids(f"&~w={tok}")["p"] == "PERSON"
    for junk in (None, {}, "", "p~", "p~!!!", {"queryParams": {"~w": "p~AAAA"}}, 3):
        assert P._link_ids(junk) == {}


def test_player_pages():
    p = P.parse_atrium_player(_body("12__person_detail.json")).row(0, named=True)
    assert p["name"] == "Braian Angola-Rodas" and p["nationality"] == "Colombie" and p["age"] == 32
    assert str(p["date_of_birth"]) == "1994-04-06" and p["points_per_game"] > 0
    seasons = P.parse_atrium_player_seasons(_body("12__person_detail.json"))
    assert seasons["season_end_year"].to_list() == [2026] * 3 and seasons["team_id"].null_count() == 0
    log = P.parse_atrium_player_game_log(_body("12__person_detail__log.json"))
    assert log.schema["date"] == pl.Datetime("us") and log["result"].is_in(["W", "L"]).all()
    assert log["score"][0] == "88 - 91" and log["points"].null_count() == 0


def test_team_page_and_team_list():
    team = P.parse_atrium_team(_body("12__entity_detail.json")).row(0, named=True)
    assert team["entity_id"] == "f43b1b0f-55bc-11f0-84d9-01499d2f0a07" and team["name"] == "Paris"
    assert team["previous_home_score"] == 87 and team["previous_away_score"] == 76 and team["next_fixture_id"] is None
    teams = P.parse_atrium_teams(_body("12__entities.json"))
    assert teams["entity_id"].n_unique() == 3 and teams["season_id"].null_count() == 0


def test_fixture_header_reads_the_rules_and_captains():
    h = P.parse_atrium_fixture(_body("14__fixture_detail__statistics.json")).row(0, named=True)
    assert (h["number_of_periods"], h["period_length"], h["overtime_length"], h["shot_clock"]) == (4, 10, 5, 24)
    assert h["away_captain_person_id"] == "d1f62f2d-2b0f-11ed-96ed-359aacd8dec1" and h["home_captain_person_id"] is None
    assert h["status"] == "CONFIRMED" and str(h["updated"]).startswith("2026-10-12")


# --- seasons and the END-year mapping ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("site", "year", "end"),
    [
        (12, 2025, 2026),
        (12, 2023, 2024),
        (14, 2026, 2026),
        (3, 2022, 2022),
        (13, 2026, 2026),
        (22, 2024, 2024),
        (326, 2026, 2026),
    ],
)
def test_end_year_mapping(site, year, end):
    assert P.atrium_end_year(site, year) == end


@pytest.mark.parametrize(
    ("site", "year"), [(236, 2026), (999, 2026), (12, None), (12, "2025"), (12, True), ("12", 2025)]
)
def test_end_year_is_none_when_it_cannot_be_mapped(site, year):
    assert P.atrium_end_year(site, year) is None


def test_seasons_frame_carries_the_end_year_per_site():
    lnb = P.parse_atrium_seasons(_body("12__fixtures.json"))
    assert lnb.height == 19 and (lnb["season_end_year"] == lnb["year"] + 1).all()
    assert lnb["is_default_season"].sum() == 1 and lnb["season_id"].n_unique() == 19
    assert lnb.filter(pl.col("is_default_season"))["season_id"].item() == "df310a05-51ad-11f0-bd89-c735508e1e09"
    lnbp = P.parse_atrium_seasons(_body("14__fixtures__default.json"))
    assert (lnbp["season_end_year"] == lnbp["year"]).all()
    assert P.parse_atrium_seasons(_body("236__fixtures__default.json")).height == 0  # Austria lists no seasons
    unmapped = copy.deepcopy(_body("14__fixtures__default.json"))
    unmapped["links"]["self"] = unmapped["links"]["self"].replace("/embed/14/", "/embed/777/")
    assert (
        P.parse_atrium_seasons(unmapped)["season_end_year"].null_count()
        == unmapped["data"]["seasons"]["seasons"].__len__()
    )


def test_duration_and_score_helpers():
    assert [P._seconds(v) for v in ("PT10M0S", "PT200M", "PT1H2M3S", "PT0M8S", "PT45S")] == [600, 12000, 3723, 8, 45]
    assert [P._seconds(v) for v in (None, "", "10:00", "P1D", "PT", 600)] == [None] * 6
    assert [P._int(v) for v in ("87", 87, "  9 ", 5.0)] == [87, 87, 9, 5]
    assert [P._int(v) for v in ("", "x", None, True, 5.5, "1.5")] == [None] * 6
    assert [P._id(v) for v in ("a", "", None, 5, 5.0, True)] == ["a", None, None, "5", None, None]


def test_an_unlisted_statistic_is_appended_not_dropped():
    body = copy.deepcopy(_body("14__fixture_detail__statistics.json"))
    for g in body["data"]["statistics"]["data"]["base"]["home"]["persons"]:
        for r in g["rows"]:
            r["statistics"]["secondChancePoints"] = 3
    df = P.parse_atrium_box_score(body)
    assert df.columns[-1] == "second_chance_points" and df.columns[:-1] == list(P.SCHEMAS["box_score"])
    assert df["second_chance_points"].null_count() == 12


def test_a_value_the_documented_dtype_cannot_hold_widens_the_column():
    body = copy.deepcopy(_body("14__fixture_detail__statistics.json"))
    body["data"]["statistics"]["data"]["base"]["home"]["persons"][0]["rows"][0]["statistics"]["points"] = 2.5
    df = P.parse_atrium_box_score(body)
    assert df.schema["points"] == pl.Float64 and 2.5 in df["points"].to_list()


# --- hostile bodies -----------------------------------------------------------------------------------


def _paths(obj, prefix=(), depth=0):
    """Every key path of a body (a list is entered through its first element, as ``0``), to a depth of 7."""
    if depth > 7:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield (*prefix, k)
            yield from _paths(v, (*prefix, k), depth + 1)
    elif isinstance(obj, list) and obj:
        yield (*prefix, 0)
        yield from _paths(obj[0], (*prefix, 0), depth + 1)


def _set(obj, path, value):
    for k in path[:-1]:
        obj = obj[k]
    obj[path[-1]] = value


@pytest.mark.parametrize("typ", sorted(PARSERS))
def test_a_wrongly_typed_value_anywhere_in_a_body_never_raises(typ):
    """Every key of a real body, swapped for a string, a number, an empty object and a mixed list: a frame, never an exception."""
    path = next(p for p in CAPTURES if _type_of(p) == typ)
    real = _load(path)
    for where in _paths(real):
        for junk in ("x", 5, {}, [1, "a"]):
            body = copy.deepcopy(real)
            _set(body, where, junk)
            for fn in PARSERS[typ]:
                df = fn(body)
                assert isinstance(df, pl.DataFrame), (fn.__name__, where, junk)
                assert df.columns[: len(P.SCHEMAS[fn.__name__.removeprefix("parse_atrium_")])] == list(
                    P.SCHEMAS[fn.__name__.removeprefix("parse_atrium_")]
                )


# --- the codegen output ------------------------------------------------------------------------------


def _yaml() -> dict:
    return yaml.safe_load(YAML.read_text(encoding="utf-8"))


def test_yaml_lists_every_route_and_every_wrapper():
    doc = _yaml()
    spec_paths = json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(spec_paths) == 14
    assert {e["path"].replace("{website_id}", "{websiteId}") for e in doc["endpoints"]} == set(spec_paths)
    captured = {re.sub(r"^\d+__", "", p.stem).split("__")[0] for p in CAPTURES}
    assert captured <= {e["path"].rsplit("/", 1)[1] for e in doc["endpoints"]}
    assert len(doc["endpoints"]) == 22 and len({e["short"] for e in doc["endpoints"]}) == 22
    assert doc["name_pattern"] == "atrium_{short}" and doc["getter_module"].endswith("atrium_runtime")


def test_no_path_token_named_league_or_sport_and_website_id_is_the_only_path_param():
    for e in _yaml()["endpoints"]:
        assert re.findall(r"\{([^}]+)\}", e["path"]) == ["website_id"], e["path"]
        assert [p["name"] for p in e["path_params"]] == ["website_id"]
        assert not any(q["name"] in ("league", "sport") for q in e.get("extra_params", []))


def test_every_wrapper_has_a_parser_a_schema_and_a_description_for_every_column():
    notes = yaml.safe_load(NOTES.read_text(encoding="utf-8"))
    for e in _yaml()["endpoints"]:
        assert callable(getattr(P, e["parser"])), e["parser"]
        schema = yaml.safe_load((SCHEMA_DIR / f"{e['short']}.yaml").read_text(encoding="utf-8"))
        assert schema["schema"] == f"atrium_{e['short']}" and e["returns_schema"] == f"native/atrium/{e['short']}"
        frame = e["parser"].removeprefix("parse_atrium_")
        assert [c["name"] for c in schema["columns"]] == list(P.SCHEMAS[frame])
        assert all(c["description"] == "" for c in schema["columns"])  # descriptions live in manual_column_descriptions
        assert set(notes[schema["schema"]]) == {c["name"] for c in schema["columns"]}
        assert all(len(d) >= 15 for d in notes[schema["schema"]].values()), schema["schema"]


def test_schema_types_match_the_parser_dtypes():
    want = {pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.Date: "date", pl.String: "character"}
    for e in _yaml()["endpoints"]:
        schema = yaml.safe_load((SCHEMA_DIR / f"{e['short']}.yaml").read_text(encoding="utf-8"))
        dtypes = P.SCHEMAS[e["parser"].removeprefix("parse_atrium_")]
        for col in schema["columns"]:
            dt = dtypes[col["name"]]
            assert col["type"] == ("datetime" if isinstance(dt, pl.Datetime) else want[dt]), col["name"]


def test_wrappers_pass_their_fixed_sub_and_the_site_in_the_path(monkeypatch):
    from sportsdataverse.basketball.atrium import atrium as api

    seen = {}

    def fake(url, params=None, **kw):
        seen.update(url=url, params=params)
        return {}

    monkeypatch.setattr(api, "_get", fake)
    api.atrium_pbp(website_id=14, fixture_id="f", return_parsed=False)
    assert seen["url"].endswith("/v1/embed/14/fixture_detail")
    assert seen["params"] == {"sub": "pbp", "fixtureId": "f", "locale": None}
    api.atrium_fixtures(website_id=12, season=2026, competition_id="c", sub="RESULTS", return_parsed=False)
    assert seen["url"].endswith("/v1/embed/12/fixtures")
    assert seen["params"] == {
        "season": 2026, "seasonId": None, "competitionId": "c", "entityId": None, "roundNumber": None,
        "sub": "RESULTS", "locale": None,
    }  # fmt: skip
    assert api.atrium_box_score.__doc__ and "Example:" in api.atrium_box_score.__doc__
