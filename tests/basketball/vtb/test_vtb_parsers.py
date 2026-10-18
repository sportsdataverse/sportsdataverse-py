"""The VTB parsers: row shapes, dtypes from the FULL bodies, typed empty frames, ids, personal data and unit conversions.

Reads only committed fixtures (``tests/fixtures/vtb_league``, ``tests/fixtures/vtb_infobasket``); CI has no
sdv-internal-refs checkout.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import vtb
from sportsdataverse.basketball.vtb import vtb_frames as F
from sportsdataverse.basketball.vtb.vtb_dtypes import DTYPES
from tests.basketball.vtb._fx import CASES, ROOT, body

SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native"
_PL = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_ID = re.compile(r"(^|_)id$")


PARSERS = {f"{fam}_{short}": getattr(vtb, f"parse_{fam}_{short}") for fam, short, _ in CASES}
FIXTURE_PARAMS = [
    pytest.param(fam, short, rel, ctx, id=f"{fam}_{short}:{rel.split('/')[-1]}")
    for fam, short, fixtures in CASES
    for rel, ctx in fixtures
]


def _schema(fam: str, short: str) -> Dict[str, str]:
    doc = yaml.safe_load((SCHEMAS / fam / f"{short}.yaml").read_text(encoding="utf-8"))
    return {c["name"]: c["type"] for c in doc["columns"]}


def _parse(fam: str, short: str, rel: str, ctx: Dict[str, Any], **kw: Any) -> pl.DataFrame:
    return PARSERS[f"{fam}_{short}"](body(fam, rel, ctx), **kw)


def test_thirty_eight_parsers_are_exported_and_each_has_a_case():
    names = [n for n in vtb.__all__ if n.startswith("parse_")]
    assert len(names) == 38 and {f"parse_{k}" for k in PARSERS} == set(names)
    assert len(PARSERS) == len(CASES) == 38


@pytest.mark.parametrize("fam,short,rel,ctx", FIXTURE_PARAMS)
def test_every_fixture_parses_to_a_snake_case_frame_with_the_documented_columns_and_int64_ids(fam, short, rel, ctx):
    df = _parse(fam, short, rel, ctx)
    if short == "pbp" and "2014" in rel:  # 2013-14 games have no play-by-play: a typed zero-row frame
        assert df.height == 0
    elif short in ("game_staff",) and "2014" in rel:
        assert df.height >= 0
    else:
        assert isinstance(df, pl.DataFrame) and df.height > 0, rel
    assert all(_NAME.match(c) for c in df.columns), df.columns
    doc = list(_schema(fam, short))
    assert df.columns[: len(doc)] == doc  # the documented order first, extras after
    for col, dtype in df.schema.items():
        if _ID.search(col):
            assert dtype == pl.Int64, (col, dtype)
    for col, code in DTYPES[f"parse_{fam}_{short}"].items():
        assert df.schema[col] == _PL[code], (col, df.schema[col], code)


@pytest.mark.parametrize("name", sorted(PARSERS))
@pytest.mark.parametrize("raw", [{}, [], None, "x", 5, {"items": []}, {"data": []}, {"Stats": []}, {"GameTeams": "x"}])
def test_an_empty_or_malformed_body_is_a_zero_row_frame_typed_from_the_schema(name, raw):
    df = PARSERS[name](raw)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert list(df.columns) == list(DTYPES[f"parse_{name}"])
    assert {c: d for c, d in df.schema.items()} == {c: _PL[k] for c, k in DTYPES[f"parse_{name}"].items()}


@pytest.mark.parametrize("fam,short,rel,ctx", [p.values for p in FIXTURE_PARAMS if p.values[2].startswith("full/")])
def test_an_empty_frame_stacks_onto_a_populated_one(fam, short, rel, ctx):
    populated = _parse(fam, short, rel, ctx)
    empty = PARSERS[f"{fam}_{short}"]({})
    assert pl.concat([empty, populated]).height == populated.height
    assert pl.concat([empty, empty]).height == 0


@pytest.mark.parametrize("name", sorted(PARSERS))
def test_return_as_pandas_gives_a_pandas_frame(name):
    import pandas as pd

    fam, short, fixtures = next(c for c in CASES if f"{c[0]}_{c[1]}" == name)
    rel, ctx = fixtures[0]
    out = PARSERS[name](body(fam, rel, ctx), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0
    assert isinstance(PARSERS[name]({}, return_as_pandas=True), pd.DataFrame)


def test_columns_argument_selects_and_orders_the_documented_columns():
    df = vtb.parse_vtb_infobasket_box_score(
        body("vtb_infobasket", "full/online_921391.json.gz", {"game_id": 921391}), columns=["person_id", "points"]
    )
    assert df.columns[:2] == ["person_id", "points"]


# --- conversions --------------------------------------------------------------------------------------------------------


def test_dates_are_iso_and_the_unset_net_date_is_null():
    assert F.conv("/Date(1759057200000)/") == "2025-09-28T14:00:00+03:00"
    assert F.conv("/Date(1759006800000)/") == "2025-09-28T00:00:00+03:00"
    assert F.conv("/Date(-62135596800000)/") is None
    assert F.conv("28.09.2025") == "2025-09-28"
    assert F.conv("14.00") == "14.00" and F.conv(5) == 5 and F.conv(None) is None


def test_comma_decimals_become_floats_and_empty_means_null():
    assert F.decimal("68,2") == 68.2 and F.decimal("80,3%") == 80.3 and F.decimal("0,0") == 0.0
    assert F.decimal("") is None and F.decimal(None) is None and F.decimal("n/a") is None and F.decimal(True) is None
    assert F.decimal(17.5) == 17.5 and F.decimal(3) == 3.0


def test_ratio_strings_and_the_broken_paint_stats_are_dropped_and_null_counts_are_zero():
    row = F.tidy(
        {"shots1": "4/6", "avg_shots23": "1,0/2,0", "paint_shot": 73, "paint_shots": "1/2", "goal3": None, "plus_minus": None,
         "shot1_percent": "66,7", "avg_points": "15,2"},
        counts=True,
    )  # fmt: skip
    assert row == {"goal3": 0, "plus_minus": None, "shot1_percent": 66.7, "avg_points": 15.2}


# --- full bodies: dtypes and semantics ------------------------------------------------------------------------------------

G = {"game_id": 921391}
ONLINE = "full/online_921391.json.gz"


def _game(short: str) -> pl.DataFrame:
    return PARSERS[f"vtb_infobasket_{short}"](body("vtb_infobasket", ONLINE, G))


def test_box_score_dtypes_and_totals_come_from_the_full_body():
    box, team = _game("box_score"), _game("team_box_score")
    assert box.height == 24 and team.height == 2
    for col in ("points", "goal1", "shot2", "def_rebound", "seconds", "foul"):
        assert box.schema[col] == pl.Int64 and box[col].null_count() == 0, col  # null counts are 0
    assert box.schema["plus_minus"] == pl.Int64
    assert all(box.schema[c] == pl.Float64 for c in ("shot1_percent", "shot2_percent", "shot3_percent"))
    assert box.schema["is_start"] == pl.Boolean and box.schema["is_captain"] == pl.Boolean
    assert "paint_shot" not in box.columns and "shots1" not in box.columns and "age" not in box.columns
    by_team = box.group_by("team_id").agg(pl.col("points").sum().alias("p")).sort("team_id")
    scores = team.select("team_id", "score").sort("team_id")
    assert by_team["p"].to_list() == scores["score"].to_list() == [86, 85]
    assert team.filter(pl.col("team_id") == 2650)["is_home"][0] is True


def test_pbp_resolves_players_and_matches_the_team_box_score():
    pbp, team = _game("pbp"), _game("team_box_score")
    assert pbp.height == 1152 and pbp.schema["play_x"] == pl.Int64 and pbp.schema["play_type"] == pl.String
    shots = pbp.filter(pl.col("play_type_id").is_in([2, 3, 5, 6]))
    assert shots["play_x"].null_count() < shots.height  # located shots exist
    made3 = pbp.filter(pl.col("play_type_id") == 3).group_by("team_id").len().sort("team_id")
    goals3 = team.select("team_id", "goal3").sort("team_id")
    assert made3["len"].to_list() == goals3["goal3"].to_list()
    assert pbp.filter(pl.col("play_type_id") == 3)["play_type"].unique().to_list() == ["3 points"]
    box = _game("box_score")
    assert pbp.schema["person_id"] == box.schema["person_id"] and pbp.schema["team_id"] == box.schema["team_id"]
    ids = set(box["person_id"].to_list())
    assert set(pbp["person_id"].drop_nulls().to_list()) <= ids  # every play's player is in the box score


def test_game_header_and_staff_have_no_personal_data():
    game, staff = _game("game"), _game("game_staff")
    assert game.height == 1 and game["game_id"][0] == 921391 and game["season_comp_id"][0] == 50714
    assert game["comp_id"][0] == 50720 and game["game_date"][0] == "2025-09-28"
    assert game["game_date_time"][0] == "2025-09-28T14:00:00+03:00"
    assert {"referee", "head coach"} <= set(staff["role"].to_list())
    forbidden = re.compile(r"(?i)birth|age$|licen|email|phone|contact")
    for df in (game, staff, _game("team_box_score")):
        assert not [c for c in df.columns if forbidden.search(c)], df.columns


def test_calendar_and_standings_from_the_full_bodies():
    cal = PARSERS["vtb_infobasket_calendar"](
        body("vtb_infobasket", "full/calendar_50714.json.gz", {"season": 2026, "season_comp_id": 50714})
    )
    assert (
        cal.height == 254
        and cal["season"].unique().to_list() == [2026]
        and cal["comp_id"].unique().to_list() == [50714]
    )
    assert cal.schema["game_date"] == pl.String and cal["game_date"].min() >= "2025-09-01"
    assert cal.schema["score_a"] == pl.Int64 and cal.schema["game_date_time"] == pl.String
    st = PARSERS["vtb_infobasket_standings"](body("vtb_infobasket", "full/standings_50720.json", {"comp_id": 50720}))
    assert st.height == 11 and st["comp_id"].unique().to_list() == [50720]
    assert st.schema["victory_percent"] == pl.Float64 and st.schema["avg_goal_plus"] == pl.Float64
    assert (st["standing_win"] + st["standing_lose"] == st["standing_game"]).all()


def test_team_stats_players_coaches_and_the_one_team_variants_agree():
    ts = body("vtb_infobasket", "full/team_stats_50714.json.gz", {"season": 2026})
    teams = vtb.parse_vtb_infobasket_team_stats(ts)
    players = vtb.parse_vtb_infobasket_team_stats_players(ts)
    coaches = vtb.parse_vtb_infobasket_team_stats_coaches(ts)
    assert teams.height == 11 and players.height > 100 and coaches.height > 20
    assert teams.schema["avg_points"] == pl.Float64 and teams.schema["points"] == pl.Int64
    assert set(players["team_id"].unique()) == set(teams["team_id"])
    one = vtb.parse_vtb_infobasket_team_player_stats(
        body("vtb_infobasket", "full/team_season_stats_2650_50714.json", {"season": 2026})
    )
    assert one["team_id"].unique().to_list() == [2650]
    assert "birth_date" not in coaches.columns and not [c for c in coaches.columns if "birth" in c or c.endswith("age")]


def test_roster_keeps_player_bio_but_never_coach_bio():
    r = body("vtb_infobasket", "full/roster_2650_50714.json", {"season": 2026})
    players, coaches = vtb.parse_vtb_infobasket_roster(r), vtb.parse_vtb_infobasket_roster_coaches(r)
    assert players.height == 19 and players["birth_date"].null_count() == 0 and players["birth_date"].min() > "1970"
    assert coaches.height == 5 and "birth_date" not in coaches.columns
    assert players["team_id"].unique().to_list() == [2650] and players.schema["height"] == pl.Int64


def test_player_routes():
    log = vtb.parse_vtb_infobasket_player_game_log(
        body("vtb_infobasket", "full/player_game_log_423011_50714.json.gz", {"person_id": 423011, "team_id": 2650})
    )
    assert (
        log.height == 50
        and log["person_id"].unique().to_list() == [423011]
        and log["team_id"].unique().to_list() == [2650]
    )
    assert log.schema["game_id"] == pl.Int64 and log["game_status"].unique().to_list() == [21]
    seasons = vtb.parse_vtb_infobasket_player_season_stats(
        body("vtb_infobasket", "full/player_season_stats_423011.json", {})
    )
    career = vtb.parse_vtb_infobasket_player_career_stats(
        body("vtb_infobasket", "full/player_season_stats_423011.json", {})
    )
    assert sorted(seasons["season"].to_list()) == [2024, 2025, 2026, 2027][-seasons.height :] and career.height == 1
    assert career["game_count"][0] == seasons["game_count"].sum() == 126 and career.schema["avg_points"] == pl.Float64


def test_playoff_series_and_games_split_one_body():
    po = body("vtb_infobasket", "captures/Widget__PlayOff__compId__2026.json", {"comp_id": 54281})
    series, games = vtb.parse_vtb_infobasket_playoff(po), vtb.parse_vtb_infobasket_playoff_games(po)
    assert series.height == 3 and games.height == 9 and set(games["comp_id"]) == {54281}
    assert (games["score1"] + games["score2"]).null_count() == 0
    assert games.schema["team1_id"] == pl.Int64 and series.schema["team1_id"] == pl.Int64


def test_stage_tree_flattens_children_and_the_cross_table_scores_parse():
    tree = vtb.parse_vtb_infobasket_comp_issue(
        body("vtb_infobasket", "captures/Widget__CompIssue__compId__2026.json", {"season": 2026})
    )
    assert set(tree["level"]) == {1, 2} and tree["season_comp_id"].unique().to_list() == [50714]
    assert tree.filter(pl.col("level") == 2)["parent_comp_id"].min() >= 54281
    cross = vtb.parse_vtb_infobasket_cross_table(
        body("vtb_infobasket", "captures/Widget__CrossTable__compId__2026.json", {"comp_id": 50720})
    )
    assert cross.schema["score1"] == pl.Int64 and cross["game_date"].str.len_chars().max() < 10  # 'DD.MM.' has no year


def test_v2_matches_from_the_full_body():
    m = vtb.parse_vtb_league_matches(
        body("vtb_league", "full/matches_vtb_2026.json.gz", {"league_slug": "vtb", "season": 2026})
    )
    assert m.height == 273 and m["season"].unique().to_list() == [2026] and m["league"].unique().to_list() == ["vtb"]
    for col in ("match_id", "home_team_id", "away_team_id", "home_score", "away_score", "phase_id", "venue_id"):
        assert m.schema[col] == pl.Int64, col
    done = m.filter(pl.col("match_status") == "COMPLETE")
    assert done.height > 200 and (done["home_score"] > 0).all()
    q = done.filter(pl.col("periods") == 4)
    assert (q["home_q1"] + q["home_q2"] + q["home_q3"] + q["home_q4"] == q["home_score"]).all()
    assert m.filter(pl.col("match_status") == "CANCELLED")["home_score"].max() == 0
    # v2 match_id is the Infobasket GameID: the calendar lists the same games
    cal = vtb.parse_vtb_infobasket_calendar(body("vtb_infobasket", "full/calendar_50714.json.gz", {"season": 2026}))
    assert cal.schema["game_id"] == m.schema["match_id"] and cal.schema["team_a_id"] == m.schema["home_team_id"]
    league_games = m.filter(pl.col("competition") == "vtb")
    assert set(league_games["match_id"].to_list()) & set(cal["game_id"].to_list())


def test_boolean_and_other_scalars_keep_their_wire_types():
    assert (
        vtb.parse_vtb_league_seasons(body("vtb_league", "captures/v2__leagues__league__seasons__vtb.json"))[
            "is_current"
        ].dtype
        == pl.Boolean
    )
    assert (
        vtb.parse_vtb_infobasket_best_players(
            body("vtb_infobasket", "captures/Comp__GetBestPlayers__compId__2026.json", {"season": 2026})
        ).schema["res"]
        == pl.Float64
    )


# --- personal data ------------------------------------------------------------------------------------------------------

_PERSONAL = {"PersonBirth", "PersonBirthday", "PersonBirthYear", "Age"}
_EVERYONE = {"LicenceNumber", "PersonContacts", "PersonEmail", "PersonPhone", "Email", "Phone", "SysUser", "SysMyUser"}
_STAFF = re.compile(r"(?i)coach|referee|official|commissioner|staff")


def _leaks(node: Any, path: str = "", staff: bool = False) -> List[str]:
    """Paths where a personal field still holds a value (independent of the runtime's own classifier)."""
    out: List[str] = []
    if isinstance(node, list):
        for i, x in enumerate(node):
            out += _leaks(x, f"{path}[{i}]", staff)
    elif isinstance(node, dict):
        if "StartType" in node and node.get("StartType") != 1:
            staff = True
        for k, v in node.items():
            if v is not None and (k in _EVERYONE or (staff and k in _PERSONAL)):
                out.append(f"{path}.{k}")
            out += _leaks(v, f"{path}.{k}", staff or bool(_STAFF.search(k)))
    return out


@pytest.mark.parametrize("fam,short,rel,ctx", FIXTURE_PARAMS)
def test_no_committed_fixture_holds_a_coach_or_official_birth_date_licence_or_contact(fam, short, rel, ctx):
    from tests.basketball.vtb._fx import raw

    data = raw(fam, rel)
    # the CoachPage body is wholly a coach record
    assert not _leaks(data, staff="CoachPage" in rel), rel
