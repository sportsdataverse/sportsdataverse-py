"""Offline tests for the ACB (``acb``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies of match 105401 (and the round, standings and club
season bodies) in ``tests/fixtures/acb/full/`` (dtypes come from those), and the refs capture samples in
``tests/fixtures/acb/captures/`` for the routes without a full body (and the legacy 2016 / 2005 matches). The
runtime is driven through a scripted ``requests`` adapter mounted on a ``PoliteSession``, so every refusal is
exercised end to end and every attempt is counted. No network, and nothing reads the private sdv-internal-refs
checkout.
"""

from __future__ import annotations

import copy
import gzip
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.acb as acb_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.acb import acb_codes
from sportsdataverse.basketball.acb import acb_parsers as P
from sportsdataverse.basketball.acb import acb_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "acb"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "acb.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "acb"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_acb  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
MATCH = 105401
ALL = sorted(gen_acb.ENDPOINTS)


def _full(name: str) -> Dict[str, Any]:
    path = FIXTURES / "full" / name
    text = gzip.open(path, "rt", encoding="utf-8").read() if name.endswith(".gz") else path.read_text("utf-8")
    return json.loads(text)


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _stamped(body: Dict[str, Any], **req: Any) -> Dict[str, Any]:
    out = copy.deepcopy(body)
    out[R.REQUEST_KEY] = req
    return out


# --- parsers over every committed body ----------------------------------------------------------------


@pytest.mark.parametrize("short", ALL)
def test_every_wrapper_parses_its_fixture(short):
    df = gen_acb._frame(short)
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short
    ids = [c for c in df.columns if c.endswith("_id") or c in ("season", "edition_id")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}, short
    # an id is never a float-origin string
    for c in df.columns:
        if df.schema[c] == pl.Utf8 and (c.endswith("_id") or c == "shirt_number"):
            assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)


def test_every_parser_has_a_field_table_endpoint_and_extractor():
    assert set(P.FIELDS) == set(gen_acb.ENDPOINTS) == set(P._EXTRACT) == set(P.SPECS)
    assert len(P.__all__) == 45 == len(set(P.__all__))
    for short in P.FIELDS:
        cols = [c for c, _t, _p in P.FIELDS[short]]
        assert len(cols) == len(set(cols)), (short, [c for c in cols if cols.count(c) > 1])


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_acb_player_box(_stamped(_full("boxscores.json"), match_id=MATCH))
    assert all(box.schema[c] == pl.Int64 for c, _ in P.BOX_STATS)
    assert box.schema["seconds_played"] == pl.Int64 and box.schema["shirt_number"] == pl.Utf8
    assert box.schema["is_starter"] == pl.Boolean and box.schema["play_time"] == pl.Utf8
    stats = P.parse_acb_team_stats(_stamped(_full("team-stats.json"), club_id=2))
    # the API sends whole-number averages and percentages as integers: the schema must not follow the sample
    assert all(stats.schema[c] == pl.Float64 for c in stats.columns if c.startswith(("avg_", "per40_")))
    assert all(
        stats.schema[f"total_{c}"] == pl.Float64 for c in ("three_pointers_percentage", "two_pointers_percentage")
    )
    assert stats.schema["total_points"] == pl.Int64 and stats.schema["total_time_played"] == pl.Int64
    table = P.parse_acb_standings(_full("standings.json"))
    assert table.schema["win_pct"] == pl.Float64 and table.schema["position"] == pl.Int64
    rounds = P.parse_acb_round_matches(_full("matches.json"))
    assert rounds.schema["start"] == pl.Datetime("us") and rounds.schema["home_team_id"] == pl.Int64


def test_header_and_period_scores():
    body = _stamped(_full("match-header.json"), match_id=MATCH)
    head = P.parse_acb_match_header(body)
    assert head.height == 1
    row = head.row(0, named=True)
    assert (row["match_id"], row["competition_id"], row["status"]) == (MATCH, 1, "FINALIZED")
    assert (row["home_score"], row["away_score"]) == (89, 86)
    assert (row["home_team_id"], row["home_club_id"], row["away_club_id"]) == (4467, 2, 13)
    assert (row["edition_id"], row["season"]) == (91, 2027)
    assert row["start"].isoformat() == "2026-10-11T17:00:00"
    periods = P.parse_acb_period_scores(body)
    assert periods["quarter"].to_list() == [1, 2, 3, 4]
    assert (periods["home_score"].sum(), periods["away_score"].sum()) == (89, 86)  # per-period, not cumulative


def test_box_score_adds_up_to_the_header_and_the_team_totals():
    body = _stamped(_full("boxscores.json"), match_id=MATCH)
    box = P.parse_acb_player_box(body)
    whole = box.filter(pl.col("quarter") == 0)
    pts = dict(whole.group_by("team_side").agg(pl.col("points").sum()).iter_rows())
    assert pts == {"home": 89, "away": 86}
    assert sorted(box["quarter"].unique().to_list()) == [0, 1, 2, 3, 4]
    totals = P.parse_acb_team_box(body)
    total = totals.filter((pl.col("scope") == "total") & (pl.col("quarter") == 0))
    assert total["points"].to_list() == [89, 86] and total["team_side"].to_list() == ["home", "away"]
    # a team's total is its players plus the part credited to the team itself
    own = totals.filter((pl.col("scope") == "team") & (pl.col("quarter") == 0))
    for side in ("home", "away"):
        got = (
            whole.filter(pl.col("team_side") == side)["total_rebounds"].sum()
            + own.filter(pl.col("team_side") == side)["total_rebounds"].item()
        )
        assert got == total.filter(pl.col("team_side") == side)["total_rebounds"].item()
    info = P.parse_acb_box_info(body)
    assert info.height == 2 and info["arena"].unique().to_list() == ["Palau Blaugrana"]
    assert info["attendance"].unique().to_list() == [5342]
    assert info["head_coach"].to_list() == ["Aleksander Sekulic", "Xavier Albert"]
    assert "; " in info["referees"][0] and "; " in info["assistant_coaches"][0]
    # play_time and seconds_played agree, ids are the licence number
    one = whole.filter(pl.col("player_id") == 30003361).row(0, named=True)
    assert (one["play_time"], one["seconds_played"], one["shirt_number"], one["is_starter"]) == (
        "23:42",
        1422,
        "0",
        True,
    )


def test_old_box_scores_carry_only_the_whole_game_period():
    box = P.parse_acb_player_box(_stamped(_cap("matchdata__Result__boxscores__2005"), match_id=12559))
    assert box["quarter"].unique().to_list() == [0] and box["season"].unique().to_list() == [2006]


def test_pbp_clock_codes_and_running_stats():
    pbp = P.parse_acb_pbp(_stamped(_full("play-by-play.json.gz"), match_id=MATCH))
    assert pbp.height == 735 and pbp["play_index"].to_list() == list(range(1, 736))
    assert pbp["order"].n_unique() < pbp.height  # the order key is not unique (the starting five share one)
    first = pbp.row(0, named=True)
    assert (first["play_type"], first["play_name"], first["clock_seconds"]) == (599, "STARTING_LINEUP", 600)
    tagged = pbp.filter(pl.col("play_tag").is_not_null())
    assert tagged.filter(pl.col("play_tag") != 1)["turnover_type"].is_in(list(acb_codes.PLAY_TAGS.values())).all()
    assert (
        tagged.filter(pl.col("play_tag") == 1)["turnover_type"].null_count()
        == tagged.filter(pl.col("play_tag") == 1).height
    )
    last_score = pbp.filter(pl.col("score_home").is_not_null()).sort("play_index").row(-1, named=True)
    assert (last_score["score_home"], last_score["score_away"]) == (89, 86)
    assert pbp.filter(pl.col("play_type") == 600).select(pl.col("play_name").null_count()).item() > 0  # unmapped code
    assert pbp.schema["run_points"] == pl.Int64 and pbp.schema["player_id"] == pl.Int64
    # the running points of each player never go down
    grouped = pbp.filter(pl.col("player_id").is_not_null()).group_by("player_id", maintain_order=True)
    assert all(g["run_points"].is_sorted() for _k, g in grouped)


def test_shots_join_the_pbp_and_branch_on_the_coordinate_system():
    shots = P.parse_acb_shots(_stamped(_full("match-shots.json"), match_id=MATCH))
    pbp = P.parse_acb_pbp(_stamped(_full("play-by-play.json.gz"), match_id=MATCH))
    assert shots.height == 188 and shots["coord_system"].unique().to_list() == ["mm"]
    made = shots.filter(pl.col("made") == True)  # noqa: E712
    fg = pbp.filter(pl.col("play_type").is_in([93, 94, 100]))
    assert made.filter(pl.col("play_type") != 92).height == fg.height  # every made field goal is in both
    # a join on the licence number keeps one dtype
    assert shots.schema["player_id"] == pbp.schema["player_id"] == pl.Int64
    assert (
        shots.join(pbp.select("player_id", "player_name").unique("player_id"), on="player_id", how="left").height == 188
    )
    ft = shots.filter(pl.col("play_type").is_in([92, 96]))
    assert ft.height > 0 and ft["x_basket_m"].null_count() == ft.height and ft["zone"].null_count() == ft.height
    fgs = shots.filter(~pl.col("play_type").is_in([92, 96]))
    assert (fgs["x_basket_m"] * 1000 == fgs["pos_x"]).all() and fgs["player_name"].null_count() == 0
    # threes are at least the FIBA arc away from the basket (the refs' evidence for the unit and origin)
    threes = fgs.filter(pl.col("play_type").is_in([94, 98]))
    assert (((threes["pos_x"] ** 2 + threes["pos_y"] ** 2) ** 0.5) >= 6700).all()
    legacy = P.parse_acb_shots(_stamped(_cap("matchdata__MatchShots__match-shots__2016"), match_id=17718))
    assert legacy["coord_system"].unique().to_list() == ["grid"] and legacy["x_basket_m"].null_count() == legacy.height
    assert P.parse_acb_shots(_cap("matchdata__MatchShots__match-shots__2016"))["coord_system"].null_count() == 3


def test_lead_tracker_lineup_and_leaders():
    lead = P.parse_acb_lead_tracker(_stamped(_full("lead-tracker.json"), match_id=MATCH))
    assert lead.height == 97 and lead.row(-1, named=True)["margin"] == 3
    assert (lead["margin"] == lead["home_score"] - lead["away_score"]).all()
    stats = P.parse_acb_lead_stats(_stamped(_full("lead-tracker.json"), match_id=MATCH))
    assert sorted(stats["stat"].unique().to_list()) == [
        "best_score_run",
        "lead_changes",
        "max_difference",
        "time_ahead",
    ]
    assert stats.filter((pl.col("stat") == "time_ahead") & (pl.col("side") == "home"))["value"].item() == "25:37"
    lineup = P.parse_acb_lineup(_stamped(_full("lineup.json"), match_id=MATCH))
    assert lineup.height == 24 and lineup.group_by("team_side").len()["len"].to_list() == [12, 12]
    assert lineup.filter(pl.col("team_side") == "home")["lineup_order"].to_list() == list(range(1, 13))
    leaders = P.parse_acb_match_leaders(_stamped(_full("match-leaders.json"), match_id=MATCH))
    top = leaders.filter((pl.col("metric") == "points") & (pl.col("rank") == 1) & (pl.col("team_side") == "home"))
    assert top.row(0, named=True)["value"] == 13.0 and top["player_name"].item() == "Kevin Punter"
    assert leaders.schema["value"] == pl.Float64


def test_team_comparison_parses_decimal_comma_bars():
    cmp_ = P.parse_acb_team_comparison(_stamped(_cap("matchdata__Overview__match-team-comparison"), match_id=MATCH))
    fg = cmp_.filter((pl.col("quarter") == 0) & (pl.col("stat") == "field_goals")).row(0, named=True)
    assert (fg["home_value"], fg["home_made"], fg["home_attempted"], fg["away_attempted"]) == (47.06, 32, 68, 77)
    reb = cmp_.filter((pl.col("quarter") == 0) & (pl.col("stat") == "rebounds")).row(0, named=True)
    assert (reb["home_value"], reb["away_value"], reb["home_made"]) == (45.0, 37.0, None)


def test_advanced_stats_are_long_and_float():
    adv = P.parse_acb_match_advanced_stats(
        _stamped(_cap("matchdata__AdvancedStats__match-advanced-stats"), match_id=MATCH)
    )
    assert adv["team_side"].unique().sort().to_list() == ["away", "home"]
    row = adv.filter((pl.col("team_side") == "home") & (pl.col("stat") == "efg_pct")).row(0, named=True)
    assert (row["group"], row["match_value"], row["season_value"], row["season_diff"]) == (
        "four_factors",
        51.5,
        59.6,
        -8.1,
    )
    player = P.parse_acb_player_advanced_stats(
        _stamped(_cap("matchdata__AdvancedStats__player-advanced-stats"), match_id=MATCH, player_id=20212243)
    )
    assert player["player_id"].unique().to_list() == [20212243]
    assert player.filter(pl.col("stat") == "ts_pct")["match_value"].item() == 101.4
    assert player.schema["match_value"] == pl.Float64


def test_serie_bracket_and_matchlists():
    serie = P.parse_acb_serie(_stamped(_cap("matchdata__Overview__serie"), match_id=17718))
    assert (
        serie["requested_match_id"].unique().to_list() == [17718]
        and serie["match_id"].n_unique() == 3
        and serie["series_home_club_id"].unique().to_list() == [9]
    )
    assert serie["series_home_wins"].unique().to_list() == [1] and serie["series_away_wins"].unique().to_list() == [3]
    assert serie.schema["match_id"] == pl.Int64 and serie["season"].unique().to_list() == [2017]
    bracket = P.parse_acb_bracket(_cap("seasondata__Competition__bracket"))
    assert bracket["stage"].to_list() == ["quarter_finals"] * 3 + ["semifinals"] * 2 + ["final"]
    games = P.parse_acb_bracket_games(_cap("seasondata__Competition__bracket"))
    assert games["stage"].n_unique() == 3 and games.height == 17
    mlist = P.parse_acb_matchlist(_stamped(_cap("matchdata__Menu__matchlist"), match_id=MATCH))
    assert mlist["match_grouping"].unique().to_list() == ["round"] and mlist["season"].unique().to_list() == [2027]
    qf = P.parse_acb_quickfacts(_stamped(_cap("matchdata__Preview__quickfacts"), match_id=MATCH))
    assert qf["team_side"].to_list() == ["home", "away"] and qf["last_results"][0].count("; ") >= 0


def test_round_calendar_and_standings_join_on_one_dtype():
    rounds = P.parse_acb_round_matches(_full("matches.json"))
    table = P.parse_acb_standings(_full("standings.json"))
    assert rounds.height == 9 and rounds["round_id"].unique().to_list() == [6017]
    assert rounds["round_number"].unique().to_list() == [3] and rounds["season"].unique().to_list() == [2027]
    assert rounds["home_team"].null_count() == 0 and rounds["away_club_id"].null_count() == 0
    # the join key has one dtype on both sides, and every game team is in the table
    assert rounds.schema["home_team_id"] == table.schema["team_id"]
    joined = rounds.join(table.select("team_id", "position"), left_on="home_team_id", right_on="team_id", how="inner")
    assert joined.height == rounds.height
    assert sorted(table["position"].to_list()) == list(range(1, 19)) and table["total_rounds"].unique().to_list() == [
        34
    ]
    assert table["club_id"].null_count() == 0 and table.schema["club_id"] == pl.Int64
    history = P.parse_acb_standings_rounds(_full("standings.json"))
    assert history.height == 18 * 4 and history["team_id"].n_unique() == 18
    assert "; " in rounds["broadcasters"][0]
    cal = P.parse_acb_calendar(_cap("seasondata__Calendar__season-calendar"))
    assert cal["round_id"].null_count() == 0 and cal["week_id"].null_count() == 9  # the calendar sends no week
    assert 0 < cal["home_club_id"].null_count() < 9  # the capture cuts the team list to 3 rows


def test_team_season_routes():
    stats = P.parse_acb_team_stats(_stamped(_full("team-stats.json"), club_id=2))
    totals = P.parse_acb_team_totals(_stamped(_full("team-stats.json"), club_id=2))
    assert stats.height == 14 and totals.height == 1
    assert (
        stats["club_id"].unique().to_list() == [2]
        and totals["team_id"].item() == stats["team_id"].unique().item() == 4467
    )
    assert totals["total_points"].item() == stats["total_points"].sum()  # the team total is the sum of its players
    assert stats["per40_time_played"].null_count() == stats.height  # the per-40 block carries no time
    leaders = P.parse_acb_team_leaders(_stamped(_cap("seasondata__TeamProfile__team-leaders"), club_id=2))
    assert sorted(leaders["scope"].unique().to_list()) == ["all_time", "edition"]
    assert (
        leaders.filter(
            (pl.col("scope") == "all_time") & (pl.col("metric") == "played_matches") & (pl.col("rank") == 1)
        )["value"].item()
        == 689.0
    )
    roster = P.parse_acb_current_roster(_stamped(_cap("seasondata__TeamProfile__current-roster"), club_id=2))
    assert sorted(roster["role"].unique().to_list()) == ["player", "staff"] and roster[
        "club_id"
    ].unique().to_list() == [2]
    assert roster.filter(pl.col("person_name") == "Kevin Punter")["nationality_iso"].item() == "RS"
    records = P.parse_acb_team_records(_stamped(_cap("seasondata__TeamProfile__team-records"), club_id=2))
    assert records.height == 15 and records["game_date"].dtype == pl.Datetime("us")
    club = P.parse_acb_club_data(_cap("seasondata__TeamProfile__club-data"))
    assert club.row(0, named=True)["stadium_capacity"] == 7786 and club["club_id"].item() == 2
    zones = P.parse_acb_league_zones(_cap("seasondata__ShotChart__league-zones"))
    assert set(zones["zone"]) <= set(acb_codes.SHOT_ZONES) and zones["season"].unique().to_list() == [2027]
    teams = P.parse_acb_teams(_cap("seasondata__TeamProfile__teams"))
    assert (teams["team_id"] == teams["latest_team_id"]).all() and teams["season"].unique().to_list() == [2027]


def test_player_routes():
    data = P.parse_acb_player_data(_cap("seasondata__PlayerProfile__player-data"))
    row = data.row(0, named=True)
    assert (row["player_id"], row["birth_date"].isoformat(), row["height_m"], row["birth_country_iso"]) == (
        20209407,
        "1992-03-22",
        2.2,
        "CV",
    )
    stats = P.parse_acb_player_stats(_stamped(_cap("seasondata__PlayerProfile__player-stats"), player_id=20209407))
    career = P.parse_acb_player_totals(_cap("seasondata__PlayerProfile__player-stats"))
    assert stats["player_id"].unique().to_list() == [20209407] and career["total_num_games"].item() == 405
    stints = P.parse_acb_career_stints(_stamped(_cap("seasondata__PlayerProfile__career"), player_id=20209407))
    assert stints["n_editions"].to_list() == [3, 3] or stints["n_editions"].dtype == pl.Int64
    trophies = P.parse_acb_career_trophies(_stamped(_cap("seasondata__PlayerProfile__career"), player_id=20209407))
    assert trophies.schema["competition_id"] == pl.Int64 and trophies["player_id"].unique().to_list() == [20209407]
    log = P.parse_acb_player_matches(_stamped(_cap("seasondata__PlayerProfile__matches"), player_id=20209407))
    first = log.row(0, named=True)
    assert (first["play_time"], first["seconds_played"], first["has_won"], first["season"]) == (
        "19:52",
        1192,
        True,
        2027,
    )
    search = P.parse_acb_search(_cap("seasondata__Search__search"))
    assert search["kind"].to_list() == ["player"] and search["name"].item() == "Edy Tavares"
    assert search["person_id"].item() == 20209407


def test_competition_data_frames():
    body = _cap("matchdata__Menu__competition-data")
    eds = P.parse_acb_competition_editions(body)
    assert eds["season"].to_list() == [eds["season_start_year"][i] + 1 for i in range(eds.height)]
    assert eds["edition_id"].to_list() == [s - 1935 for s in eds["season_start_year"].to_list()]
    assert P.parse_acb_competition_rounds(body)["round_id"].null_count() == 0
    assert P.parse_acb_competition_weeks(body)["start_date"].dtype == pl.Datetime("us")
    cur = P.parse_acb_competition_current(body).row(0, named=True)
    assert (cur["competition_id"], cur["edition_id"], cur["season"], cur["round_id"]) == (1, 91, 2027, 6017)


def test_codes_decode_the_bundle_enums():
    assert acb_codes.PLAY_TYPES[93] == "TWO_POINTER_MADE" and acb_codes.PLAY_TAGS[12] == "BAD_PASS"
    assert len(acb_codes.PLAY_TYPES) == len(acb_codes.PLAY_TYPE_MEANINGS) >= 129
    assert acb_codes.COUNTRIES["EE.UU."] == ("United States", "US") and len(acb_codes.COUNTRIES) == 31
    assert len(acb_codes.SHOT_ZONES) == 10


# --- the parser contract --------------------------------------------------------------------------------

BAD = [
    {},
    [],
    None,
    "x",
    7,
    {"teams": None},
    {"plays": []},
    {"matches": [{}]},
    {"teamBoxscores": [None, 3]},
    {"stats": 4},
]


@pytest.mark.parametrize("short", ALL)
@pytest.mark.parametrize("bad", BAD)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = getattr(P, f"parse_acb_{short}")(bad)
    if df.height == 0:
        assert dict(df.schema) == dict(P.SPECS[short])
        assert df.columns == list(P.SPECS[short])
    else:  # a lone empty record row (e.g. {"matches": [{}]}) is still a typed row
        assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", ALL)
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = getattr(P, f"parse_acb_{short}")({})
    full = gen_acb._frame(short)
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height == pl.concat([full, empty]).height


@pytest.mark.parametrize("short", ALL)
def test_return_as_pandas(short):
    entry = gen_acb.ENDPOINTS[short]
    df = entry[1](entry[2](), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and list(df.columns) == list(P.SPECS[short]) and len(df) > 0


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_acb_pbp({}, columns=["order", "extra_col"])
    assert df.schema["extra_col"] == pl.Utf8 and df.height == 0 and df.columns[-1] == "extra_col"


def test_no_float_origin_id_strings_and_no_id_stringified():
    for short in ALL:
        df = gen_acb._frame(short)
        for c in df.columns:
            if c.endswith("_id"):
                assert df.schema[c] == pl.Int64, (short, c)


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc
    public = {n for n in dir(P) if n.startswith("parse_")}
    assert public == set(P.__all__)
    assert acb_pkg.__all__ and set(P.__all__) <= set(acb_pkg.__all__)
    assert len([n for n in acb_pkg.__all__ if n.startswith("acb_") and n != "acb_codes"]) == 45


# --- YAML, schemas, docs ----------------------------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    doc = _yaml()
    routes = json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(routes) == 33
    assert {e["path"] for e in doc["endpoints"]} == set(routes)
    assert [e["short"] for e in doc["endpoints"]] == list(gen_acb.ENDPOINTS) and len(doc["endpoints"]) == 45
    assert set(R._ROUTES) == {p for p in routes}  # the runtime knows exactly the spec's routes
    for e in doc["endpoints"]:
        assert e["parser"] == f"parse_acb_{e['short']}" and e["returns_schema"] == f"native/acb/{e['short']}"
        assert e["host"] if "host" in e else True


def test_no_path_token_named_league_or_sport():
    doc = _yaml()
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & {p["name"] for p in e.get("extra_params", [])}
        assert not re.search(r"\{(league|sport)\}", e["path"])


def test_yaml_params_are_the_runtimes_and_the_season_is_the_end_year():
    doc = _yaml()
    for e in doc["endpoints"]:
        required, optional = R._ROUTES[e["path"]][0], R._OPTIONAL.get(e["path"], ())
        keys = {p["query_key"] for p in e.get("extra_params", [])}
        assert keys == set(required) | set(optional), e["short"]
        assert {p["query_key"] for p in e.get("extra_params", []) if p.get("required")} == set(required), e["short"]
        for p in e.get("extra_params", []):
            if p["query_key"] == "season":
                assert "END year" in p["description"] and p["name"] == "season"
    assert "season" not in {
        p["query_key"] for e in doc["endpoints"] for p in e.get("extra_params", []) if p["name"] != "season"
    }


def test_yaml_carries_the_legal_key_season_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "sui generis database right" in notes and "prior express consent" in notes and "owner" in notes
    assert "X-APIKEY" in notes and "editionId = season - 1936" in notes and "coord_system" in notes


@pytest.mark.parametrize("short", ALL)
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short]) and schema["schema"] == f"acb_{short}"
    assert all(c["description"] == "" for c in schema["columns"])


def test_local_descriptions_name_real_columns_and_are_not_filler():
    for short, cols in gen_acb.LOCAL_BY.items():
        assert short in P.SPECS, short
        for col, text in cols.items():
            assert col in P.SPECS[short], (short, col)
            assert len(text) >= 15 and not gen_acb._BANNED.match(text), (short, col, text)
    for col, text in gen_acb.LOCAL.items():
        assert len(text) >= 15 and not gen_acb._BANNED.match(text), (col, text)


def test_unverified_caveats_stay_in_the_descriptions():
    text = " ".join(gen_acb.LOCAL.values()) + " ".join(t for c in gen_acb.LOCAL_BY.values() for t in c.values())
    assert text.count("unverified") >= 4 and "inferred by the refs" in text


# --- runtime: a scripted transport ----------------------------------------------------------------------------

JSON = {"content-type": "application/json; charset=utf-8"}
PROBLEM = {"content-type": "application/problem+json; charset=utf-8"}
TEXT = {"content-type": "text/plain; charset=utf-8"}
HTML = {"content-type": "text/html; charset=utf-8"}
API = "https://api2.acb.com/api"


class Acb(HTTPAdapter):
    """A scripted ACB: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = request.url
        resp.request = request
        return resp

    def api(self) -> List[requests.PreparedRequest]:
        return [c for c in self.calls if "api2.acb.com" in (c.url or "")]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount an Acb on a fresh polite session; the key cache starts empty and ``$ACB_API_KEY`` is unset."""
    monkeypatch.delenv(R.KEY_ENV, raising=False)

    def install(handler):
        adapter = Acb(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_key", {"value": None})
        return adapter

    return install


def _serve(body_for: Callable[[str], Any], status: int = 200, headers: Dict[str, str] = JSON):
    def handler(req):
        body = body_for(req.url)
        return status, headers, body if isinstance(body, (str, bytes)) else json.dumps(body)

    return handler


_FULL = {
    "/matchdata/MatchHeader/match-header": "match-header.json",
    "/matchdata/Result/boxscores": "boxscores.json",
    "/matchdata/PlayByPlay/play-by-play": "play-by-play.json.gz",
    "/matchdata/MatchShots/match-shots": "match-shots.json",
    "/matchdata/Overview/lead-tracker": "lead-tracker.json",
    "/matchdata/Overview/lineup": "lineup.json",
    "/matchdata/Overview/match-leaders": "match-leaders.json",
    "/seasondata/Competition/matches": "matches.json",
    "/seasondata/Competition/standings": "standings.json",
    "/seasondata/TeamProfile/team-stats": "team-stats.json",
}


def _by_route(url: str) -> Any:
    path = url.split("?")[0][len(API) :]
    return _full(_FULL[path])


def test_wrappers_end_to_end_headers_params_and_one_wait_per_request(site, waits):
    adapter = site(_serve(_by_route))
    assert acb_pkg.acb_player_box(match_id=MATCH).height == 120
    assert acb_pkg.acb_pbp(match_id=MATCH).height == 735
    assert acb_pkg.acb_shots(match_id=MATCH).height == 188
    assert acb_pkg.acb_standings(season=2027).height == 18
    assert acb_pkg.acb_team_stats(club_id=2, season=2027).height == 14
    calls = adapter.api()
    assert all(c.headers["X-APIKEY"] == R.DEFAULT_KEY and c.headers["Accept"] == "application/json" for c in calls)
    assert "matchId=105401" in calls[0].url and calls[0].url.startswith(f"{API}/matchdata/Result/boxscores")
    assert "editionId=91" in calls[3].url and "season=" not in calls[3].url  # END year mapped to the API's key
    assert "clubId=2" in calls[4].url and "editionId=91" in calls[4].url
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request


def test_return_parsed_false_hands_back_the_stamped_body(site):
    site(_serve(_by_route))
    raw = acb_pkg.acb_period_scores(match_id=MATCH, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"match_id": MATCH} and raw["matchId"] == MATCH
    assert acb_pkg.parse_acb_match_header(raw).height == 1 and acb_pkg.parse_acb_period_scores(raw).height == 4
    stand = acb_pkg.acb_standings(season=2027, return_parsed=False)
    assert stand[R.REQUEST_KEY] == {"edition_id": 91, "season": 2027}


def test_season_end_year_maps_to_the_edition_id():
    assert [R.edition_id(s) for s in (1984, 2017, 2027)] == [48, 81, 91]
    assert [R.season_of(e) for e in (48, 81, 91)] == [1984, 2017, 2027]
    assert R.edition_id("2027") == 91
    for bad in (1983, 0, -1, True, 1.5, None, "abc"):
        with pytest.raises(ValueError):
            R.edition_id(bad)


def test_a_body_for_another_season_is_refused_not_returned(site):
    site(_serve(_by_route))  # the standings fixture is Liga 2026-27 (edition 91)
    with pytest.raises(AssetFetchError, match="out of scope"):
        acb_pkg.acb_standings(season=2026)
    with pytest.raises(AssetFetchError, match="out of scope"):
        acb_pkg.acb_standings(season=2027, competition_id=2)
    with pytest.raises(AssetFetchError, match="out of scope"):
        acb_pkg.acb_standings(season=2027, round_id=1)


def test_a_header_for_another_match_is_refused(site):
    site(_serve(_by_route))
    with pytest.raises(AssetFetchError, match="out of scope"):
        acb_pkg.acb_match_header(match_id=MATCH + 1)


@pytest.mark.parametrize(
    "short,kwargs,route,mutate",
    [
        ("team_matches", {"club_id": 9, "season": 2027}, "team-matches", None),
        ("next_matches", {"club_id": 9}, "next-matches", None),
        ("team_records", {"club_id": 9}, "team-records", None),
        ("club_data", {"club_id": 9}, "club-data", None),
        ("team_stats", {"club_id": 9, "season": 2027}, "team-stats", None),
        ("player_data", {"player_id": 20212243}, "player-data", None),
        ("player_stats", {"player_id": 20212243}, "player-stats", None),
        ("search", {"search_term": "zzz"}, "search", None),
        ("league_zones", {"season": 2026}, "league-zones", None),
        ("teams", {"season": 2026}, "teams", None),
        ("calendar", {"season": 2026}, "season-calendar", None),
        ("bracket", {"season": 2027}, "bracket", None),
        ("competition_current", {"competition_ids": 10}, "competition-data", None),
    ],
)
def test_wrong_scope_bodies_are_asset_fetch_errors_never_empty_frames(site, short, kwargs, route, mutate):
    caps = {
        "team-matches": "seasondata__TeamProfile__team-matches",
        "next-matches": "seasondata__TeamProfile__next-matches",
        "team-records": "seasondata__TeamProfile__team-records",
        "club-data": "seasondata__TeamProfile__club-data",
        "team-stats": None,
        "player-data": "seasondata__PlayerProfile__player-data",
        "player-stats": "seasondata__PlayerProfile__player-stats",
        "search": "seasondata__Search__search",
        "league-zones": "seasondata__ShotChart__league-zones",
        "teams": "seasondata__TeamProfile__teams",
        "season-calendar": "seasondata__Calendar__season-calendar",
        "bracket": "seasondata__Competition__bracket",
        "competition-data": "matchdata__Menu__competition-data",
    }
    body = _full("team-stats.json") if caps[route] is None else _cap(caps[route])
    site(_serve(lambda u: body))
    with pytest.raises(AssetFetchError, match="out of scope"):
        getattr(acb_pkg, f"acb_{short}")(**kwargs)


def test_scope_checks_accept_the_matching_bodies(site):
    site(_serve(lambda u: _cap("seasondata__TeamProfile__team-matches")))
    assert acb_pkg.acb_team_matches(club_id=2, season=2027).height == 3
    site(_serve(lambda u: _cap("seasondata__Search__search")))
    assert acb_pkg.acb_search(search_term="TÁVARES ").height == 1  # accent- and case-insensitive
    site(_serve(lambda u: _cap("seasondata__PlayerProfile__player-data")))
    assert acb_pkg.acb_player_data(player_id=20209407).height == 1


def test_bool_and_list_params_are_encoded(site):
    two = _cap("matchdata__Menu__competition-data")
    two["competitions"] = [two["competitions"][0], {**two["competitions"][0], "id": 2}]
    adapter = site(_serve(lambda u: two))
    acb_pkg.acb_competition_rounds(competition_ids=[1, 2])
    assert adapter.api()[-1].url.count("competitionIds=") == 2
    adapter = site(_serve(lambda u: _full("matches.json")))
    acb_pkg.acb_round_matches(season=2027, round_id=6017, is_round_selected=True)
    assert "isRoundSelected=true" in adapter.api()[-1].url and "roundId=6017" in adapter.api()[-1].url


def test_the_key_comes_from_the_environment_and_is_never_in_an_error(site, monkeypatch):
    adapter = site(_serve(lambda u: b"", status=401, headers=JSON))
    monkeypatch.setenv(R.KEY_ENV, "env-key-0123456789abcdef")
    with pytest.raises(AssetFetchError) as exc:
        acb_pkg.acb_pbp(match_id=MATCH)
    assert adapter.api()[0].headers["X-APIKEY"] == "env-key-0123456789abcdef"
    assert "env-key-0123456789abcdef" not in str(exc.value) and R.DEFAULT_KEY not in str(exc.value)
    assert len(adapter.api()) == 1  # an env key is the caller's: no bundle re-read


def test_401_rereads_the_bundle_once_then_succeeds(site):
    new_key = "11111111-2222-3333-4444-555555555555"
    chunk = "/_next/static/chunks/app-1234.js"
    js = f'x,"sharedServicesConfigDotnet",0,{{baseUrl:"https://api2.acb.com",apiKey:"{new_key}"}},y'

    def handler(req):
        if req.url.rstrip("/") == "https://www.acb.com":
            return 200, HTML, f'<script src="{chunk}"></script>'
        if req.url.endswith(chunk):
            return 200, {"content-type": "application/javascript"}, js
        if req.headers["X-APIKEY"] == new_key:
            return 200, JSON, json.dumps(_full("lead-tracker.json"))
        return 401, JSON, b""

    adapter = site(handler)
    assert acb_pkg.acb_lead_tracker(match_id=MATCH).height == 97
    keys = [c.headers.get("X-APIKEY") for c in adapter.api()]
    assert keys == [R.DEFAULT_KEY, new_key]
    assert acb_pkg.acb_lead_tracker(match_id=MATCH).height == 97  # the renewed key is cached
    assert [c.headers.get("X-APIKEY") for c in adapter.api()][-1] == new_key


def test_a_key_the_api_keeps_rejecting_is_a_fetch_failure(site):
    def handler(req):
        if req.url.rstrip("/") == "https://www.acb.com":
            return 200, HTML, "<html>no chunks</html>"
        return 401, JSON, b""

    site(handler)
    with pytest.raises(AssetFetchError, match="HTTP 401") as exc:
        acb_pkg.acb_lead_tracker(match_id=MATCH)
    assert R.DEFAULT_KEY not in str(exc.value)


def test_404_is_no_data_204_is_zero_rows_and_400_is_a_value_error(site):
    site(_serve(lambda u: "not found", status=404, headers=TEXT))
    with pytest.raises(NoDataError):
        acb_pkg.acb_lineup(match_id=1)
    site(_serve(lambda u: b"", status=204, headers={}))
    # a game outside a playoff series: {} -> zero rows, as every runtime answers a 204 (EX-SYNC-a; was NoDataError)
    assert acb_pkg.acb_serie(match_id=MATCH, return_parsed=False) == {}
    assert acb_pkg.acb_serie(match_id=MATCH).height == 0
    site(_serve(lambda u: json.dumps({"title": "Bad Request", "status": 400}), status=400, headers=PROBLEM))
    with pytest.raises(ValueError, match="HTTP 400"):
        acb_pkg.acb_team_leaders(club_id=2, number_of_players=0)


def test_other_statuses_retry_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def handler(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_full("lead-tracker.json"))

    adapter = site(handler)
    assert acb_pkg.acb_lead_tracker(match_id=MATCH).height == 97
    assert len(adapter.api()) == 3 and len(waits.urls) == len(adapter.calls) == 3
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    adapter = site(_serve(lambda u: b"", status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        acb_pkg.acb_shots(match_id=MATCH)
    assert len(adapter.api()) == R.ATTEMPTS and len(waits.urls) == R.ATTEMPTS


@pytest.mark.parametrize(
    "body,headers",
    [
        (b"", JSON),
        (b"not json", JSON),
        (json.dumps({"title": "Bad", "status": 500, "traceId": "t"}), JSON),
        (json.dumps({"error": "x"}), JSON),
        (json.dumps([1, 2]), JSON),
        (json.dumps({}), JSON),
        (json.dumps({"plays": [], "matchFinished": None, "homeTeam": None}), JSON),
        (json.dumps({"teamBoxscores": [None], "arena": "", "referees": []}), JSON),
        (json.dumps({"x": 1}), HTML),
        (json.dumps({"x": 1}), PROBLEM),
    ],
)
def test_error_envelopes_stubs_and_non_json_bodies_are_refused(site, body, headers):
    site(_serve(lambda u: body, headers=headers))
    with pytest.raises((AssetFetchError, NoDataError)):
        acb_pkg.acb_player_box(match_id=MATCH)


def test_a_body_of_another_shape_is_a_shape_failure(site):
    site(_serve(lambda u: {"something": [1, 2, 3]}))
    with pytest.raises(AssetFetchError, match="not this route's shape"):
        acb_pkg.acb_player_box(match_id=MATCH)


def test_pre_2016_play_by_play_and_shots_are_no_data_not_empty_frames(site):
    empty_pbp = _cap("matchdata__PlayByPlay__play-by-play__2016")
    empty_pbp = {**empty_pbp, "plays": [], "matchFinished": False}
    site(_serve(lambda u: empty_pbp))
    with pytest.raises(NoDataError, match="no rows"):
        acb_pkg.acb_pbp(match_id=12559)
    empty_shots = {**_cap("matchdata__MatchShots__match-shots__2016"), "shotPoints": []}
    site(_serve(lambda u: empty_shots))
    with pytest.raises(NoDataError):
        acb_pkg.acb_shots(match_id=12559)


def test_an_unfinished_game_is_refused_not_returned_partial(site):
    live = {**_full("boxscores.json"), "matchFinished": False}
    site(_serve(lambda u: live))
    with pytest.raises(AssetFetchError, match="not finished"):
        acb_pkg.acb_player_box(match_id=MATCH)
    live_pbp = {**_full("play-by-play.json.gz"), "matchFinished": False}
    site(_serve(lambda u: live_pbp))
    with pytest.raises(AssetFetchError, match="not finished"):
        acb_pkg.acb_pbp(match_id=MATCH)


def test_a_club_with_no_games_is_no_data(site):
    body = {**_cap("seasondata__TeamProfile__team-matches"), "matches": []}
    site(_serve(lambda u: body))
    with pytest.raises(NoDataError):
        acb_pkg.acb_team_matches(club_id=2, season=2027)


def test_a_connection_failure_names_the_exception_class_only(site, monkeypatch):
    def boom(req):
        raise requests.ConnectionError("https://api2.acb.com/api/x?secret=1 refused")

    adapter = site(boom)
    adapter.handler = boom
    with pytest.raises(AssetFetchError) as exc:
        acb_pkg.acb_pbp(match_id=MATCH)
    assert "ConnectionError" in str(exc.value) and "secret" not in str(exc.value)


@pytest.mark.parametrize(
    "call",
    [
        lambda: acb_pkg.acb_pbp(match_id=None),
        lambda: acb_pkg.acb_pbp(match_id="abc"),
        lambda: acb_pkg.acb_pbp(match_id=True),
        lambda: acb_pkg.acb_pbp(match_id=1.5),
        lambda: acb_pkg.acb_pbp(match_id=-3),
        lambda: acb_pkg.acb_standings(season=1983),
        lambda: acb_pkg.acb_standings(season="x"),
        lambda: acb_pkg.acb_standings(competition_id=[1]),
        lambda: acb_pkg.acb_round_matches(is_round_selected="yes"),
        lambda: acb_pkg.acb_search(search_term="  "),
        lambda: acb_pkg.acb_competition_current(competition_ids=[]),
    ],
)
def test_arguments_are_validated_before_any_request(site, call):
    adapter = site(_serve(_by_route))
    with pytest.raises((ValueError, TypeError)):
        call()
    assert adapter.calls == []


def test_unsupported_kwargs_params_and_foreign_urls_are_refused(site):
    adapter = site(_serve(_by_route))
    with pytest.raises(TypeError, match="unsupported argument"):
        acb_pkg.acb_pbp(match_id=MATCH, timeout=5)
    with pytest.raises(ValueError, match="unsupported parameter"):
        acb_pkg.acb_pbp(match_id=MATCH, params={"editionId": 91})
    with pytest.raises(ValueError, match="not an ACB route"):
        R._get("https://example.com/api/matchdata/Result/boxscores", {"matchId": 1})
    with pytest.raises(ValueError, match="not an ACB route"):
        R._get(f"{API}/matchdata/Nope/nope", {"matchId": 1})
    assert adapter.calls == []


def test_the_runtime_tables_cover_every_wrapper_route():
    assert {e["path"] for e in _yaml()["endpoints"]} == set(R._ROUTES)
    for path, (required, rows, finished, scope) in R._ROUTES.items():
        assert isinstance(required, tuple) and isinstance(finished, bool)
        assert path in {e[0] for e in gen_acb.ENDPOINTS.values()}
        for _bpath, key in scope:
            assert key in {"matchId", "competitionId", "editionId", "roundId", "weekId", "clubId", "playerLicenceId"}
