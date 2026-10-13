"""Offline tests for the TPBL (``tpbl``) flat-API family: parsers, wrappers, YAML and schemas.

Parsers are asserted against real data: the FULL bodies of one game (6), one division and one season in
``tests/fixtures/tpbl/full/`` (dtypes come from those) and the refs capture samples of every route in
``tests/fixtures/tpbl/captures/``. No network, and nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import polars as pl
import pytest
import yaml

import sportsdataverse.basketball.tpbl as tpbl_pkg
from sportsdataverse.basketball.tpbl import tpbl as wrappers
from sportsdataverse.basketball.tpbl import tpbl_parsers as P

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "tpbl"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "tpbl.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "tpbl"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_tpbl  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(P, f"parse_tpbl_{n}") for n in P.SPECS}
BOX = ("box_players", "box_player_periods", "box_teams", "box_team_periods")


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _frame(short: str) -> pl.DataFrame:
    return gen_tpbl._frame(gen_tpbl.ENDPOINTS[short])


# --- parsers over every committed body ------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_every_wrapper_parses_its_fixture(short):
    df = _frame(short)
    assert df.height > 0 and list(df.columns) == list(P.SPECS[short]) and dict(df.schema) == dict(P.SPECS[short])
    assert all(SNAKE.match(c) for c in df.columns)
    for c, dt in df.schema.items():
        if c.endswith("_id") or c.startswith("gohoops_") or c in ("rank", "period", "periods"):
            assert dt == pl.Int64, (short, c)


def test_numeric_dtypes_come_from_the_full_bodies():
    players = _frame("box_players")
    assert players.schema["time_on_court"] == pl.Int64 and players.schema["score"] == pl.Int64
    for pct in ("field_goals_percentage", "efg", "tsp"):  # one-decimal strings on the wire
        assert players.schema[pct] == pl.Float64
    assert players["efg"].drop_nulls().max() <= 100.0 and (players["field_goals_percentage"].drop_nulls() % 1 > 0).any()
    assert _frame("player_games").schema["turnovers_percentage"] == pl.Float64
    assert _frame("roster").schema["weight_kg"] == pl.Float64 and _frame("roster").schema["height_cm"] == pl.Int64
    assert _frame("standings").schema["games_behind"] == pl.Float64


def test_the_stat_name_lists_match_the_full_box_score():
    """A key the API adds or drops must change the lists (and the schema), not slip through."""
    body = _full("game_stats_6")
    team = body["home_team"]
    full_line = next(v for v in team["players"]["total"].values() if len(v) > 20)
    text = {"field_goals", "two_pointers", "three_pointers", "free_throws"}
    identity = {
        "id",
        "gohoops_id",
        "gohoops_roster_id",
        "name",
        "status",
        "roster_status",
        "injury_status",
        "injury_status_label",
        "positions",
        "number",
        "is_starting",
    }
    always_null = {"won_score", "lost_score", "total_won_score", "total_lost_score"}
    assert list(P._PLAYER_STATS) == [k for k in full_line if k not in text | identity | always_null]
    assert list(P._TEAM_TOTAL_STATS) == [k for k in team["teams"]["total"] if k not in text]
    assert list(P._TEAM_PERIOD_STATS) == [k for k in team["teams"]["rounds"]["1"] if k not in text | {"round"}]
    assert always_null <= set(full_line) and all(full_line[k] is None for k in always_null)
    log = _full("player_games_75_div2")[0]
    assert len(P._LOG_STATS) == len(log["accumulated_stats"]) and len(P._LOG_PERCENTAGES) == len(
        log["percentage_stats"]
    )


def test_box_score_adds_up():
    box = _full("game_stats_6")
    players = _frame("box_players")
    teams = _frame("box_teams")
    # the game: 6, Hsinchu Lioneers 111-108 Taipei Mars (home first)
    assert players["game_id"].unique().to_list() == [6] and teams["game_id"].to_list() == [6, 6]
    assert teams["side"].to_list() == ["home", "away"] and teams["won_score"].to_list() == [111, 108]
    assert (
        teams["lost_score"].to_list() == [108, 111]
        and teams["opponent_team_id"].to_list()[::-1] == teams["team_id"].to_list()
    )
    by_side = players.group_by("side").agg(pl.col("score").sum().alias("pts"))
    assert dict(zip(by_side["side"], by_side["pts"])) == {"home": 111, "away": 108}
    assert players["gohoops_roster_id"].n_unique() == players.height  # one line per roster id
    assert (
        players.filter(pl.col("entered") == False)["score"].null_count()
        == players.filter(pl.col("entered") == False).height
    )  # noqa: E712
    assert players.filter(pl.col("entered") == True)["time_on_court"].null_count() == 0  # noqa: E712
    # team quarters add up to the team total, and the last running total is the final score
    q = _frame("box_team_periods")
    assert sorted(q["period"].unique().to_list()) == [1, 2, 3, 4]
    for side, final in (("home", 111), ("away", 108)):
        rows = q.filter(pl.col("side") == side).sort("period")
        assert rows["won_score"].sum() == final and rows["total_won_score"].to_list()[-1] == final
        assert rows["field_goals_made"].sum() == teams.filter(pl.col("side") == side)["field_goals_made"].item()
    # a player's quarters add up to his whole-game line
    qp = _frame("box_player_periods")
    assert qp.schema.get("efficiency") is None and qp["entered"].all()
    sums = qp.group_by("gohoops_roster_id").agg(pl.col("score").sum(), pl.col("time_on_court").sum())
    joined = sums.join(
        players.select("gohoops_roster_id", "score", "time_on_court"), on="gohoops_roster_id", suffix="_game"
    )
    assert joined.height == sums.height and (joined["score"] == joined["score_game"]).all()
    assert (joined["time_on_court"] == joined["time_on_court_game"]).all()
    assert box["home_team"]["id"] == teams["team_id"][0]


def test_box_score_joins_the_roster_on_the_roster_id_with_matching_dtypes():
    box, roster = _frame("box_players"), _frame("roster")
    assert box.schema["gohoops_roster_id"] == roster.schema["gohoops_roster_id"] == pl.Int64
    assert box.schema["player_id"] == roster.schema["player_id"] == pl.Int64
    both = box.join(roster, on="gohoops_roster_id", how="inner", suffix="_roster")
    assert both.height >= 5  # team 4 (Hsinchu) is in game 6 and the roster is the same squad-season
    assert (both["player_id"] == both["player_id_roster"]).all()  # roster id -> same person


def test_games_list_and_game_header():
    games = _frame("games")
    assert (
        games.height == 133
        and games["season"].unique().to_list() == [2027]
        and games["season_id"].unique().to_list() == [3]
    )
    played = games.filter(pl.col("status") == "COMPLETED")
    scheduled = games.filter(pl.col("status") == "NOT_STARTED")
    assert played.height == 7 and scheduled.height == 126  # 2026-27: preseason only so far
    assert scheduled["home_score"].null_count() == scheduled.height and scheduled["periods"].unique().to_list() == [0]
    assert played["home_score"].null_count() == 0 and played["attendance"].dtype == pl.Int64
    assert games.schema["game_date"] == pl.Date and games.schema["tipoff"] == pl.Datetime("us")
    assert games["game_id"].n_unique() == games.height
    one = _frame("game")
    assert one.height == 1 and one["game_id"][0] == 6 and one["home_score"][0] == 111 and one["away_score"][0] == 108
    assert one["periods"][0] == 4 and one["season"][0] is None and one["gohoops_season_id"][0] == 101
    assert pl.concat([one, games]).height == 134  # same columns: they stack


def test_referees_come_from_the_game_detail_with_names_only():
    refs = _frame("referees")
    assert refs.height == 3 and refs["game_id"].unique().to_list() == [6] and refs["sequence"].to_list() == [1, 2, 3]
    assert refs["role"][0] == "BASKETBALL_CHIEF_REFEREE" and set(refs.columns) == set(P.SPECS["referees"])


def test_seasons_and_divisions_read_the_end_year():
    seasons = _frame("seasons")
    assert seasons["season"].to_list() == [2025, 2026, 2027] and seasons["season_id"].to_list() == [1, 2, 3]
    assert seasons["gohoops_season_id"].to_list() == [101, 213, 275]
    divisions = _frame("divisions")
    assert divisions["division_id"].to_list() == [655, 656] and divisions["season"].unique().to_list() == [2027]
    bare = P.parse_tpbl_divisions(_cap("events__event_id__divisions@3"))  # no request stamp: season is unknown
    assert bare.height == 2 and bare["season"].null_count() == 2 and bare["division_id"].to_list() == [655, 656]
    teams = _frame("teams")
    assert teams["season"].unique().to_list() == [2026] and set(teams.columns) == set(P.SPECS["teams"])


def test_standings_and_head_to_head():
    table, h2h = _frame("standings"), _frame("standings_against")
    assert (
        table.height == 7
        and table["rank"].to_list() == list(range(1, 8))
        and table["division_id"].unique().to_list() == [2]
    )
    assert table["drawn"].sum() == 0 and table["games_behind"][0] == 0.0
    assert h2h.height == 7 * 6 and h2h.filter(pl.col("team_id") == pl.col("opponent_team_id")).height == 0
    # a team's wins over its opponents add up to its division wins (head-to-head covers every game)
    wins = h2h.group_by("team_id").agg(pl.col("won").sum())
    both = wins.join(table.select("team_id", "won"), on="team_id", suffix="_table")
    assert both.height == 7 and (both["won"] == both["won_table"]).all()
    assert pl.concat([table.head(0), table]).height == 7


def test_roster_drops_personal_data_and_keeps_bio():
    roster = _frame("roster")
    assert (
        roster.height == 13
        and roster["team_id"].unique().to_list() == [4]
        and roster["division_id"].unique().to_list() == [2]
    )
    assert not [c for c in roster.columns if "birth" in c or "contact" in c or "mail" in c or "phone" in c]
    assert roster["height_cm"].null_count() == 0 and roster["registered"].all()


def test_shot_lists_keep_coordinates_in_millimetres_and_the_request_scope():
    team, player = _frame("team_shots"), _frame("player_shots")
    assert team.height == 777 and team["rounds"].unique().to_list() == ["1"] and "rounds" not in player.columns
    assert team["point_x"].max() <= 28000 and team["point_y"].max() <= 15000 and team["point_x"].min() >= 0
    assert set(team["offense_basket"].unique().to_list()) <= {0, 1} and team["is_made"].dtype == pl.Boolean
    assert player["player_id"].unique().to_list() == [75] and player["division_id"].unique().to_list() == [2]
    bare = P.parse_tpbl_team_shots(_cap("divisions__division_id__teams__team_id__shooting-spots"))
    assert bare.height == 3 and bare["team_id"].null_count() == 3 and bare["rounds"].null_count() == 3


def test_player_game_log_is_one_row_per_game_with_the_box_vocabulary():
    log = _frame("player_games")
    assert log.height == 20 and log["game_id"].n_unique() == 20 and log["player_id"].unique().to_list() == [75]
    assert log["division_id"].unique().to_list() == [2] and log["gohoops_division_id"].unique().to_list() == [65]
    assert (log["pir"] == log["efficiency"] - log["fouls"]).all()  # pir = efficiency - fouls, as in the box score
    assert {"pir", "efg", "tsp"} <= set(log.columns) and not {"performance_index_rating"} & set(log.columns)
    assert set(P._PLAYER_STATS) - {"won_score"} >= set()  # the box vocabulary is shared where the API shares it
    shared = (set(P._PLAYER_STATS) & set(log.columns)) - {"efficiency", "pir"}
    assert len(shared) > 100
    assert (log["score"] == log["field_goals_made"] * 2 + log["three_pointers_made"] + log["free_throws_made"]).all()


# --- the parser contract ---------------------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", 7, {"data": []}, {"home_team": None}, {"id": "x"}, [1, None]])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = _frame(short)
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_return_as_pandas(short):
    entry = gen_tpbl.ENDPOINTS[short]
    df = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and list(df.columns) == list(P.SPECS[short]) and len(df) > 0


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_tpbl_seasons({}, columns=["season", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.String


def test_no_float_origin_id_strings():
    for short in P.SPECS:
        df = _frame(short)
        for c, dt in df.schema.items():
            if dt == pl.String and (c.endswith("_id") or c == "number"):
                assert not df[c].drop_nulls().str.contains(r"\.0$").any(), (short, c)
            if c.endswith("_id"):
                assert dt == pl.Int64, (short, c)


# --- every capture (3-row samples) still parses ----------------------------------------------------

CAPTURE_PARSERS = {
    "seasons": ("seasons",),
    "events__event_id__divisions": ("divisions",),
    "events__event_id__teams": ("teams",),
    "seasons__season_id__games": ("games",),
    "divisions__division_id__games__standings": ("standings", "standings_against"),
    "games__game_id": ("game", "referees"),
    "games__game_id__stats": BOX,
    "divisions__division_id__teams__team_id__players": ("roster",),
    "divisions__division_id__teams__team_id__shooting-spots": ("team_shots",),
    "divisions__division_id__players__player_id__shooting-spots": ("player_shots",),
    "players__player_id__games__stats": ("player_games",),
}


def test_every_capture_is_covered_and_parses_to_the_documented_schema():
    files = sorted(p.stem for p in (FIXTURES / "captures").glob("*.json"))
    assert len(files) == 27
    seen = set()
    for stem in files:
        route = stem.split("@")[0]
        assert route in CAPTURE_PARSERS, stem
        seen.add(route)
        body = _cap(stem)
        for short in CAPTURE_PARSERS[route]:
            df = ALL_PARSERS[short](body)
            assert dict(df.schema) == dict(P.SPECS[short]), (stem, short)
            if short not in ("standings_against",):
                assert df.height > 0, (stem, short)
    assert seen == set(CAPTURE_PARSERS)


def test_overtime_game_keeps_its_extra_quarters():
    box = _cap("games__game_id__stats")  # game 69, two overtimes, quarters 1, 2 and 6 survive the 3-key cut
    periods = P.parse_tpbl_box_team_periods(box)["period"].unique().sort().to_list()
    assert 6 in periods and P.parse_tpbl_box_teams(box).height == 2


# --- the YAML, schemas and generator ---------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 11
    assert spec_paths == {e[0] for e in gen_tpbl.ENDPOINTS.values()}
    doc = _yaml()
    assert {(e["short"], e["path"]) for e in doc["endpoints"]} == {(s, e[1]) for s, e in gen_tpbl.ENDPOINTS.items()}
    assert len(doc["endpoints"]) == 16 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    assert doc["host"] == "https://api.tpbl.basketball/api" and not any("host" in e for e in doc["endpoints"])
    assert doc["getter_module"] == "sportsdataverse.basketball.tpbl.tpbl_runtime"


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_season_keyed_routes_take_the_end_year_and_the_rest_the_api_ids():
    by = {e["short"]: e for e in _yaml()["endpoints"]}
    for short in ("divisions", "teams", "games"):
        assert [p["name"] for p in by[short]["path_params"]] == ["season"] and by[short]["example_args"] == {
            "season": 2026
        }
        assert "END year" in by[short]["path_params"][0]["description"]
    assert [p["name"] for p in by["roster"]["path_params"]] == ["division_id", "team_id"]
    assert [p["name"] for p in by["player_games"]["extra_params"]] == ["division_id"]
    assert by["player_games"]["extra_params"][0]["required"] is True
    assert {p["name"] for p in by["games"]["extra_params"]} == {"team_id", "verify"}
    assert [p["name"] for p in by["team_shots"]["extra_params"]] == ["rounds"]


def test_yaml_carries_the_unofficial_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    for needle in (
        "Unofficial",
        "robots.txt",
        "not redistributed",
        "FINISHED",
        "NO play-by-play",
        "Traditional Chinese",
        "gohoops_roster_id",
        "verify=True",
    ):
        assert needle in notes, needle


@pytest.mark.parametrize("short", sorted(gen_tpbl.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in the notes file


def test_the_description_notes_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_tpbl.NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"tpbl_{s}" for s in gen_tpbl.ENDPOINTS}
    for key, cols in notes.items():
        assert set(cols) == set(P.SPECS[key[5:]]), key
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col and not gen_tpbl._BANNED.match(text), (key, col)


def test_traditional_chinese_is_translated_and_unverified_caveats_stay_in_the_descriptions():
    notes = yaml.safe_load(gen_tpbl.NOTES.read_text(encoding="utf-8"))
    assert "Kaohsiung Aquas" in notes["tpbl_games"]["home_team"] and "preseason" in notes["tpbl_divisions"]["name"]
    assert "Xinzhuang" in notes["tpbl_games"]["venue"] and "play-in" in notes["tpbl_divisions"]["name"]
    assert "unverified" in notes["tpbl_team_shots"]["point_x"] and "unverified" in notes["tpbl_team_shots"]["point_y"]
    assert "unverified" in P.parse_tpbl_team_shots.__doc__
    assert (
        "UTC+8" in notes["tpbl_games"]["tipoff"]
        and "join tpbl_box_players to tpbl_roster" in notes["tpbl_box_players"]["gohoops_roster_id"]
    )
    assert "as of the request" in notes["tpbl_box_players"]["status"]
    # the stat lines are described from the returns md, per wrapper wording
    assert "whole game" in notes["tpbl_box_players"]["score"] and "quarter" in notes["tpbl_box_player_periods"]["score"]


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert set(P.__all__) <= set(tpbl_pkg.__all__)
    assert len(P.__all__) == 16 and len(wrappers.__all__) == 16
    assert all(n.startswith("tpbl_") for n in wrappers.__all__)


def test_the_rendered_module_exposes_one_wrapper_per_endpoint():
    assert set(wrappers.__all__) == {f"tpbl_{e['short']}" for e in _yaml()["endpoints"]}
    assert set(wrappers._PARSER_COLUMNS) == set(P.SPECS)
    for short, cols in wrappers._PARSER_COLUMNS.items():
        assert cols == list(P.SPECS[short]), short


def test_no_fixture_holds_personal_contact_or_birth_fields():
    pattern = re.compile(r'"([^"]*(?:birth|e_?mail|phone|mobile|address|contact)[^"]*)"\s*:\s*(.*)', re.I)
    for path in FIXTURES.rglob("*.json"):
        for m in pattern.finditer(path.read_text(encoding="utf-8")):
            assert m.group(2).startswith('"<redacted>"'), (path.name, m.group(1))
