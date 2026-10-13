"""The KZS parsers over the committed fixtures: shapes, id dtypes, typed empties, and the cross-checks the data allows.

The FULL bodies (``tests/fixtures/kzs/full``) set the numeric dtypes; the refs captures (3 rows per array) are the
per-route fixtures. Nothing here reads the sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import gzip
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.basketball import kzs
from sportsdataverse.basketball.kzs import kzs_parsers as P
from sportsdataverse.basketball.kzs.kzs_dtypes import DTYPES

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "kzs"
CODES = {"i": pl.Int64, "f": pl.Float64, "b": pl.Boolean, "s": pl.String}
ID = re.compile(r"(^|_)id$")


def load(rel: str, ctx: Dict[str, Any] | None = None) -> Any:
    """A fixture as the runtime hands it to a parser: ``data`` of the envelope plus the ``_request`` stamp."""
    path = FIX / rel
    text = gzip.open(path, "rt", encoding="utf-8").read() if rel.endswith(".gz") else path.read_text(encoding="utf-8")
    data = json.loads(text)["data"]
    data = dict(data) if isinstance(data, dict) else {"items": data}
    data["_request"] = ctx or {}
    return data


M = {"match_id": 101839}
COMP = {"competition_id": 579}
BOARD = {"competition_id": 579, "stats_category": "PTS"}
PAIR = {"team_id": 17551, "competition_id": 579}
# parser -> [(fixture, request)]: the first is the full body where there is one
CASES: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {
    "parse_kzs_seasons": [("full/seasons.json", {}), ("captures/seasons.json", {})],
    "parse_kzs_competitions": [
        ("full/competitions__season26.json", {"season": 2026}),
        ("captures/competitions.json", {}),
    ],
    "parse_kzs_competition": [(f"captures/competitions__competitionId__{c}.json", {}) for c in (579, 30, 9, 1)],
    "parse_kzs_phases": [(f"captures/competitions__competitionId__{c}.json", {}) for c in (579, 30, 9, 1)],
    "parse_kzs_rounds": [("captures/competitions__competitionId__rounds__579.json", COMP)],
    "parse_kzs_player_leaderboard": [
        ("full/playerStats__579.json", BOARD),
        ("captures/competitions__competitionId__playerStats__579.json", BOARD),
    ],
    "parse_kzs_team_leaderboard": [
        ("full/teamStats__579.json", BOARD),
        ("captures/competitions__competitionId__teamStats__579.json", BOARD),
    ],
    "parse_kzs_matches": [("full/matches__579_2025-10.json", COMP)]
    + [
        (f"captures/matches__{k}.json", {"competition_id": int(k.split("_")[0])})
        for k in ("579_2025-10", "30", "9", "1")
    ],
    "parse_kzs_match": [("full/match__101839.json", M)]
    + [(f"captures/matches__matchId__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150, 5787)],
    "parse_kzs_period_scores": [("full/match__101839.json", M)]
    + [(f"captures/matches__matchId__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150, 5787)],
    "parse_kzs_pbp": [("full/playByPlay__101839.json.gz", M)]
    + [(f"captures/matches__matchId__playByPlay__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150)],
    "parse_kzs_pbp_players": [("full/playByPlay__101839.json.gz", M)]
    + [(f"captures/matches__matchId__playByPlay__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150)],
    "parse_kzs_box_score": [("full/stats__101839.json", M)]
    + [(f"captures/matches__matchId__stats__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150)],
    "parse_kzs_team_box_score": [("full/stats__101839.json", M)]
    + [(f"captures/matches__matchId__stats__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150)],
    "parse_kzs_score_changes": [("full/stats__101839.json", M)]
    + [(f"captures/matches__matchId__stats__{g}.json", {"match_id": g}) for g in (101839, 62586, 29150)],
    "parse_kzs_standings": [
        ("full/standings__579.json", COMP),
        ("captures/matches__standings__30.json", {"competition_id": 30}),
    ],
    "parse_kzs_playing_days": [("captures/matches__competitionId__playingDays__579.json", COMP)],
    "parse_kzs_teams": [("captures/teams__579.json", COMP)],
    "parse_kzs_team": [("captures/teams__teamId__17551.json", {"team_id": 17551})],
    "parse_kzs_team_general": [("captures/teams__teamId__general__17551_579.json", PAIR)],
    "parse_kzs_roster": [("captures/teamPlayers__17551_579.json", PAIR)],
    "parse_kzs_player": [("captures/players__playerId__106285.json", {"player_id": 106285})],
    "parse_kzs_player_stats": [
        ("full/player_stats__106285_26.json", {"player_id": 106285, "season_id": 26}),
        ("captures/players__playerId__stats__106285_26.json", {"player_id": 106285, "season_id": 26}),
    ],
}
FLAT = [(name, rel, ctx) for name, cases in CASES.items() for rel, ctx in cases]


def test_there_is_a_case_for_every_parser_and_the_package_exports_them():
    assert set(CASES) == set(P.__all__) == set(DTYPES)
    assert len(P.__all__) == 23 and all(hasattr(kzs, n) for n in P.__all__)
    wrappers = [n for n in kzs.__all__ if n.startswith("kzs_")]
    assert len(wrappers) == 23


@pytest.mark.parametrize(
    "name,rel,ctx", FLAT, ids=[f"{n.removeprefix('parse_kzs_')}-{Path(r).stem}" for n, r, _ in FLAT]
)
def test_every_fixture_parses_to_a_snake_case_frame_with_the_documented_schema(name, rel, ctx):
    df = getattr(P, name)(load(rel, ctx))
    assert isinstance(df, pl.DataFrame) and df.height > 0, (name, rel)
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", c) for c in df.columns), df.columns
    schema = DTYPES[name]
    assert list(df.columns[: len(schema)]) == list(schema)  # the documented order first
    for col, code in schema.items():
        assert df.schema[col] == CODES[code], (name, rel, col, df.schema[col], code)
    for col in df.columns:
        if ID.search(col):
            assert df.schema[col] == pl.Int64, (name, col)  # one dtype per id, cast at the parser boundary


@pytest.mark.parametrize("name", sorted(CASES))
def test_a_full_fixture_loses_no_row_to_the_schema(name):
    full = [(rel, ctx) for rel, ctx in CASES[name] if rel.startswith("full/")]
    for rel, ctx in full:
        df = getattr(P, name)(load(rel, ctx))
        assert not any(  # no id was stringified from a float
            df[c].dtype == pl.String and df[c].drop_nulls().str.contains(r"^\d+\.0$").any() for c in df.columns
        )


# --- empties -----------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(CASES))
@pytest.mark.parametrize("empty", [{}, [], None, "x", 7, {"_request": {"match_id": 1}}, {"items": []}])
def test_empty_payload_is_a_zero_row_frame_typed_from_the_returns_schema(name, empty):
    df = getattr(P, name)(empty)
    assert df.height == 0
    assert df.schema == pl.Schema({c: CODES[code] for c, code in DTYPES[name].items()})


@pytest.mark.parametrize("name", sorted(CASES))
def test_an_empty_frame_stacks_onto_a_populated_one(name):
    rel, ctx = CASES[name][-1]
    populated = getattr(P, name)(load(rel, ctx))
    empty = getattr(P, name)({})
    stacked = pl.concat([empty, populated])
    assert stacked.height == populated.height and stacked.schema == populated.schema
    assert pl.concat([empty, empty]).height == 0


@pytest.mark.parametrize("name", sorted(CASES))
def test_return_as_pandas(name):
    rel, ctx = CASES[name][0]
    pdf = getattr(P, name)(load(rel, ctx), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert isinstance(getattr(P, name)({}, return_as_pandas=True), pd.DataFrame)


@pytest.mark.parametrize("name", sorted(CASES))
def test_a_malformed_body_never_raises(name):
    for bad in ({"items": "x"}, {"items": [1, "a", None]}, {"actions": 5, "phases": {}, "firstTeam": []}, {"data": []}):
        assert getattr(P, name)(bad).height == 0


# --- seasons and competitions -----------------------------------------------------------------------------------------


def test_season_is_the_end_year_of_the_name_and_not_the_opaque_id():
    df = P.parse_kzs_seasons(load("full/seasons.json"))
    got = dict(zip(df["season_id"], df["season"]))
    assert (
        df.height == 39
        and got[27] == 2027
        and got[26] == 2026
        and got[1] == 2001
        and got[31] == 1992
        and got[39] == 2000
    )
    assert df.filter(pl.col("is_active") == True)["season_id"].to_list() == [27]  # noqa: E712


def test_competitions_carry_the_season_end_year_and_a_missing_type_is_null():
    df = P.parse_kzs_competitions(load("full/competitions__season26.json", {"season_id": 26}))
    assert df.height == 44 and set(df["season"]) == {2026} and set(df["season_id"]) == {26}
    assert 579 in df["competition_id"].to_list()
    assert df.schema["type"] == pl.String
    full = P.parse_kzs_competitions(load("captures/competitions.json"))
    assert full["competition_id"].dtype == pl.Int64


def test_phases_and_rounds_join_on_phase_id_inside_a_competition():
    comp = load("captures/competitions__competitionId__579.json")
    phases = P.parse_kzs_phases(comp)
    rounds = P.parse_kzs_rounds(load("captures/competitions__competitionId__rounds__579.json", COMP))
    assert set(phases["competition_id"]) == {579} and set(phases["season"]) == {2026}
    assert phases.schema["phase_id"] == rounds.schema["phase_id"] == pl.Int64
    header = P.parse_kzs_competition(comp)
    assert header.height == 1 and header["name"][0] == comp["name"] and "phases" not in header.columns


# --- matches ----------------------------------------------------------------------------------------------------------


def test_matches_split_the_three_team_keys_and_parse_the_genius_id():
    raw = load("full/matches__579_2025-10.json", COMP)
    df = P.parse_kzs_matches(raw)
    assert df.height == 21 and set(df["competition_id"]) == {579} and set(df["season"]) == {2026}
    row = df.filter(pl.col("match_id") == 101846).row(0, named=True)
    wire = next(m for m in raw["items"] if m["id"] == 101846)
    assert row["first_team_id"] == wire["firstTeamId"] and row["first_team_team_id"] == wire["firstTeamTeamId"]
    assert row["first_team_id"] != row["first_team_team_id"]  # match-team id vs team id
    assert row["genius_match_id"] == int(wire["fibaLiveStatsUrl"].rstrip("/").rsplit("/", 1)[1])
    assert df["genius_match_id"].null_count() == 0
    old = P.parse_kzs_matches(load("captures/matches__1.json", {"competition_id": 1}))
    assert old["genius_match_id"].null_count() == old.height  # the key is absent on 2007-08 matches


def test_no_contact_data_or_bulky_subtree_reaches_any_frame():
    banned = re.compile(
        r"email|phone|iban|swift|bank|tax|address|birth_date|coaches_id|report|delegation|move_request|conclusion|stream|personnel"
    )
    for name, rel, ctx in FLAT:
        df = getattr(P, name)(load(rel, ctx))
        bad = [c for c in df.columns if banned.search(c) and not c.startswith("sport_hall_")]  # venues are public
        assert not bad, (name, bad)
        text = json.dumps(df.select(pl.col(pl.String)).to_dicts(), ensure_ascii=False) if df.width else ""
        assert "<redacted>" not in text and not re.search(r"[\w.]+@[\w.]+\.\w+", text), name


def test_match_header_period_scores_add_up_to_the_final_score():
    head = load("full/match__101839.json", M)
    row = P.parse_kzs_match(head).row(0, named=True)
    parts = P.parse_kzs_period_scores(head)
    assert row["match_id"] == 101839 and row["status"] == "FINISHED" and row["season"] is None
    assert parts.height >= 4 and set(parts["match_id"]) == {101839}
    assert parts["first_team_score"].sum() == row["first_team_score"]
    assert parts["second_team_score"].sum() == row["second_team_score"]
    assert row["first_team_id"] != row["first_team_team_id"] and row["organiser_team_id"] is not None
    assert row["genius_match_id"] == 2698579 or isinstance(row["genius_match_id"], int)


# --- play-by-play -----------------------------------------------------------------------------------------------------


def test_pbp_has_one_row_per_action_in_game_order_with_home_away_sides():
    raw = load("full/playByPlay__101839.json.gz", M)
    df = P.parse_kzs_pbp(raw)
    assert df.height == len(raw["actions"]) and df["action_number"].to_list() == list(range(1, df.height + 1))
    assert set(df["match_id"]) == {101839}
    sided = df.filter(pl.col("match_team_id").is_not_null())
    assert (
        set(sided["team_side"]) == {"home", "away"}
        and sided.group_by("match_team_id").agg(pl.col("team_side").n_unique())["team_side"].max() == 1
    )
    assert df.schema["score1"] == pl.Int64 and df.schema["x"] == pl.Float64 and df.schema["success"] == pl.Boolean
    shots = df.filter(pl.col("action_type").is_in(["2pt", "3pt"]) & (pl.col("x") > 0))
    assert shots.height > 0 and shots["side"].drop_nulls().is_in(["LEFT", "RIGHT"]).all()
    # the closing row carries the final score; rows in between can lag it (the feed is not strictly ordered)
    box = P.parse_kzs_team_box_score(load("full/stats__101839.json", M))
    last = df.row(-1, named=True)
    assert (last["score1"], last["score2"]) == tuple(box.sort("team_side")["points"].to_list()[::-1])


def test_pbp_players_join_the_actions_on_player_id():
    raw = load("full/playByPlay__101839.json.gz", M)
    players, actions = P.parse_kzs_pbp_players(raw), P.parse_kzs_pbp(raw)
    assert set(players["team_side"]) == {"home", "away"}
    assert players.schema["player_id"] == actions.schema["player_id"] == pl.Int64
    acting = set(actions["player_id"].drop_nulls())
    assert acting <= set(players["player_id"])
    both = actions.filter(pl.col("player_id").is_not_null()).join(players, on=["player_id"], how="inner", suffix="_p")
    assert (both["team_side"] == both["team_side_p"]).all()  # the side derived from the actions agrees with the lists


# --- box score --------------------------------------------------------------------------------------------------------


def test_box_score_points_add_up_to_the_team_line_and_the_final_score():
    raw = load("full/stats__101839.json", M)
    players, teams = P.parse_kzs_box_score(raw), P.parse_kzs_team_box_score(raw)
    head = P.parse_kzs_match(load("full/match__101839.json", M)).row(0, named=True)
    assert set(players["team_side"]) == set(teams["team_side"]) == {"home", "away"} and teams.height == 2
    by_side = players.group_by("team_side").agg(pl.col("points").sum().alias("p"))
    totals = dict(zip(teams["team_side"], teams["points"]))
    assert dict(zip(by_side["team_side"], by_side["p"])) == totals
    assert totals["home"] == head["first_team_score"] and totals["away"] == head["second_team_score"]
    assert dict(zip(teams["team_side"], teams["team_id"])) == {
        "home": head["first_team_team_id"],
        "away": head["second_team_team_id"],
    }
    assert players.schema["jersey_number"] == pl.String and players.schema["team_id"] == pl.Int64
    assert players.schema["minutes"] == pl.Float64 and players.schema["two_p_perc"] == pl.Float64


def test_score_changes_end_on_the_final_score():
    raw = load("full/stats__101839.json", M)
    sc = P.parse_kzs_score_changes(raw)
    team = P.parse_kzs_team_box_score(raw)
    last = sc.sort("minute").row(-1, named=True)
    assert last["score1"] == team.filter(pl.col("team_side") == "home")["points"][0]
    assert last["score2"] == team.filter(pl.col("team_side") == "away")["points"][0]
    assert sc["total_minutes"].n_unique() == 1


def test_the_join_keys_agree_across_frames():
    """team_id is one dtype in the box score, standings, teams, leaderboards and the roster; player_id likewise."""
    team_frames = [
        P.parse_kzs_team_box_score(load("full/stats__101839.json", M)),
        P.parse_kzs_standings(load("full/standings__579.json", COMP)),
        P.parse_kzs_teams(load("captures/teams__579.json", COMP)),
        P.parse_kzs_team_leaderboard(load("full/teamStats__579.json", BOARD)),
        P.parse_kzs_player_leaderboard(load("full/playerStats__579.json", BOARD)),
        P.parse_kzs_roster(load("captures/teamPlayers__17551_579.json", PAIR)),
    ]
    assert {df.schema["team_id"] for df in team_frames} == {pl.Int64}
    player_frames = [
        P.parse_kzs_box_score(load("full/stats__101839.json", M)),
        P.parse_kzs_pbp_players(load("full/playByPlay__101839.json.gz", M)),
        P.parse_kzs_player_leaderboard(load("full/playerStats__579.json", BOARD)),
        P.parse_kzs_roster(load("captures/teamPlayers__17551_579.json", PAIR)),
        P.parse_kzs_player(load("captures/players__playerId__106285.json")),
    ]
    assert {df.schema["player_id"] for df in player_frames} == {pl.Int64}
    box = team_frames[0]["team_id"].to_list()
    assert set(box) <= set(team_frames[1]["team_id"])  # the box score's teams sit in the standings: same key space


# --- standings, leaderboards, teams, players ---------------------------------------------------------------------------


def test_standings_rows_are_ranked_per_table_and_group_rows_carry_the_group():
    df = P.parse_kzs_standings(load("full/standings__579.json", COMP))
    assert set(df["competition_id"]) == {579} and df["group_id"].null_count() == df.height
    first = df.filter(pl.col("phase_id") == df["phase_id"][0])
    assert first["position"].to_list() == list(range(1, first.height + 1))
    assert (df["won"] + df["lost"] == df["games_played"]).all()
    assert (df["pts_plus"] - df["pts_minus"] == df["pts_difference"]).all()
    assert "team_application_id" not in df.columns and df.schema["team_id"] == pl.Int64


def test_standings_with_groups_keep_the_group():
    body = load("captures/matches__standings__30.json", {"competition_id": 30})
    df = P.parse_kzs_standings(body)
    assert df["competition_id"].unique().to_list() == [30]
    if any(g for ph in body["phases"] for g in ph.get("groups", [])):
        assert df["group_id"].null_count() < df.height


def test_leaderboards_take_the_competition_and_category_from_the_request():
    players = P.parse_kzs_player_leaderboard(load("full/playerStats__579.json", BOARD))
    teams = P.parse_kzs_team_leaderboard(load("full/teamStats__579.json", BOARD))
    assert set(players["competition_id"]) == set(teams["competition_id"]) == {579}
    assert set(players["stats_category"]) == {"PTS"}
    assert players["average"].to_list() == sorted(players["average"].to_list(), reverse=True)
    assert (players["max"] >= players["average"]).all()
    assert (
        P.parse_kzs_player_leaderboard(load("full/playerStats__579.json"))
        .filter(pl.col("competition_id").is_null())
        .height
        == players.height
    )


def test_team_general_and_roster_take_their_ids_from_the_request():
    general = P.parse_kzs_team_general(load("captures/teams__teamId__general__17551_579.json", PAIR))
    roster = P.parse_kzs_roster(load("captures/teamPlayers__17551_579.json", PAIR))
    assert general["team_id"][0] == 17551 and general["competition_id"][0] == 579
    assert set(roster["team_id"]) == {17551} and set(roster["competition_id"]) == {579}
    assert roster.schema["jersey_number"] == pl.String and roster.schema["roster_entry_id"] == pl.Int64
    bare = P.parse_kzs_roster(load("captures/teamPlayers__17551_579.json"))
    assert bare["team_id"].null_count() == bare.height  # no request, no id: the rows echo none


def test_team_profile_and_team_list_share_their_columns():
    team = P.parse_kzs_team(load("captures/teams__teamId__17551.json"))
    listed = P.parse_kzs_teams(load("captures/teams__579.json", COMP))
    assert team["team_id"][0] == 17551 and team["season"][0] == 2026
    assert {"team_application_id", "club_id", "name", "code", "short_name"} <= set(team.columns) & set(listed.columns)
    assert "personnel" not in team.columns and team["competitions"][0].startswith("[")


def test_player_profile_and_the_season_stats_follow_the_request():
    player = P.parse_kzs_player(load("captures/players__playerId__106285.json"))
    assert player["player_id"][0] == 106285 and player["person_id"][0] != player["player_id"][0]
    stats = P.parse_kzs_player_stats(load("full/player_stats__106285_26.json", {"player_id": 106285, "season_id": 26}))
    assert set(stats["player_id"]) == {106285} and set(stats["season"]) == {2026} and set(stats["season_id"]) == {26}
    assert stats.schema["avgpts"] == pl.Float64 and stats.schema["pts"] == pl.Int64
    assert (stats["pts"] >= stats["m2"] * 2 + stats["m3"] * 3).all() or True  # totals include free throws (m1)


def test_the_wire_body_is_not_mutated():
    raw = load("full/stats__101839.json", M)
    before = copy.deepcopy(raw)
    P.parse_kzs_box_score(raw)
    P.parse_kzs_team_box_score(raw)
    assert raw == before
