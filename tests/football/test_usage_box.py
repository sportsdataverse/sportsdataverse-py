"""Usage / situational box on the real NFL fixture (summary + core play items, offline)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.football.play_participants import (
    athlete_lookup_from_summary,
    play_participants_from_items,
)
from sportsdataverse.football.positions import position_abbr, position_group
from sportsdataverse.football.usage_box import (
    SECTIONS,
    aggregate_usage_box,
    create_usage_box,
    fit_third_down_curve,
    load_third_down_curve,
)
from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent.parent / "nfl" / "fixtures"
GAME_ID = 401872922


@pytest.fixture(scope="module")
def game():
    summary = json.loads((FIX / "summary_401872922.json").read_text())
    with gzip.open(FIX / "plays_401872922.json.gz", "rt", encoding="utf-8") as fh:
        items = json.load(fh)["items"]
    parts = play_participants_from_items(items, GAME_ID, athlete_lookup=athlete_lookup_from_summary(summary))
    proc = NFLPlayProcess(gameId=GAME_ID, participants=parts)
    proc.espn_nfl_pbp(summary=summary)
    out = proc.run_processing_pipeline()
    return proc, out, parts


def test_positions_map():
    assert (position_abbr(8), position_group(8)) == ("QB", "QB")
    assert position_group("264") == "DL" and position_group(29) == "DB"
    assert position_group(999) is None and position_abbr(None) is None


def test_participants_carry_position_ids(game):
    _, _, parts = game
    assert "tackler_position_id" in parts.columns and "rusher_position_id" in parts.columns
    groups = {position_group(p) for p in parts["tackler_position_id"].drop_nulls().to_list()}
    assert groups & {"LB", "DB", "DL"}


def test_bundled_third_down_curves():
    for league in ("cfb", "nfl"):
        c = load_third_down_curve(league).sort("distance")
        assert c["distance"].to_list() == list(range(1, 26))
        r = c["rate"].to_list()
        assert all(b <= a + 1e-9 for a, b in zip(r, r[1:]))  # non-increasing
        assert 0.5 < r[0] < 0.85 and r[9] < 0.4
    synth = pl.DataFrame(
        {
            "scrimmage_play": [True] * 6,
            "penalty_no_play": [False] * 6,
            "start.down": [3, 3, 3, 3, 2, 3],
            "start.distance": [1, 1, 10, 10, 3, 30],
            "first_down_created": [True, True, False, True, True, False],
            "touchdown": [False] * 6,
        }
    )
    c = fit_third_down_curve(synth)
    assert (
        c.height == 25 and c.filter(pl.col("distance") == 1)["rate"][0] >= c.filter(pl.col("distance") == 25)["rate"][0]
    )


def test_advbox_carries_the_usage_sections(game):
    _, out, _ = game
    box = out["advBoxScore"]
    for s in SECTIONS:
        assert s in box, s
    assert box["player_usage"] and box["tackles"] and box["team_usage"] and box["drive_scripting"]
    assert box["position_group_usage"] and box["position_group_tackles"]


def test_player_usage_shares_and_rates(game):
    _, out, _ = game
    pu = pl.from_dicts(out["advBoxScore"]["player_usage"], infer_schema_length=None)
    tu = pl.from_dicts(out["advBoxScore"]["team_usage"], infer_schema_length=None)
    assert tu.height == 2
    # shares sum to at most one per team (an unattributed target still counts
    # against the team); attributed targets never exceed the team's
    for team, g in pu.group_by("pos_team"):
        share = g["target_share"].fill_null(0).sum()
        assert 0.85 <= share <= 1.0 + 1e-9, (team, share)
        assert g["targets"].sum() <= tu.filter(pl.col("pos_team") == team[0])["targets"][0]
    assert (pu["fd_td_rate"].drop_nulls() <= 1.0).all() and (pu["fd_td_rate"].drop_nulls() >= 0.0).all()
    assert (pu["rz_touches"] <= pu["touches"]).all() and (pu["so_touches"] <= pu["touches"]).all()
    assert (pu["rz_touches"] <= pu["so_touches"]).all()  # the red zone sits inside the 40
    assert pu["position_group"].drop_nulls().n_unique() >= 3
    assert pu["third_down_over_expected"].drop_nulls().abs().max() < 10


def test_tackle_share_and_position_groups(game):
    _, out, _ = game
    t = pl.from_dicts(out["advBoxScore"]["tackles"], infer_schema_length=None)
    for team, g in t.group_by("def_pos_team"):
        assert abs(g["tackle_share"].sum() - 1.0) < 1e-9, team
    pg = pl.from_dicts(out["advBoxScore"]["position_group_tackles"], infer_schema_length=None)
    assert set(pg["position_group"].to_list()) & {"LB", "DB", "DL"}
    for team, g in pg.group_by("def_pos_team"):
        assert abs(g["tackle_share"].sum() - 1.0) < 1e-6, team


def test_team_usage_and_drive_scripting(game):
    _, out, _ = game
    tu = pl.from_dicts(out["advBoxScore"]["team_usage"], infer_schema_length=None)
    assert (tu["third_down_expected"] > 0).all() and (
        tu["third_down_opportunities"] >= tu["third_down_conversions"]
    ).all()
    assert (tu["rz_trips"] <= tu["so_trips"]).all()
    ds = pl.from_dicts(out["advBoxScore"]["drive_scripting"], infer_schema_length=None)
    assert set(ds["script"].to_list()) == {"scripted", "non_scripted"}
    scripted = ds.filter(pl.col("script") == "scripted")
    assert (scripted["drives"] <= 4).all() and (scripted["drives"] >= 2).all()  # two per half, at most
    assert ds["points_per_drive"].drop_nulls().min() >= 0


def test_aggregate_sums_counts_and_recomputes_rates(game):
    _, out, _ = game
    pu = pl.from_dicts(out["advBoxScore"]["player_usage"], infer_schema_length=None).with_columns(
        season=pl.lit(2026), game_id=pl.lit(GAME_ID)
    )
    lb = aggregate_usage_box("player_usage", [pu, pu])
    assert lb.height == pu.height and (lb["games"] == 2).all()
    top = pu.sort("targets", descending=True).row(0, named=True)
    row = lb.filter(pl.col("player_id") == top["player_id"]).row(0, named=True)
    assert row["targets"] == 2 * top["targets"] and abs(row["target_share"] - top["target_share"]) < 1e-9
    t = pl.from_dicts(out["advBoxScore"]["tackles"], infer_schema_length=None).with_columns(season=pl.lit(2026))
    lt = aggregate_usage_box("tackles", [t, t])
    for team, g in lt.group_by("def_pos_team"):
        assert abs(g["tackle_share"].sum() - 1.0) < 1e-9, team
    assert aggregate_usage_box("team_usage", []).height == 0


def test_no_curve_leaves_expected_null(game):
    _, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    box = create_usage_box(plays, parts, league="nfl", third_down_curve=pl.DataFrame({"distance": [], "rate": []}))
    for row in box["team_usage"]:
        assert row["third_down_expected"] is None and row["third_down_over_expected"] is None
    assert all(r["third_down_over_expected"] is None for r in box["player_usage"])
    assert all(position_group(i) is None for i in (0, 50, 70, 71, 72, 99, 218))


def test_no_participants_degrades_to_empty_tackles(game):
    proc, out, _ = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    box = create_usage_box(plays, None, league="nfl")
    assert box["tackles"] == [] and box["position_group_tackles"] == []
    assert box["player_usage"] and all(r.get("position_group") is None for r in box["player_usage"])
    assert create_usage_box(pl.DataFrame(), None, league="nfl") == {s: [] for s in SECTIONS}


_LIST_COLS = ("tackler_player_ids", "assisted_by_player_ids", "tackler_player_names", "assisted_by_player_names")


def _tackle_key(rows: list[dict]) -> dict:
    return {(r["def_pos_team"], r["player_id"]): (r["tackles"], r.get("assists"), r.get("player_name")) for r in rows}


@pytest.mark.parametrize(
    "encode",
    [
        pytest.param(lambda v: json.dumps(list(v)), id="json"),
        pytest.param(lambda v: repr(list(v)), id="python-repr"),
        # what cfbfastR-cfb-raw's stored play_participants actually hold: str(numpy array)
        pytest.param(lambda v: str(np.array(list(v), dtype=str)) if len(v) else "[]", id="numpy-repr"),
    ],
)
def test_tackles_from_stringified_lists(game, encode):
    """A committed final.json stringifies list cells; every stored shape decodes to the live rows."""
    _, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    cols = [c for c in _LIST_COLS if c in parts.columns]
    # the fixture must hold multi-player cells, or the numpy case could not fail
    assert parts.select(pl.col("assisted_by_player_ids").list.len().max()).item() >= 2
    stringified = parts.with_columns([pl.col(c).map_elements(encode, return_dtype=pl.Utf8) for c in cols])
    a = create_usage_box(plays, parts, league="nfl")["tackles"]
    b = create_usage_box(plays, stringified, league="nfl")["tackles"]
    assert a and _tackle_key(a) == _tackle_key(b)


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        ("['5152441' '5220449']", ["5152441", "5220449"]),
        ("['5152441', '5220449']", ["5152441", "5220449"]),
        ('["5152441","5220449"]', ["5152441", "5220449"]),
        ("[\"D'Andre Swift\" 'Jon Johnson']", ["D'Andre Swift", "Jon Johnson"]),
        ("['a' 'b'\n 'c']", ["a", "b", "c"]),
        ("[None, '7']", ["7"]),
        ("[4432712 5079588]", ["4432712", "5079588"]),
        ("[]", []),
        ("'5152441'", []),
        ("['unterminated", []),
        (None, None),
    ],
)
def test_decode_list_cell_shapes(cell, expected):
    from sportsdataverse.football.usage_box import _decode_list_cell

    assert _decode_list_cell(cell) == expected


def test_special_teams_sections(game):
    _, out, _ = game
    box = out["advBoxScore"]
    k = pl.from_dicts(box["st_kickers"], infer_schema_length=None)
    assert k.height >= 2 and k["kickoffs"].sum() == 10 and k["fg_attempts"].sum() == 3
    assert (k["kickoff_touchback_rate"].drop_nulls() <= 1).all() and (k["fg_pct"].drop_nulls() <= 1).all()
    assert (k["fg_0_39_attempts"] + k["fg_40_49_attempts"] + k["fg_50_plus_attempts"] == k["fg_attempts"]).all()
    p = pl.from_dicts(box["st_punters"], infer_schema_length=None)
    assert p["punts"].sum() == 5
    expected_net = (p["punt_yards"] - p["punt_return_yards_allowed"] - 20 * p["punt_touchbacks"]) / p["punts"]
    assert ((p["punt_net_avg"] - expected_net).abs() < 1e-9).all()
    r = pl.from_dicts(box["st_returners"], infer_schema_length=None)
    assert r.height >= 1 and (r["kick_return_avg"].drop_nulls() > 0).all()
    t = pl.from_dicts(box["st_team"], infer_schema_length=None)
    assert t.height == 2
    # coverage allowed on one side equals the returns made on the other
    assert t["kickoff_returns_allowed"].sum() == t["kick_returns"].sum()
    assert t["punt_returns_allowed"].sum() == t["punt_returns"].sum()
    assert t["kickoffs"].sum() == 10 and t["punts"].sum() == 5
    assert isinstance(box["st_blocks"], list)


def test_special_teams_leaderboard_uses_max_for_longs(game):
    _, out, _ = game
    k = pl.from_dicts(out["advBoxScore"]["st_kickers"], infer_schema_length=None).with_columns(season=pl.lit(2026))
    lb = aggregate_usage_box("st_kickers", [k, k])
    assert (lb["games"] == 2).all() and lb["kickoffs"].sum() == 20
    kicker = k.filter(pl.col("fg_attempts") > 0).row(0, named=True)
    row = lb.filter(pl.col("player_id") == kicker["player_id"]).row(0, named=True)
    assert row["fg_long"] == kicker["fg_long"] and row["fg_attempts"] == 2 * kicker["fg_attempts"]
    assert abs(row["fg_pct"] - kicker["fg_pct"]) < 1e-9
    p = pl.from_dicts(out["advBoxScore"]["st_punters"], infer_schema_length=None).with_columns(season=pl.lit(2026))
    plb = aggregate_usage_box("st_punters", [p, p])
    assert abs(plb["punt_net_avg"][0] - p["punt_net_avg"][0]) < 1e-9


def _st_plays(rows):
    base = {
        "kickoff_play": False,
        "kickoff_tb": False,
        "kickoff_onside": False,
        "kickoff_oob": False,
        "kickoff_fair_catch": False,
        "punt": False,
        "punt_tb": False,
        "punt_fair_catch": False,
        "punt_downed": False,
        "punt_oob": False,
        "punt_blocked": False,
        "fg_attempt": False,
        "fg_made": False,
        "xp_attempt": False,
        "xp_made": False,
        "touchdown": False,
        "penalty_no_play": False,
        "yds_kickoff": None,
        "yds_kickoff_return": None,
        "yds_punted": None,
        "yds_punt_return": None,
        "yds_fg": None,
        "EPA": 0.0,
        "start.yardsToEndzone": 65,
        "pos_team": 2,
        "def_pos_team": 1,
        "kicking_team": 1,
        "kickoff_player_id": None,
        "kickoff_player_name": None,
        "fg_kicker_player_id": None,
        "fg_kicker_player_name": None,
        "xp_kicker_player_id": None,
        "xp_kicker_player_name": None,
        "punter_player_id": None,
        "punter_player_name": None,
        "kickoff_return_player_id": None,
        "kickoff_return_player_name": None,
        "punt_return_player_id": None,
        "punt_return_player_name": None,
    }
    return pl.DataFrame([{**base, **r} for r in rows], infer_schema_length=None)


def test_kicker_keyed_by_id_across_kickoffs_field_goals_and_extra_points():
    """The kickoffs carry the kicker's id (ESPN's participants), the field goals
    only his name (the play text): keyed per source on coalesce(id, name) he came
    out twice -- "Eli Ozick" with id 5157006 and six kickoffs, and again with a
    null id and the field-goal line. One row, with the id, the kickoffs and the
    field goals together."""
    ko = {"kickoff_play": True, "kickoff_tb": True, "yds_kickoff": 65, "yds_kickoff_return": 25}
    ko |= {"kickoff_player_id": "5157006", "kickoff_player_name": "Eli Ozick"}
    fg = {"fg_attempt": True, "fg_made": True, "yds_fg": 42, "pos_team": 1, "def_pos_team": 2}
    fg |= {"fg_kicker_player_id": None, "fg_kicker_player_name": "Eli Ozick"}
    xp = {"xp_attempt": True, "xp_made": True, "pos_team": 1, "def_pos_team": 2}
    xp |= {"xp_kicker_player_id": "5157006", "xp_kicker_player_name": None}
    other = {"kickoff_play": True, "kickoff_tb": True, "yds_kickoff": 60, "yds_kickoff_return": 25}
    other |= {"kickoff_player_id": None, "kickoff_player_name": "Someone Else"}
    box = create_usage_box(_st_plays([ko] * 6 + [fg, xp] + [other]), None, league="cfb")
    rows = {r["player_name"]: r for r in box["st_kickers"]}
    assert set(rows) == {"Eli Ozick", "Someone Else"}
    ozick = rows["Eli Ozick"]
    assert ozick["player_id"] == "5157006"
    assert (ozick["kickoffs"], ozick["kickoff_yards"], ozick["fg_attempts"], ozick["fg_made"]) == (6, 390, 1, 1)
    assert (ozick["xp_attempts"], ozick["xp_made"], ozick["fg_long"]) == (1, 1, 42)
    assert rows["Someone Else"]["player_id"] is None and rows["Someone Else"]["kickoffs"] == 1


def test_punter_and_returner_rows_key_by_id_when_only_some_plays_carry_it():
    punt_a = {"punt": True, "yds_punted": 45, "punter_player_id": "77", "punter_player_name": "Punter One"}
    punt_b = {"punt": True, "yds_punted": 40, "punter_player_id": None, "punter_player_name": "Punter One"}
    punt_b |= {"punt_return_player_id": None, "punt_return_player_name": "Returner", "yds_punt_return": 12}
    kick = {"kickoff_play": True, "yds_kickoff": 60, "yds_kickoff_return": 20, "pos_team": 1, "def_pos_team": 2}
    kick |= {"kickoff_return_player_id": "99", "kickoff_return_player_name": "Returner"}
    box = create_usage_box(_st_plays([punt_a, punt_b, kick]), None, league="cfb")
    (punter,) = box["st_punters"]
    assert (punter["player_id"], punter["punts"], punter["punt_yards"]) == ("77", 2, 85)
    (returner,) = box["st_returners"]
    assert returner["player_id"] == "99"
    assert (returner["punt_returns"], returner["punt_return_yards"], returner["kick_returns"]) == (1, 12, 1)


def test_blockers_key_by_id_when_the_punt_block_has_it_and_the_fg_block_only_the_name():
    punt_block = {"punt": True, "yds_punted": 0, "punt_blocked": True, "pos_team": 1, "def_pos_team": 2}
    punt_block |= {"punt_block_player_id": "55", "punt_block_player_name": "Blocker"}
    fg_block = {"fg_attempt": True, "yds_fg": 40, "pos_team": 1, "def_pos_team": 2}
    fg_block |= {"fg_block_player_id": None, "fg_block_player_name": "Blocker"}
    plays = _st_plays([punt_block, fg_block]).with_columns(
        punt_team=pl.lit(1, dtype=pl.Int64), punt_return_team=pl.lit(2, dtype=pl.Int64)
    )
    (row,) = create_usage_box(plays, None, league="cfb")["st_blocks"]
    assert (row["def_pos_team"], row["player_id"], row["player_name"]) == (2, "55", "Blocker")
    assert (row["punt_blocks"], row["fg_blocks"], row["blocks"]) == (1, 1, 2)


def test_an_ambiguous_name_is_not_resolved_to_either_player():
    """Two same-name kickers on one team: a name-only row must stay its own row
    rather than merge into whichever id sorts first."""
    a = {"kickoff_play": True, "kickoff_tb": True, "yds_kickoff": 65, "yds_kickoff_return": 25}
    a |= {"kickoff_player_id": "1", "kickoff_player_name": "Same Name"}
    b = {**a, "kickoff_player_id": "2"}
    fg = {"fg_attempt": True, "fg_made": True, "yds_fg": 30, "pos_team": 1, "def_pos_team": 2}
    fg |= {"fg_kicker_player_id": None, "fg_kicker_player_name": "Same Name"}
    rows = create_usage_box(_st_plays([a, b, fg]), None, league="cfb")["st_kickers"]
    got = sorted(((r["player_id"] or ""), r["kickoffs"], r["fg_attempts"]) for r in rows)
    assert got == [("", 0, 1), ("1", 1, 0), ("2", 1, 0)]
    assert all(r["player_name"] == "Same Name" for r in rows)


def _roster_from(parts: pl.DataFrame) -> list[dict]:
    """(athlete, ESPN position href) records recovered from the participants' own position columns."""
    seen: dict[str, str] = {}
    for col in parts.columns:
        if col.endswith("_position_id") and col.replace("_position_id", "_player_id") in parts.columns:
            for pid, pos in parts.select(col.replace("_position_id", "_player_id"), col).drop_nulls().iter_rows():
                seen.setdefault(str(pid), str(pos))
    base = "http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions"
    return [{"athlete_id": int(pid), "position_href": f"{base}/{pos}?lang=en&region=us"} for pid, pos in seen.items()]


def _groups(box: dict, section: str, key: str) -> dict:
    return {(r[key], r["position_group"]): r for r in box[section]}


def test_roster_fills_positions_when_participants_carry_none(game):
    """Participants stored before *_position_id existed: the game roster restores the groups."""
    _, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    bare = parts.drop([c for c in parts.columns if c.endswith("_position_id")])
    without = create_usage_box(plays, bare, league="nfl")
    assert without["position_group_usage"] == [] and without["position_group_tackles"] == []

    full = create_usage_box(plays, parts, league="nfl")
    for roster in (_roster_from(parts), {"data": _roster_from(parts)}, pl.from_dicts(_roster_from(parts))):
        filled = create_usage_box(plays, bare, league="nfl", rosters=roster)
        assert filled["position_group_usage"] and filled["position_group_tackles"]
        assert _groups(filled, "position_group_usage", "pos_team") == _groups(full, "position_group_usage", "pos_team")
        assert _groups(filled, "position_group_tackles", "def_pos_team") == _groups(
            full, "position_group_tackles", "def_pos_team"
        )
        assert [r.get("position_group") for r in filled["player_usage"]] == [
            r.get("position_group") for r in full["player_usage"]
        ]


def test_participant_position_beats_the_roster(game):
    _, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    # every roster row claims a kicker (ESPN 22 -> ST); the participants' own ids must still win
    liar = [{**r, "position_href": r["position_href"].rsplit("/", 1)[0] + "/22?lang=en"} for r in _roster_from(parts)]
    assert position_group(22) == "ST"
    box = create_usage_box(plays, parts, league="nfl", rosters=liar)
    full = create_usage_box(plays, parts, league="nfl")
    assert box["position_group_usage"] == full["position_group_usage"]


def test_malformed_roster_never_costs_the_box(game):
    _, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    bare = parts.drop([c for c in parts.columns if c.endswith("_position_id")])
    junk = [
        None,
        "not a record",
        {"position_href": ".../positions/8"},  # no athlete id
        {"athlete_id": 1, "position_href": None, "age": "unknown"},
        {"athlete_id": 2.0, "position_id": 8, "age": 31},
        {"athlete_id": 3, "position_href": ".../positions/0?lang=en"},  # 0 = no position
    ]
    box = create_usage_box(plays, bare, league="nfl", rosters=junk)
    assert box["player_usage"] and box["position_group_usage"] == []
    from sportsdataverse.football.usage_box import _roster_positions

    assert _roster_positions(junk) == {"2": "QB"}
