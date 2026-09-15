"""Usage / situational box on the real NFL fixture (summary + core play items, offline)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

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


def test_no_participants_degrades_to_empty_tackles(game):
    proc, out, _ = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    box = create_usage_box(plays, None, league="nfl")
    assert box["tackles"] == [] and box["position_group_tackles"] == []
    assert box["player_usage"] and all(r.get("position_group") is None for r in box["player_usage"])
    assert create_usage_box(pl.DataFrame(), None, league="nfl") == {s: [] for s in SECTIONS}


def test_tackles_from_json_string_lists(game):
    """A committed final.json stringifies list cells; the tackle path decodes them."""
    import json as _json

    proc, out, parts = game
    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    stringified = parts.with_columns(
        [
            pl.col(c).map_elements(lambda v: _json.dumps(list(v)), return_dtype=pl.Utf8)
            for c in (
                "tackler_player_ids",
                "assisted_by_player_ids",
                "tackler_player_names",
                "assisted_by_player_names",
            )
            if c in parts.columns
        ]
    )
    a = create_usage_box(plays, parts, league="nfl")["tackles"]
    b = create_usage_box(plays, stringified, league="nfl")["tackles"]
    assert a and len(a) == len(b)
    assert sum(r["tackles"] for r in a) == sum(r["tackles"] for r in b)


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
