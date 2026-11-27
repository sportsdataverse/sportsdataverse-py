"""Game story on real hockey, baseball and soccer games: the pre-registered gates and the shootout / extra-inning rules.

Fixtures: ``tests/fixtures/game_story/`` (provenance in that directory's README): released NHL, PWHL and MLB rows of
a few real games, and raw ESPN Premier League summaries.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_story import GAME_STORY_SCHEMAS
from sportsdataverse.mlb.mlb_game_story import mlb_game_story
from sportsdataverse.nhl.nhl_game_story import nhl_game_story
from sportsdataverse.pwhl.pwhl_game_story import pwhl_game_story
from sportsdataverse.soccer.soccer_game_story import soccer_game_story

FB = Path(__file__).resolve().parents[1] / "fixtures" / "game_story"


def _read(name: str) -> pl.DataFrame:
    return pl.read_parquet(FB / name)


def _assert_schemas(story: dict) -> None:
    assert set(story) == set(GAME_STORY_SCHEMAS)
    for k, v in story.items():
        assert v.schema == pl.Schema(GAME_STORY_SCHEMAS[k]), k


def _gates(story: dict, *, exact: bool) -> None:
    """G1 on decided games, G3 on every game, and (``exact``) the accounting adding up to the margin."""
    s = story["summary"]
    for row in s.iter_rows(named=True):
        gid = row["game_id"]
        e = story["edges"].filter(pl.col("game_id") == gid)
        if row["decided"]:
            assert e.filter(pl.col("rank") == 1)["points_worth"][0] > 0, gid  # G1
        acct = e.filter(pl.col("kind") == "accounting")["points_worth"].sum()
        if exact:
            assert acct == pytest.approx(row["margin"], abs=1e-9), gid
        per = story["periods"].filter(pl.col("game_id") == gid)
        assert row["periods_agree"], gid  # G3
        assert (per["home_pts"].sum(), per["away_pts"].sum()) == (
            row["home_score"],
            row["away_score"],
        ), gid


def test_nhl_gates_shootout_and_overtime() -> None:
    pbp, sched, sk = (
        _read("nhl_pbp_games.parquet"),
        _read("nhl_schedule_games.parquet"),
        _read("nhl_skater_box_games.parquet"),
    )
    story = nhl_game_story(pbp, sched, sk)
    _assert_schemas(story)
    _gates(story, exact=True)
    s = story["summary"]
    so = s.filter(pl.col("game_id") == 2024020022).row(0, named=True)  # PHI at SEA, won in a shootout
    per = story["periods"].filter(pl.col("game_id") == 2024020022)
    assert per["period_label"].to_list() == ["P1", "P2", "P3", "OT", "SO"]
    assert per.filter(pl.col("period_label") == "SO").select(pl.col("home_pts") + pl.col("away_pts")).item() == 1
    e = story["edges"].filter((pl.col("game_id") == 2024020022) & (pl.col("edge") == "shootout")).row(0, named=True)
    assert (e["winner_value"], e["loser_value"]) == (1.0, 0.0) and so["overtime"]
    # the shootout settles it: its row carries the swing from the end of overtime to the result
    m = story["moment"].filter(pl.col("game_id") == 2024020022).row(0, named=True)
    assert m["text"] == "Shootout" and m["winner_wpa"] > 0
    assert story["performers"].height == 2 * s.height


def test_nhl_moment_is_the_largest_swing() -> None:
    """G2 against the WP model run here, outside the story."""
    from sportsdataverse.nhl.nhl_market import (
        nhl_in_game_win_prob,
        win_prob_from_margin,
    )
    from sportsdataverse.nhl.nhl_prediction_constants import get_constants

    pbp, sched, sk = (
        _read("nhl_pbp_games.parquet"),
        _read("nhl_schedule_games.parquet"),
        _read("nhl_skater_box_games.parquet"),
    )
    story = nhl_game_story(pbp, sched, sk)
    prior = win_prob_from_margin(get_constants("nhl").hfa)
    for gid in story["summary"].filter(pl.col("overtime") == False)["game_id"].to_list():  # no shootout row
        one = pbp.filter(pl.col("game_id") == gid).sort("event_idx")
        wp = nhl_in_game_win_prob(one, prior)["home_win_prob"]
        wpa = (wp - wp.shift(1).fill_null(prior)).abs()
        best = one["event_idx"][int(wpa.arg_max())]
        assert story["moment"].filter(pl.col("game_id") == gid)["order"][0] == best


def test_pwhl_gates_and_deciding_score() -> None:
    story = pwhl_game_story(
        _read("pwhl_pbp_games.parquet"),
        _read("pwhl_schedule_games.parquet"),
        _read("pwhl_skater_box_games.parquet"),
        _read("pwhl_xg_games.parquet"),
    )
    _assert_schemas(story)
    _gates(story, exact=True)
    assert set(story["moment"]["rule"].to_list()) == {"deciding_score"}
    so = story["summary"].filter(pl.col("overtime") & (pl.col("n_periods") > 4))
    assert so.height >= 1
    gid = so["game_id"][0]
    assert story["periods"].filter(pl.col("game_id") == gid)["period_label"].to_list()[-1] == "SO"
    assert story["moment"].filter(pl.col("game_id") == gid)["text"][0] == "Shootout"
    assert {"xg", "goaltending"} <= set(story["edges"]["edge"].to_list())


def test_pwhl_without_xg_keeps_the_goal_accounting() -> None:
    story = pwhl_game_story(
        _read("pwhl_pbp_games.parquet"),
        _read("pwhl_schedule_games.parquet"),
        _read("pwhl_skater_box_games.parquet"),
    )
    assert set(story["edges"]["kind"].to_list()) == {"accounting"}
    _gates(story, exact=True)


def test_mlb_gates_extra_innings_and_wpa_alignment() -> None:
    pbp, sched, wpa, re24 = (_read(f"mlb_{k}_games.parquet") for k in ("pbp", "schedule", "wpa", "re24"))
    story = mlb_game_story(pbp, sched, wpa, re24)
    _assert_schemas(story)
    _gates(story, exact=False)
    extra = story["summary"].filter(pl.col("overtime"))
    assert extra.height >= 1
    per = story["periods"].filter(pl.col("game_id") == extra["game_id"][0])
    assert per["period_label"].to_list()[9] == "10" and per["is_overtime"].to_list()[9]
    # the release keys a PA's WPA to the next index: the moment's wpa is the next row's
    for m in story["moment"].iter_rows(named=True):
        nxt = wpa.filter(
            (pl.col("game_id").cast(pl.Int64) == m["game_id"]) & (pl.col("at_bat_index") == m["order"] + 1)
        )
        assert nxt["wpa"][0] == pytest.approx(m["wpa"])
        want = wpa.filter(pl.col("game_id").cast(pl.Int64) == m["game_id"]).filter(pl.col("at_bat_index") > 0)
        assert abs(m["wpa"]) == pytest.approx(want["wpa"].abs().max())  # G2
    # each side's starter-plus-bullpen runs saved: within a run or so of the margin
    acct = story["edges"].filter(pl.col("kind") == "accounting").group_by("game_id").agg(pl.col("points_worth").sum())
    gap = acct.join(story["summary"].select("game_id", "margin"), on="game_id")
    assert (gap["points_worth"] - gap["margin"]).abs().max() < 1.5


def _summaries() -> list[dict]:
    out = []
    for f in sorted(FB.glob("eng1_*.json.gz")):
        with gzip.open(f, "rt", encoding="utf-8") as fh:
            out.append(json.load(fh))
    return out


def test_soccer_gates_draw_and_own_goal() -> None:
    story = soccer_game_story(_summaries())
    _assert_schemas(story)
    _gates(story, exact=True)
    s = story["summary"]
    assert (s["decided"] == False).sum() >= 1  # a draw: no winner, no moment
    draws = s.filter(pl.col("decided") == False)["game_id"].to_list()
    assert story["moment"].filter(pl.col("game_id").is_in(draws)).height == 0
    assert "own_goals_for" in story["edges"].filter(pl.col("winner_value") > 0)["edge"].to_list()
    assert set(story["moment"]["rule"].to_list()) == {"deciding_score"}
    # the deciding goal: the winner is ahead after it and never level or behind again
    for m in story["moment"].iter_rows(named=True):
        row = s.filter(pl.col("game_id") == m["game_id"]).row(0, named=True)
        sign = 1 if row["winner_team_id"] == row["home_team_id"] else -1
        assert sign * (m["home_score"] - m["away_score"]) > 0


def test_nhl_extra_attacker_goal_is_even_strength() -> None:
    """2024020009's home goal at 6v5 (goalie pulled, opponent not short) is even strength, not a power play."""
    pbp, sched, sk = (
        _read("nhl_pbp_games.parquet"),
        _read("nhl_schedule_games.parquet"),
        _read("nhl_skater_box_games.parquet"),
    )

    def one(d: pl.DataFrame) -> pl.DataFrame:
        return d.filter(pl.col("game_id").cast(pl.Int64) == 2024020009)

    story = nhl_game_story(one(pbp), one(sched), one(sk))
    s = story["summary"].row(0, named=True)
    goals = one(pbp).filter(pl.col("event_type") == "GOAL")
    home_goals = goals.filter(pl.col("event_team_type") == "home")
    pp_home = home_goals.filter(
        (pl.col("home_skaters").clip(upper_bound=5) > pl.col("away_skaters")) & (pl.col("empty_net") == False)
    )
    e = story["edges"].filter(pl.col("edge") == "power_play_goals").row(0, named=True)
    home_pp = e["winner_value"] if s["winner_team_id"] == s["home_team_id"] else e["loser_value"]
    assert home_pp == pp_home.height
    assert home_goals.filter(pl.col("strength_state") == "6v5").height == 1


def test_mlb_matrix_of_another_season_raises() -> None:
    pbp, sched, wpa, re24 = (_read(f"mlb_{k}_games.parquet") for k in ("pbp", "schedule", "wpa", "re24"))
    with pytest.raises(ValueError, match="no run expectancy"):
        mlb_game_story(pbp, sched, wpa, re24.with_columns(pl.col("season") + 1))


def test_mlb_partial_matrix_raises() -> None:
    pbp, sched, wpa, re24 = (_read(f"mlb_{k}_games.parquet") for k in ("pbp", "schedule", "wpa", "re24"))
    one = sched["game_pk"][0]
    sched = sched.with_columns(pl.when(pl.col("game_pk") == one).then(2099).otherwise(pl.col("season")).alias("season"))
    with pytest.raises(ValueError, match=r"season\(s\) \[2099\]"):
        mlb_game_story(pbp, sched, wpa, re24)


def test_mlb_automatic_runner_who_scores_starts_on_second() -> None:
    import importlib

    mod = importlib.import_module("sportsdataverse.mlb.mlb_game_story")
    pbp, sched, re24 = (
        _read("mlb_pbp_716356.parquet"),
        _read("mlb_schedule_716356.parquet"),
        _read("mlb_re24_2023.parquet"),
    )
    games = sched.select(
        pl.col("game_pk").cast(pl.Int64).alias("game_id"),
        pl.col("season").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
    )
    st = mod._states(pbp, re24, games)
    opens = st.filter(
        (pl.col("inning") > 9) & (pl.col("at_bat_index") == pl.col("at_bat_index").min().over("inning", "half"))
    )
    # every extra half opens with him on 2nd; two show him only through the run he scores
    assert opens["base_state"].to_list() == ["_2_"] * 4
    assert opens.filter(pl.col("runs_on_play") > 0).height == 2
    reg_opens = st.filter(
        (pl.col("inning") <= 9) & (pl.col("at_bat_index") == pl.col("at_bat_index").min().over("inning", "half"))
    )
    assert reg_opens["base_state"].unique().to_list() == ["___"]
    # a World Series game has no automatic runner: its extra halves open empty
    pbp, sched = _read("mlb_pbp_748542.parquet"), _read("mlb_schedule_748542.parquet")
    ws_games = sched.select(
        pl.col("game_pk").cast(pl.Int64).alias("game_id"),
        pl.col("season").cast(pl.Int64),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
    )
    ws = mod._states(pbp, re24, ws_games)
    ws_opens = ws.filter(
        (pl.col("inning") > 9) & (pl.col("at_bat_index") == pl.col("at_bat_index").min().over("inning", "half"))
    )
    assert ws_opens.height == 4
    assert ws_opens["base_state"].unique().to_list() == ["___"]


def test_soccer_summary_passed_twice_counts_once() -> None:
    once, twice = soccer_game_story(_summaries()), soccer_game_story(_summaries() * 2)
    for k in ("summary", "edges", "periods", "performers"):
        assert twice[k].equals(once[k]), k


def test_soccer_without_key_events_has_no_accounting() -> None:
    raw = _summaries()
    scored = next(
        p for p in raw if any("oal" in str(e.get("type", {}).get("text", "")) for e in p.get("keyEvents", []))
    )
    gid = int(scored["header"]["id"])
    stripped = {**scored, "keyEvents": []}
    story = soccer_game_story([stripped])
    edges = story["edges"].filter(pl.col("game_id") == gid)
    assert edges.filter(pl.col("kind") == "accounting").height == 0


def test_soccer_two_finals_for_one_game_raise() -> None:
    raw = _summaries()
    stale = {**raw[0], "header": {**raw[0]["header"]}}
    comp = stale["header"]["competitions"][0]
    stale["header"]["competitions"] = [
        {**comp, "competitors": [{**c, "score": str(int(c.get("score") or 0) + 1)} for c in comp["competitors"]]}
    ]
    with pytest.raises(ValueError, match="different finals"):
        soccer_game_story([raw[0], stale])


def test_mlb_matrix_missing_a_state_raises() -> None:
    pbp, sched, wpa, re24 = (_read(f"mlb_{k}_games.parquet") for k in ("pbp", "schedule", "wpa", "re24"))
    gap = re24.filter(((pl.col("base_state") == "1__") & (pl.col("outs") == 0)) == False)
    with pytest.raises(ValueError, match="some PAs' base-out states"):
        mlb_game_story(pbp, sched, wpa, gap)
