"""BB-D31: nba_game_splits on real ESPN captures of all four leagues.

Fixtures: tests/fixtures/espn/summary_*.json and tests/fixtures/espn/basketball_pbp/*.json.gz
(provenance in tests/fixtures/espn/basketball_pbp/README.md). The box score of each capture is
the oracle: counted possessions against the box estimate, the split families against the totals,
and the counted shots, free throws, turnovers, steals, assists and rebounds against the box.
"""

from __future__ import annotations

import copy
import gzip
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_page as gp
from sportsdataverse.nba import nba_game_splits as gs

ESPN = Path(__file__).resolve().parents[1] / "fixtures" / "espn"
BB = ESPN / "basketball_pbp"

#: Complete games: (league, fixture). Pros first.
GAMES = [
    ("nba", "summary_nba.json"),
    ("nba", "nba_401766458.json.gz"),
    ("nba", "nba_401704638.json.gz"),
    ("nba", "nba_400829116.json.gz"),
    ("wnba", "summary_wnba.json"),
    ("wnba", "wnba_400927398.json.gz"),
    ("mbb", "summary_mbb.json"),
    ("mbb", "mbb_401812161.json.gz"),
    ("mbb", "mbb_401856600.json.gz"),
    ("mbb", "mbb_401830342.json.gz"),
    ("wbb", "summary_wbb.json"),
    ("wbb", "wbb_401364640.json.gz"),
    ("wbb", "wbb_400509734.json.gz"),
]
#: Counted / box-estimate possessions. The box estimate (FGA - ORB + TOV + 0.44 FTA) runs high in
#: the pros (0.945-1.007 here, mean 0.977) and low in college, where 0.44 undercounts the
#: one-and-one trips and the box's rebounds include team rebounds (0.989-1.049 here).
#: Captures whose plays miss an event the box has, so the per-event counts cannot equal the box:
#: 401704638's plays carry one made free throw fewer than the box (README: they sum to 104-139
#: against an official 104-140), 401364640's one field goal (and its assist) fewer.
SHORT_PLAYS = {"nba_401704638.json.gz", "wbb_401364640.json.gz"}
RATIO = {"nba": (0.93, 1.02), "wnba": (0.93, 1.02), "mbb": (0.97, 1.06), "wbb": (0.97, 1.06)}


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def _raw(name: str) -> dict:
    if name.endswith(".gz"):
        with gzip.open(BB / name, "rt", encoding="utf-8") as f:
            return json.load(f)
    return json.loads((ESPN / name).read_text(encoding="utf-8"))


def _frames(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> dict:
    """``game_page_frames`` on the fixture, offline (the fixture is served as the HTTP body)."""
    raw = _raw(name)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    final = getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]), return_parsed=False)
    return gp.game_page_frames(final, league=lg)


def _box_tov(lg: str) -> str:
    return "total_turnovers" if lg in ("nba", "wnba") else "turnovers"


@pytest.mark.parametrize(("lg", "name"), GAMES)
def test_counted_possessions_track_the_box_estimate(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    f = _frames(monkeypatch, lg, name)
    splits = f["team_game_splits"].join(
        f["team_game_rates"].select(pl.col("team_id").cast(pl.Int32), pl.col("possessions").alias("box")), on="team_id"
    )
    assert splits.height == 2
    lo, hi = RATIO[lg]
    for counted, box in splits.select("possessions", "box").iter_rows():
        assert lo <= counted / box <= hi, (counted, box)
    # possessions alternate, so the two teams differ by at most one a period
    a, b = splits["possessions"].to_list()
    assert abs(a - b) <= f["periods"]


@pytest.mark.parametrize(("lg", "name"), GAMES)
def test_splits_add_up_and_match_the_box(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    f = _frames(monkeypatch, lg, name)
    box = f["team_box"].select(
        pl.col("team_id").cast(pl.Int32),
        "team_score",
        "field_goals_attempted",
        "field_goals_made",
        "three_point_field_goals_attempted",
        "free_throws_attempted",
        "free_throws_made",
        pl.col(_box_tov(lg)).alias("box_tov"),
        pl.col("steals").alias("opp_steals"),
        pl.col("team_id").cast(pl.Int32).alias("_me"),
        pl.col("opponent_team_id").cast(pl.Int32).alias("_opp"),
    )
    steals = box.select(pl.col("_opp").alias("team_id"), "opp_steals")
    rows = f["team_game_splits"].join(box.drop("opp_steals"), on="team_id").join(steals, on="team_id")
    assert rows.height == 2
    for r in rows.iter_rows(named=True):
        poss_pts = r["points"] - r["non_possession_points"]
        for a, b in (("transition", "half_court"), ("second_chance", "first_chance")):
            assert r[f"{a}_poss"] + r[f"{b}_poss"] == r["possessions"]
            assert r[f"{a}_pts"] + r[f"{b}_pts"] == poss_pts
        assert r["points"] == r["team_score"]
        assert r["rim_fga"] + r["mid_fga"] + r["fg2_unlocated_fga"] + r["fg3_fga"] == r["fga"]
        assert r["corner3_fga"] + r["above_break3_fga"] + r["fg3_unlocated_fga"] == r["fg3_fga"]
        assert r["live_tov"] + r["dead_tov"] == r["tov"] == r["box_tov"]
        assert r["live_tov"] == r["opp_steals"]  # a live turnover is the opponent's steal
        assert r["fg3_fga"] == r["three_point_field_goals_attempted"]
        assert r["fta"] == r["free_throws_attempted"]
        if name not in SHORT_PLAYS:
            assert r["ftm"] == r["free_throws_made"]
            assert r["fga"] == r["field_goals_attempted"] and r["fgm"] == r["field_goals_made"]
        assert 0 <= r["transition_poss"] <= r["possessions"]
        assert r["ppp"] == pytest.approx(r["points"] / r["possessions"])


@pytest.mark.parametrize(("lg", "name"), GAMES)
def test_player_splits_match_the_box(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    f = _frames(monkeypatch, lg, name)
    ps = f["player_game_splits"]
    box = f["player_box"].select(
        pl.col("athlete_id").cast(pl.Int32),
        *[
            pl.col(c).fill_null(0).alias(f"b_{c}")  # a did-not-play row has null counts
            for c in ("points", "field_goals_attempted", "assists", "turnovers", "rebounds")
        ],
    )
    j = ps.join(box, on="athlete_id", how="inner")
    assert j.height == f["player_box"].height == ps.height  # every counted player is a boxed one, once
    pairs = [("tov", "b_turnovers"), ("rebounds", "b_rebounds")]
    if name not in SHORT_PLAYS:
        pairs += [("points", "b_points"), ("fga", "b_field_goals_attempted"), ("assists", "b_assists")]
    for col, b in pairs:
        assert j.filter(pl.col(col) != pl.col(b)).height == 0, col
    # the minute share spreads each team's counted possessions over its five slots
    team = f["team_game_splits"].select("team_id", "possessions")
    per = ps.group_by("team_id").agg(pl.col("est_possessions").sum()).join(team, on="team_id")
    for est, poss in per.select("est_possessions", "possessions").iter_rows():
        assert est == pytest.approx(5 * poss)
    assert (ps["rim_assisted_fgm"] <= ps["rim_fgm"]).all() and (ps["fg3_assisted_fgm"] <= ps["fg3m"]).all()
    assert (ps["live_tov"] <= ps["tov"]).all()


def test_largest_lead_matches_the_box(monkeypatch: pytest.MonkeyPatch) -> None:
    checked = 0
    for lg, name in GAMES:
        f = _frames(monkeypatch, lg, name)
        if "largest_lead" not in f["team_box"].columns or name in SHORT_PLAYS:
            continue
        checked += 1
        box = dict(f["team_box"].select(pl.col("team_id").cast(pl.Int32), "largest_lead").iter_rows())
        for team, lead in f["team_game_splits"].select("team_id", "largest_lead").iter_rows():
            assert lead == int(box[team]), (name, team)
    assert checked >= 8


def test_a_live_game_counts_the_plays_so_far(monkeypatch: pytest.MonkeyPatch) -> None:
    f = _frames(monkeypatch, "mbb", "mbb_401920423_live.json.gz")
    ts = f["team_game_splits"]
    pbp = f["pbp"].with_columns(pl.col("home_score", "away_score").forward_fill())
    home, away = int(pbp["home_team_id"][0]), int(pbp["away_team_id"][0])
    score = {home: int(pbp["home_score"][-1]), away: int(pbp["away_score"][-1])}
    assert dict(ts.select("team_id", "points").iter_rows()) == score
    assert ts["possessions"].min() > 0
    assert f["player_game_splits"]["est_possessions"].drop_nulls().len() > 0


def test_before_tip_off_the_frames_are_empty_with_their_schema() -> None:
    empty = pl.DataFrame()
    assert gs.team_game_splits(empty).schema == pl.Schema(gs._TEAM_SPLITS_SCHEMA)
    assert gs.player_game_splits(empty).schema == pl.Schema(gs._PLAYER_SPLITS_SCHEMA)


def test_transition_needs_a_live_start_and_a_quick_first_shot(monkeypatch: pytest.MonkeyPatch) -> None:
    f = _frames(monkeypatch, "nba", "summary_nba.json")
    rows = gs._split_events(f["pbp"])
    poss = gs._possessions(rows, "nba", 4)
    trans = poss.filter(pl.col("transition"))
    assert trans.height > 0
    assert trans["start_type"].is_in(gs._LIVE_STARTS).all()
    assert ((trans["_start_clk"] - trans["_shot_clk"]) <= gs.TRANSITION_WINDOW_SECONDS["nba"]).all()
    assert poss.filter(pl.col("start_type") == "period_start")["transition"].not_().all()
    # 4 periods, so 4 period starts; the counted possessions partition every team row
    assert poss.filter(pl.col("start_type") == "period_start").height == 4
