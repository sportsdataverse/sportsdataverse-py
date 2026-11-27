"""Game story on real games: the three pre-registered gates (G1 edge direction, G2 moment, G3 periods) and the schemas.

Fixtures: ``tests/fixtures/espn/basketball_pbp/*.json.gz`` (real ESPN summaries, served offline through
``espn_<lg>_pbp``) and ``tests/fixtures/game_story/espn_{cfb,nfl}_pbp_games.parquet`` (real released plays;
provenance in that directory's README).
"""

from __future__ import annotations

import copy
import gzip
import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.game_story import (
    EDGE_ROWS_SCHEMA,
    GAME_STORY_SCHEMAS,
    GAMES_SCHEMA,
    PLAY_ROWS_SCHEMA,
    PLAYER_ROWS_SCHEMA,
    game_story,
)
from sportsdataverse.cfb.cfb_game_story import cfb_game_story
from sportsdataverse.nba.nba_game_page import game_page_frames
from sportsdataverse.nba.nba_game_story import nba_game_story
from sportsdataverse.nfl.nfl_game_story import nfl_game_story

FIX = Path(__file__).resolve().parents[1] / "fixtures"
BB = FIX / "espn" / "basketball_pbp"
FB = FIX / "game_story"


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._p = payload

    def json(self) -> dict:
        return self._p


def _frames(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> dict:
    """``game_page_frames`` on a real ESPN summary, offline (the fixture is served as the HTTP body)."""
    with gzip.open(BB / name, "rt", encoding="utf-8") as f:
        raw = json.load(f)
    mod = importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp")
    monkeypatch.setattr(mod, "download", lambda url, **kwargs: _Resp(copy.deepcopy(raw)))
    final = getattr(mod, f"espn_{lg}_pbp")(game_id=int(raw["header"]["id"]), return_parsed=False)
    return game_page_frames(final, league=lg)


def _story(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> tuple[dict, dict]:
    fr = _frames(monkeypatch, lg, name)
    return fr, nba_game_story(fr["pbp"], fr["team_box"], fr["player_box"], league=lg)


def _assert_schemas(story: dict) -> None:
    assert set(story) == set(GAME_STORY_SCHEMAS)
    for k, v in story.items():
        assert v.schema == pl.Schema(GAME_STORY_SCHEMAS[k]), k


def _true_moment(plays: pl.DataFrame) -> int:
    """G2's reference: argmax |wpa| over the input plays, earliest on ties."""
    p = plays.filter(pl.col("wpa").is_not_null() & pl.col("wpa").is_not_nan())
    return p.sort(pl.col("wpa").abs(), "order", descending=[True, False])["play_id"][0]


BASKETBALL = [
    # (league, fixture, regulation, overtime): an NBA final, a WNBA halves-era OT game, a 2OT college game,
    # a women's college halves-era OT game, the 2026 men's title game
    ("nba", "nba_401910002.json.gz", 4, False),
    ("wnba", "wnba_250603003.json.gz", 2, True),
    ("mbb", "mbb_401830342.json.gz", 2, True),
    ("wbb", "wbb_400509734.json.gz", 2, True),
    ("mbb", "mbb_401856600.json.gz", 2, False),
]


@pytest.mark.parametrize(("lg", "name", "reg", "ot"), BASKETBALL)
def test_basketball_gates_on_real_games(
    monkeypatch: pytest.MonkeyPatch, lg: str, name: str, reg: int, ot: bool
) -> None:
    fr, story = _story(monkeypatch, lg, name)
    _assert_schemas(story)
    s = story["summary"].row(0, named=True)
    tb = fr["team_box"]
    home = tb.filter(pl.col("team_home_away") == "home").row(0, named=True)
    away = tb.filter(pl.col("team_home_away") == "away").row(0, named=True)
    assert (s["home_score"], s["away_score"]) == (int(home["team_score"]), int(away["team_score"]))
    assert s["decided"] and s["overtime"] is ot
    edges = story["edges"]
    # the four accounting parts add up to the margin, exactly
    acct = edges.filter(pl.col("kind") == "accounting")
    assert acct["edge"].sort().to_list() == ["free_throws", "rebounding", "shooting", "turnovers"]
    assert acct["points_worth"].sum() == pytest.approx(s["margin"], abs=1e-9)
    # G1: the rank-1 edge points the winner's way
    assert edges.filter(pl.col("rank") == 1)["points_worth"][0] > 0
    assert edges["rank"].to_list() == list(range(1, edges.height + 1))
    assert edges["points_worth"].is_sorted(descending=True)
    # G2: the moment is the max-|wp_delta| play of the scored pbp
    pbp = fr["pbp"].select(
        pl.col("id").cast(pl.Int64).alias("play_id"),
        pl.col("game_play_number").alias("order"),
        pl.col("wp_delta").alias("wpa"),
    )
    assert story["moment"]["play_id"][0] == _true_moment(pbp)
    assert story["moment"]["rule"][0] == "max_abs_wpa"
    # G3: the periods sum to the official final
    per = story["periods"]
    assert s["periods_agree"]
    assert (per["home_pts"].sum(), per["away_pts"].sum()) == (s["home_score"], s["away_score"])
    labels = per["period_label"].to_list()
    assert labels[:reg] == [f"{'H' if reg == 2 else 'Q'}{i}" for i in range(1, reg + 1)]
    assert (labels[reg:] != []) is ot and all(lab.startswith("OT") for lab in labels[reg:])
    # each side's top scorer
    perf = story["performers"]
    assert perf.height == 2
    pb = fr["player_box"]
    for row in perf.iter_rows(named=True):
        assert row["value"] == pb.filter(pl.col("team_id") == row["team_id"])["points"].max()


@pytest.mark.parametrize(
    ("lg", "name"),
    [("nba", "nba_401766458.json.gz"), ("nba", "nba_400829116.json.gz"), ("wbb", "wbb_400550784.json.gz")],
)
def test_basketball_periods_on_games_espn_scored_wrong(monkeypatch: pytest.MonkeyPatch, lg: str, name: str) -> None:
    """ESPN's own running score misses these finals (fixture README); the pbp pipeline rebuilds it, so G3 holds."""
    fr, story = _story(monkeypatch, lg, name)
    assert fr["pbp"]["score_source"].unique().to_list() == ["rebuilt"]
    s = story["summary"].row(0, named=True)
    per = story["periods"]
    assert s["periods_agree"]
    assert (per["home_pts"].sum(), per["away_pts"].sum()) == (s["home_score"], s["away_score"])
    acct = story["edges"].filter(pl.col("kind") == "accounting")
    assert acct["points_worth"].sum() == pytest.approx(s["margin"], abs=1e-9)


def test_basketball_league_check() -> None:
    with pytest.raises(ValueError, match="league must be one of"):
        nba_game_story(pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), league="nfl")


@pytest.mark.parametrize(("fn", "lg"), [(cfb_game_story, "cfb"), (nfl_game_story, "nfl")])
def test_football_gates_on_real_games(fn, lg: str) -> None:
    pbp = pl.read_parquet(FB / f"espn_{lg}_pbp_games.parquet")
    story = fn(pbp)
    _assert_schemas(story)
    summ = story["summary"]
    assert summ.height == pbp["game_id"].n_unique()
    home = pl.col("pos_team_id") == pl.col("homeTeamId")
    wpa = pbp.select(
        "game_id",
        pl.col("id").alias("play_id"),
        pl.col("game_play_number").alias("order"),
        pl.when(home).then(pl.col("wpa")).otherwise(-pl.col("wpa")).alias("wpa"),
    )
    for s in summ.iter_rows(named=True):
        gid = s["game_id"]
        e = story["edges"].filter(pl.col("game_id") == gid)
        assert e.height == 9
        assert e["edge"].n_unique() == 9
        if s["decided"]:
            assert e.filter(pl.col("rank") == 1)["points_worth"][0] > 0  # G1
        assert story["moment"].filter(pl.col("game_id") == gid)["play_id"][0] == _true_moment(  # G2
            wpa.filter(pl.col("game_id") == gid)
        )
        per = story["periods"].filter(pl.col("game_id") == gid)
        last = pbp.filter(pl.col("game_id") == gid).sort("game_play_number").tail(1).row(0, named=True)
        reaches = (last["homeScore"], last["awayScore"]) == (s["home_score"], s["away_score"])
        assert s["periods_agree"] is reaches  # G3
        assert (per["home_pts"].sum(), per["away_pts"].sum()) == (last["homeScore"], last["awayScore"])
        assert s["overtime"] is (per.height > 4)


def test_football_accounting_is_the_net_epa() -> None:
    """The four phases partition every play with an EPA: their sum is each offense's total EPA."""
    pbp = pl.read_parquet(FB / "espn_nfl_pbp_games.parquet")
    story = nfl_game_story(pbp)
    acct = story["edges"].filter(pl.col("kind") == "accounting").group_by("game_id").agg(pl.col("winner_value").sum())
    total = pbp.filter(pl.col("EPA").is_not_null())
    for row in acct.iter_rows(named=True):
        s = story["summary"].filter(pl.col("game_id") == row["game_id"]).row(0, named=True)
        side = s["winner_team_id"] if s["decided"] else s["home_team_id"]
        want = total.filter((pl.col("game_id") == row["game_id"]) & (pl.col("pos_team_id") == side))["EPA"].sum()
        assert row["winner_value"] == pytest.approx(want)


def test_football_tie_and_source_defects() -> None:
    nfl = nfl_game_story(pl.read_parquet(FB / "espn_nfl_pbp_games.parquet"))
    tie = nfl["summary"].filter(pl.col("game_id") == 401772921).row(0, named=True)  # GB 40, DAL 40 (2025, OT)
    assert not tie["decided"] and tie["margin"] == 0 and tie["winner_team_id"] is None and tie["overtime"]
    te = nfl["edges"].filter(pl.col("game_id") == 401772921)
    assert te["winner_team_id"].is_null().all() and te.height == 9
    assert nfl["moment"].filter(pl.col("game_id") == 401772921)["winner_wpa"].is_null().all()
    # college 401628337: the plays end 20-21, the official final is 14-21; reported, not patched
    cfb = cfb_game_story(pl.read_parquet(FB / "espn_cfb_pbp_games.parquet"))
    bad = cfb["summary"].filter(pl.col("game_id") == 401628337).row(0, named=True)
    assert (bad["home_score"], bad["away_score"]) == (14, 21) and not bad["periods_agree"]


def test_core_empty_and_missing_columns() -> None:
    def empty(schema: dict) -> pl.DataFrame:
        return pl.DataFrame(schema=schema)

    out = game_story(empty(GAMES_SCHEMA), empty(EDGE_ROWS_SCHEMA), empty(PLAY_ROWS_SCHEMA), empty(PLAYER_ROWS_SCHEMA))
    _assert_schemas(out)
    assert all(v.height == 0 for v in out.values())
    with pytest.raises(KeyError, match="games missing columns"):
        game_story(
            pl.DataFrame({"game_id": [1]}), empty(EDGE_ROWS_SCHEMA), empty(PLAY_ROWS_SCHEMA), empty(PLAYER_ROWS_SCHEMA)
        )


def test_core_runs_and_deciding_score_on_real_plays() -> None:
    """Without wpa the moment falls back to the winner's go-ahead-for-good score; runs are unanswered points."""
    pbp = pl.read_parquet(FB / "espn_cfb_pbp_games.parquet").filter(pl.col("game_id") == 401628333)  # OT, 34-27
    story = cfb_game_story(pbp.with_columns(pl.lit(None, dtype=pl.Float64).alias("wpa")))
    m = story["moment"].row(0, named=True)
    assert m["rule"] == "deciding_score"
    s = story["summary"].row(0, named=True)
    home_won = s["winner_team_id"] == s["home_team_id"]
    lead = (m["home_score"] - m["away_score"]) * (1 if home_won else -1)
    assert lead > 0
    # it is the last time the winner went ahead: never level or behind afterwards
    after = pbp.filter(pl.col("game_play_number") > m["order"])
    diff = (after["homeScore"] - after["awayScore"]) * (1 if home_won else -1)
    assert (diff > 0).all()
    runs = story["runs"]
    assert runs.height == 2 and (runs["points"] > 0).all()
    assert set(runs["team_id"].to_list()) == {s["home_team_id"], s["away_team_id"]}


def test_edge_only_one_side_has_ranks_last(monkeypatch: pytest.MonkeyPatch) -> None:
    """A box count missing for the winner (a college box without fast breaks) keeps the edge, unpriced, ranked last."""
    fr = _frames(monkeypatch, "nba", "nba_401910002.json.gz")
    tb = fr["team_box"]
    winner = tb.sort("team_score", descending=True)["team_id"][0]
    tb = tb.with_columns(
        pl.when(pl.col("team_id") == winner)
        .then(None)
        .otherwise(pl.col("fast_break_points"))
        .alias("fast_break_points")
    )
    edges = nba_game_story(fr["pbp"], tb, fr["player_box"])["edges"]
    fb = edges.filter(pl.col("edge") == "fast_break_points").row(0, named=True)
    assert fb["kind"] == "split" and fb["unit"] == "points"
    assert fb["winner_value"] is None and fb["loser_value"] is not None and fb["points_worth"] is None
    assert fb["rank"] == edges.height


def test_basketball_box_with_a_null_total_turnovers() -> None:
    """Released NBA 401810169 leaves one side's total_turnovers null: the row falls back to its turnovers, so the
    four parts still add up to the margin (found by the held-out run)."""
    tb = pl.read_parquet(FB / "nba_401810169_team_box.parquet")
    assert tb["total_turnovers"].null_count() == 1
    story = nba_game_story(
        pl.read_parquet(FB / "nba_401810169_pbp.parquet"), tb, pl.read_parquet(FB / "nba_401810169_player_box.parquet")
    )
    s = story["summary"].row(0, named=True)
    acct = story["edges"].filter(pl.col("kind") == "accounting")
    assert acct.height == 4 and acct["points_worth"].null_count() == 0
    assert acct["points_worth"].sum() == pytest.approx(s["margin"], abs=1e-9)
    assert s["periods_agree"]


def test_basketball_accounting_unpriced_when_a_side_lacks_a_count(monkeypatch: pytest.MonkeyPatch) -> None:
    """One side without a count leaves the whole game's four parts out, never a partial sum."""
    fr = _frames(monkeypatch, "nba", "nba_401910002.json.gz")
    tb = fr["team_box"]
    loser = tb.sort("team_score")["team_id"][0]
    tb = tb.with_columns(
        pl.when(pl.col("team_id") == loser)
        .then(None)
        .otherwise(pl.col("offensive_rebounds"))
        .alias("offensive_rebounds")
    )
    edges = nba_game_story(fr["pbp"], tb, fr["player_box"])["edges"]
    acct = edges.filter(pl.col("kind") == "accounting")
    assert acct.height == 0
    assert edges.filter(pl.col("rank") == 1)["kind"][0] == "split"


def test_football_reads_the_processor_frame_too() -> None:
    """``CFBPlayProcess(...).plays_frame`` names the offense ``pos_team`` (the releases: ``pos_team_id``); same story."""
    pbp = pl.read_parquet(FB / "espn_cfb_pbp_games.parquet")
    a = cfb_game_story(pbp)
    b = cfb_game_story(pbp.rename({"pos_team_id": "pos_team"}))
    for k in a:
        assert a[k].equals(b[k]), k


def test_basketball_one_missing_possession_estimate_prices_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A null FTA on one side leaves the shared possession count unknown: no accounting edges, not the other's count."""
    fr = _frames(monkeypatch, "nba", "nba_401910002.json.gz")
    tb = fr["team_box"]
    away = tb.filter(pl.col("team_home_away") == "away")["team_id"][0]
    tb = tb.with_columns(
        pl.when(pl.col("team_id") == away)
        .then(None)
        .otherwise(pl.col("free_throws_attempted"))
        .alias("free_throws_attempted")
    )
    edges = nba_game_story(fr["pbp"], tb, fr["player_box"])["edges"]
    assert edges.filter(pl.col("kind") == "accounting").height == 0


def test_football_each_official_final_stands_alone() -> None:
    """With only homeFinalScore, the home side keeps its official final (401628337: plays end 20, official 14)."""
    pbp = pl.read_parquet(FB / "espn_cfb_pbp_games.parquet").filter(pl.col("game_id") == 401628337)
    s = cfb_game_story(pbp.drop("awayFinalScore"))["summary"].row(0, named=True)
    assert (s["home_score"], s["away_score"]) == (14, 21)
