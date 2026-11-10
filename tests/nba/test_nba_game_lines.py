"""BB-D33: nba_game_lines on real odds-store slices of all four leagues.

Fixtures: tests/fixtures/bb_game_lines (provenance in its README): the odds store's closing-line
checkpoints, ESPN crosswalk and props player crosswalk for a few weeks of each league, with the
ESPN schedule of those games. The pbp for :func:`wp_variants` is the D31 capture of 401704638.
"""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import nba_game_lines as gl
from sportsdataverse.nba import nba_game_page as gp
from sportsdataverse.nba import nba_pbp

FIX = Path(__file__).resolve().parents[1] / "fixtures"
LINES = FIX / "bb_game_lines"
LEAGUES = ("nba", "wnba", "mbb", "wbb")


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(LINES / f"{lg}_{name}.parquet")


@pytest.fixture(scope="module", params=LEAGUES)
def lines(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    lg = request.param
    return lg, gl.game_lines(
        _read(lg, "lines_checkpoints"), _read(lg, "espn_crosswalk"), _read(lg, "schedule"), league=lg
    )


def test_game_lines_gate(lines: tuple[str, pl.DataFrame]) -> None:
    """The D33 gate: WP in [0, 1], home + away = 1 after de-vig, closing spread agrees with the favourite."""
    lg, g = lines
    assert dict(g.schema) == gl.GAME_LINES_SCHEMA
    assert g["game_id"].is_unique().all()
    assert g.height >= 0.95 * _read(lg, "schedule").height
    for t in ("open", "close"):
        wp = g.drop_nulls(f"market_home_wp_{t}")
        assert wp.height >= 0.9 * g.height
        assert wp[f"market_home_wp_{t}"].is_between(0, 1).all()
        assert ((wp[f"market_home_wp_{t}"] + wp[f"market_away_wp_{t}"]) - 1).abs().max() < 1e-9
    s = g.drop_nulls(["spread_home_close", "market_home_wp_close"]).filter(
        pl.col("spread_home_close") != 0, pl.col("market_home_wp_close") != 0.5
    )
    assert s.height >= 0.9 * g.height
    assert ((s["spread_home_close"] < 0) == (s["market_home_wp_close"] > 0.5)).mean() > 0.95
    # the moneylines name the same favourite as the de-vigged probability
    m = g.drop_nulls(["ml_home_close", "ml_away_close", "market_home_wp_close"])
    assert ((m["ml_home_close"] < m["ml_away_close"]) == (m["market_home_wp_close"] > 0.5)).mean() > 0.99
    assert g["spread_home_close"].drop_nulls().abs().max() < 45
    assert g["total_close"].drop_nulls().is_between(100, 260).all()


def test_game_lines_result_columns() -> None:
    """GSW 140, POR 104 (2024-10-23): Portland +7.5 at home lost by 36, the total 222 went over."""
    g = gl.game_lines(_read("nba", "lines_checkpoints"), _read("nba", "espn_crosswalk"), _read("nba", "schedule"))
    row = g.filter(pl.col("game_id") == 401704638).row(0, named=True)
    assert row["spread_home_close"] == 7.5
    assert row["market_home_wp_close"] == pytest.approx(0.288, abs=0.01)
    assert (row["home_margin"], row["ats_result"], row["total_result"]) == (-36, "away", "over")
    # every completed game's result columns follow from its margin, line and score
    done = g.drop_nulls(["home_margin", "spread_home_close"])
    ats = done["home_margin"] + done["spread_home_close"]
    assert (done.filter(ats > 0)["ats_result"] == "home").all()
    assert (done.filter(ats < 0)["ats_result"] == "away").all()


def test_game_lines_turns_a_flipped_match_round() -> None:
    """401829457 matched with the teams swapped: the _home columns are ESPN's home team, the odds' away side."""
    lc, xw = _read("mbb", "lines_checkpoints"), _read("mbb", "espn_crosswalk")
    ev = xw.filter(pl.col("espn_game_id") == "401829457").row(0, named=True)
    assert ev["match_method"] == "flipped"
    h2h = lc.filter(
        pl.col("event_id") == ev["event_id"], pl.col("market_key") == "h2h", pl.col("close_mins_before") >= 0
    ).with_columns((1 / gl._decimal(pl.col("close_price"))).alias("p"))
    book = h2h.group_by("bookmaker_key").agg(
        (pl.col("p").filter(pl.col("outcome_name") == ev["away_team"]).first() / pl.col("p").sum()).alias("espn_home")
    )
    g = gl.game_lines(lc, xw, _read("mbb", "schedule"), league="mbb").filter(pl.col("game_id") == 401829457)
    assert g["market_home_wp_close"][0] == pytest.approx(book["espn_home"].median(), abs=1e-12)
    spread = lc.filter(
        pl.col("event_id") == ev["event_id"],
        pl.col("market_key") == "spreads",
        pl.col("outcome_name") == ev["away_team"],
    )
    assert g["spread_home_close"][0] == spread["close_point"].median()


def test_game_lines_empty_and_unknown_league() -> None:
    lc = _read("nba", "lines_checkpoints")
    empty = gl.game_lines(lc, _read("wnba", "espn_crosswalk"), _read("nba", "schedule"), league="nba")
    assert empty.is_empty() and dict(empty.schema) == gl.GAME_LINES_SCHEMA
    with pytest.raises(ValueError, match="not supported"):
        gl.game_lines(lc, _read("nba", "espn_crosswalk"), _read("nba", "schedule"), league="nfl")


@pytest.mark.parametrize("lg", ["nba", "wnba"])
def test_player_props(lg: str) -> None:
    p = gl.player_props(
        _read(lg, "props_checkpoints"),
        _read(lg, "espn_crosswalk"),
        _read(lg, "props_player_xwalk"),
        _read(lg, "schedule"),
        league=lg,
    )
    assert dict(p.schema) == gl.PLAYER_PROPS_SCHEMA
    assert p.height > 200
    assert p.select("game_id", "player_name", "stat").is_unique().all()
    assert set(p["stat"]) == set(gl.PROP_MARKETS.values())
    assert p["athlete_id"].is_not_null().mean() > 0.95
    assert p["over_prob"].is_between(0, 1).all() and (p["line"] > 0).all()
    # every athlete plays for one of the two teams in the game
    sched = _read(lg, "schedule")
    teams = p.drop_nulls("athlete_id").join(sched, on="game_id")
    assert ((teams["team_id"] == teams["home_id"]) | (teams["team_id"] == teams["away_id"])).all()


def test_player_props_takes_the_line_most_books_closed_on() -> None:
    """The line is the modal book line; its over probability is the median over those books."""
    raw = _read("nba", "props_checkpoints").filter(
        pl.col("market_key") == "player_points", pl.col("close_mins_before") >= 0
    )
    p = gl.player_props(
        raw, _read("nba", "espn_crosswalk"), _read("nba", "props_player_xwalk"), _read("nba", "schedule")
    ).filter(pl.col("stat") == "points")
    ev = _read("nba", "espn_crosswalk").select("event_id", pl.col("espn_game_id").cast(pl.Int32).alias("game_id"))
    lines = (
        raw.filter(pl.col("outcome_name") == "Over")
        .join(ev, on="event_id")
        .group_by("game_id", pl.col("outcome_description").alias("player_name"), "close_point")
        .agg(pl.col("bookmaker_key").n_unique().alias("n"))
    )
    mode = lines.sort(["n", "close_point"], descending=[True, False]).unique(
        ["game_id", "player_name"], keep="first", maintain_order=True
    )
    both = p.join(mode, on=["game_id", "player_name"])
    assert both.height == p.height
    assert (both["line"] == both["close_point"]).mean() > 0.97  # the rest are books missing one side


@pytest.fixture(scope="module")
def game_401704638(tmp_path_factory: pytest.TempPathFactory) -> pl.DataFrame:
    with gzip.open(FIX / "espn" / "basketball_pbp" / "nba_401704638.json.gz", "rt", encoding="utf-8") as f:
        raw = json.load(f)

    class _Resp:
        def json(self) -> dict:
            return copy.deepcopy(raw)

    mp = pytest.MonkeyPatch()
    mp.setattr(nba_pbp, "download", lambda url, **kwargs: _Resp())
    try:
        frames = gp.game_page_frames(nba_pbp.espn_nba_pbp(game_id=401704638, return_parsed=False), league="nba")
    finally:
        mp.undo()
    return frames["pbp"]


def test_wp_variants(game_401704638: pl.DataFrame) -> None:
    pbp = game_401704638
    market = gl.game_lines(
        _read("nba", "lines_checkpoints"), _read("nba", "espn_crosswalk"), _read("nba", "schedule")
    ).filter(pl.col("game_id") == 401704638)["market_home_wp_close"][0]
    wp = gl.wp_variants(pbp, market, league="nba")
    assert wp.height == pbp.height and wp["game_play_number"].equals(pbp["game_play_number"])
    for c in ("home_win_prob_naive", "home_win_prob_market"):
        assert wp[c].is_between(0, 1).all()
        assert wp[c][-1] == 0.0  # Portland lost: the end row settles
    # the priors differ, so the curves start apart and the naive one starts nearer 0.5
    assert abs(wp["home_win_prob_naive"][0] - 0.5) < abs(wp["home_win_prob_market"][0] - 0.5)
    # no-op guard: the market variant with the model's own prior is the model's home_win_prob (this
    # capture carries no ESPN line, so the model's prior is home-court advantage alone)
    assert pbp["prior_source"][0] == "hfa"
    same = gl.wp_variants(pbp, pbp["pregame_home_prob"][0], league="nba")
    assert (same["home_win_prob_market"] - pbp["home_win_prob"]).abs().max() < 1e-12
    assert gl.wp_variants(pbp, None, league="nba")["home_win_prob_market"].is_null().all()
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        gl.wp_variants(pbp, 1.2, league="nba")


def test_game_lines_leaves_out_a_close_taken_after_tip() -> None:
    """A book whose last quote came in-game reads as no closing quote, not as a (wild) closing line."""
    lc, xw, sched = _read("nba", "lines_checkpoints"), _read("nba", "espn_crosswalk"), _read("nba", "schedule")
    ev = xw.filter(pl.col("espn_game_id") == "401704638")["event_id"][0]
    hit = (pl.col("event_id") == ev) & (pl.col("bookmaker_key") == "draftkings")
    late = lc.with_columns(
        pl.when(hit).then(-30).otherwise(pl.col("close_mins_before")).alias("close_mins_before"),
        pl.when(hit).then(pl.col("close_price") * 40).otherwise(pl.col("close_price")).alias("close_price"),
    )
    dropped = lc.with_columns(
        pl.when(hit).then(None).otherwise(pl.col(c)).alias(c) for c in ("close_price", "close_point")
    )
    one = pl.col("game_id") == 401704638
    got = gl.game_lines(late, xw, sched).filter(one)
    assert got.equals(gl.game_lines(dropped, xw, sched).filter(one))
    assert got["n_books"][0] == gl.game_lines(lc, xw, sched).filter(one)["n_books"][0] - 1


def test_wp_variants_ignore_the_pbp_line(game_401704638: pl.DataFrame) -> None:
    """The variants swap the prior even when the plays carry an ESPN line (which score_win_prob would prefer)."""
    lined = game_401704638.with_columns(
        pl.lit(-7.5).alias("home_team_spread"), pl.lit(True).alias("game_spread_available")
    )
    assert gp.score_win_prob(lined, league="nba")["prior_source"][0] == "spread"
    assert gl.wp_variants(lined, 0.29, league="nba").equals(gl.wp_variants(game_401704638, 0.29, league="nba"))
