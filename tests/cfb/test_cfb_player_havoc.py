"""cfb_player_havoc on Pittsburgh's real 2025 games (fixture README for provenance)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.cfb import cfb_player_havoc

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_player_havoc"
BILES = 4922981
BOWL = 401778316


@pytest.fixture(scope="module")
def frames():
    box = pl.read_parquet(FIXTURES / "player_box_2025_pitt.parquet")
    dp = pl.read_parquet(FIXTURES / "adv_defensive_players_2025_pitt.parquet")
    return box, dp


def _havoc(box, dp, **kw):
    return cfb_player_havoc(2025, player_box=box, defensive_players=dp, **kw)


def test_matches_espn_season_line(frames):
    box, dp = frames
    # ESPN's season endpoint leaves out the bowl, so compare the 9 regular-season games.
    row = _havoc(box.filter(pl.col("game_id") != BOWL), dp.filter(pl.col("game_id") != BOWL)).filter(
        pl.col("athlete_id") == BILES
    )
    r = row.to_dicts()[0]
    assert (r["games"], r["sacks"], r["pass_breakups"], r["interceptions"]) == (9, 2.5, 3.0, 2.0)
    # ESPN credits him one forced fumble its play text never names: the pbp column undercounts.
    assert r["forced_fumbles"] == 0.0


def test_havoc_is_events_without_double_counting_sacks(frames):
    h = _havoc(*frames)
    assert h.height and h.select(["season", "team_id", "athlete_id"]).is_duplicated().sum() == 0
    total = pl.col("tfl") + pl.col("pass_breakups") + pl.col("interceptions") + pl.col("forced_fumbles")
    assert h.filter((pl.col("havoc") - total).abs() > 1e-9).height == 0
    assert h.filter(pl.col("sacks") > pl.col("tfl")).height == 0  # a sack is already a tackle for loss
    biles = h.filter(pl.col("athlete_id") == BILES).to_dicts()[0]
    assert (biles["games"], biles["tfl"], biles["sacks"], biles["havoc"]) == (10, 17.0, 4.5, 23.0)
    assert biles["havoc_per_game"] == pytest.approx(2.3)


def test_thrown_interceptions_are_not_defensive(frames):
    box, dp = frames
    passers = box.filter(pl.col("category") == "passing")["athlete_id"].unique()
    defenders = box.filter(pl.col("category").is_in(["defensive", "interceptions"]))["athlete_id"].unique()
    qbs = passers.filter(~passers.is_in(defenders.implode()))
    assert qbs.len() > 0
    assert _havoc(box, dp).filter(pl.col("athlete_id").is_in(qbs.implode())).height == 0


def test_ranks_gate_on_games(frames):
    h = _havoc(*frames, min_games=6)
    ranked = h.filter(pl.col("havoc_per_game_rank").is_not_null())
    assert ranked.height >= 10
    assert ranked["havoc_per_game_n"].min() >= 6
    assert h.filter(pl.col("games") < 6)["havoc_per_game_rank"].null_count() == h.filter(pl.col("games") < 6).height
    top = ranked.sort("havoc_per_game_rank").row(0, named=True)
    assert top["athlete_id"] == BILES and top["havoc_per_game_rank"] == 1.0
    n = ranked.height
    assert top["havoc_per_game_pct"] == pytest.approx(100 * n / (n + 1))
    assert (h["havoc_per_game_n"] == h["games"]).all()


def test_unmatched_forced_fumble_warns_and_is_not_counted(frames):
    box, dp = frames
    ff = dp.filter(pl.col("forced_fumbles") > 0).row(0, named=True)
    renamed = dp.with_columns(
        pl.when((pl.col("game_id") == ff["game_id"]) & (pl.col("player_name") == ff["player_name"]))
        .then(pl.lit("Not On The Box"))
        .otherwise(pl.col("player_name"))
        .alias("player_name")
    )
    with pytest.warns(UserWarning, match="forced fumbles name a player with no box line"):
        h = _havoc(box, renamed)
    assert h["forced_fumbles"].sum() == _havoc(box, dp)["forced_fumbles"].sum() - ff["forced_fumbles"]


def test_empty_box_keeps_schema():
    out = cfb_player_havoc(2025, player_box=pl.DataFrame(), defensive_players=pl.DataFrame())
    assert out.height == 0 and {"havoc_per_game_rank", "havoc_per_game_pct", "havoc_per_game_n"} <= set(out.columns)
