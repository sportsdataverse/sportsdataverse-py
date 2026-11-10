"""BB-D34 official season profiles, on committed real ESPN captures (tests/fixtures/bb_official_seasons).

Gates: each official's ``games`` equals the officials table's count of their completed games, and
``fouls_per_game`` / ``fta_per_game`` reconcile with the team-box totals of those games.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common import official_seasons as core
from sportsdataverse.nba.nba_game_page import team_game_rates

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_official_seasons"
LEAGUES = {"nba": 2025, "wnba": 2025, "mbb": 2026, "wbb": 2026}
REG = {"nba": 48, "wnba": 40, "mbb": 40, "wbb": 40}


def _load(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"{lg}_{name}_{LEAGUES[lg]}.parquet")


def _build(lg: str, season_type: int | None = 2) -> pl.DataFrame:
    games = core.official_game_rows(_load(lg, "team_box"), _load(lg, "schedule"), league=lg, season_type=season_type)
    return core.official_seasons(core.officials_rows(_load(lg, "officials")), games, regulation_minutes=REG[lg])


def _official_games(lg: str) -> pl.DataFrame:
    """(key, game_id) of every official on a completed regular-season game, straight from the raw tables."""
    sched = _load(lg, "schedule").filter((pl.col("status_type_completed") == True) & (pl.col("season_type") == 2))  # noqa: E712
    off = _load(lg, "officials")
    key = (
        pl.col("official_id").cast(pl.Utf8)
        if "official_id" in off.columns
        else pl.col("official_full_name").str.replace_all(r"\s+", " ").str.strip_chars()
    )
    return (
        off.select(key.alias("key"), pl.col("game_id").cast(pl.Int64))
        .unique()
        .join(sched.select(pl.col("game_id").cast(pl.Int64)), on="game_id", how="semi")
    )


def _keyed(out: pl.DataFrame) -> pl.DataFrame:
    return out.with_columns(pl.coalesce(pl.col("official_id").cast(pl.Utf8), pl.col("official_name")).alias("key"))


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_games_match_officials_table(lg: str) -> None:
    out = _keyed(_build(lg))
    want = _official_games(lg).group_by("key").agg(pl.len().cast(pl.Int64).alias("want"))
    both = out.join(want, on="key", how="full", coalesce=True)
    assert both.height == out.height == want.height
    assert (both["games"] == both["want"]).all()
    assert out["games"].sum() >= out.height  # every official worked a game


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_fouls_and_fta_reconcile_with_team_box(lg: str) -> None:
    out = _keyed(_build(lg))
    box = (
        _load(lg, "team_box")
        .group_by(pl.col("game_id").cast(pl.Int64))
        .agg(
            pl.col("fouls").sum().alias("fouls"),
            pl.col("free_throws_attempted").sum().alias("fta"),
            pl.len().alias("rows"),
        )
    )
    want = (
        _official_games(lg)
        .join(box.filter(pl.col("rows") == 2), on="game_id", how="inner")
        .group_by("key")
        .agg(pl.col("fouls").sum(), pl.col("fta").sum(), pl.len().alias("n"))
    )
    got = out.join(want, on="key", how="inner")
    assert got.height == want.height > 0
    assert (got["fouls_per_game_n"] == got["n"]).all()
    assert ((got["fouls_per_game"] * got["fouls_per_game_n"] - got["fouls"]).abs() < 1e-6).all()
    assert ((got["fta_per_game"] * got["fta_per_game_n"] - got["fta"]).abs() < 1e-6).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_possessions_match_the_game_page(lg: str) -> None:
    games = core.official_game_rows(_load(lg, "team_box"), _load(lg, "schedule"), league=lg)
    box = _load(lg, "team_box").with_columns(
        pl.col("field_goals_attempted").alias("field_goals_made"),  # unread by possessions/pace
        pl.lit(0).alias("three_point_field_goals_made"),
        pl.lit(0).alias("team_score"),
        pl.lit(0).alias("defensive_rebounds"),
    )
    sample = games.filter(pl.col("possessions").is_not_null() & pl.col("game_minutes").is_not_null()).head(25)
    sched = _load(lg, "schedule").with_columns(pl.col("game_id").cast(pl.Utf8))
    for row in sample.iter_rows(named=True):
        two = box.filter(pl.col("game_id").cast(pl.Utf8) == row["game_id"])
        s = sched.filter(pl.col("game_id") == row["game_id"]).row(0, named=True)
        ref = team_game_rates(
            two, league=lg, periods=int(s["status_period"]), regulation_periods=int(s["format_regulation_periods"])
        )
        assert ref["possessions"][0] == pytest.approx(row["possessions"])
        assert ref["game_minutes"][0] == row["game_minutes"]


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_home_rates_skip_neutral_sites(lg: str) -> None:
    games = core.official_game_rows(_load(lg, "team_box"), _load(lg, "schedule"), league=lg)
    out = _keyed(_build(lg))
    neutral = (
        _official_games(lg)
        .join(games.select(pl.col("game_id").cast(pl.Int64), "neutral"), on="game_id")
        .group_by("key")
        .agg(pl.col("neutral").sum().alias("neutral"))
    )
    got = out.join(neutral, on="key")
    assert (got["home_win_pct_n"] <= got["games"] - got["neutral"]).all()
    assert (got["home_minus_away_fouls_n"] <= got["games"] - got["neutral"]).all()
    assert got["home_win_pct"].drop_nulls().is_between(0.0, 1.0).all()
    if lg in ("mbb", "wbb"):
        assert got["neutral"].sum() > 0  # November college: holiday tournaments


@pytest.mark.parametrize(("lg", "lo", "hi"), [("nba", 95, 108), ("wnba", 75, 88), ("mbb", 62, 76), ("wbb", 64, 78)])
def test_pace_is_per_regulation_length(lg: str, lo: float, hi: float) -> None:
    out = _build(lg).filter(pl.col("pace_n") >= 10)
    assert out.height > 0
    assert out["pace"].is_between(lo, hi).all()


def test_season_type_none_adds_the_postseason() -> None:
    reg, every = _build("nba"), _build("nba", season_type=None)
    assert every["games"].sum() > reg["games"].sum()


def test_name_keyed_leagues_collapse_whitespace() -> None:
    names = _build("mbb")["official_name"]
    assert not names.str.contains("  ").any()
    assert _build("mbb")["official_id"].null_count() == names.len()
    assert _build("wbb")["official_id"].null_count() == 0


def test_empty_inputs_keep_the_schema() -> None:
    empty = core.official_seasons(
        pl.DataFrame(schema=core.OFFICIALS_SCHEMA), pl.DataFrame(schema=core.GAME_ROWS_SCHEMA), regulation_minutes=48
    )
    assert empty.is_empty() and dict(empty.schema) == core.OFFICIAL_SEASONS_SCHEMA
    assert dict(_build("wnba").schema) == core.OFFICIAL_SEASONS_SCHEMA


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_league_wrapper_reads_the_three_releases(lg: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    loaders = importlib.import_module(f"sportsdataverse.{lg}.{lg}_loaders")
    for name, fixture in (("officials", "officials"), ("team_boxscore", "team_box"), ("schedule", "schedule")):
        monkeypatch.setattr(loaders, f"load_{lg}_{name}", lambda seasons, _f=fixture: _load(lg, _f))
    fn = getattr(importlib.import_module(f"sportsdataverse.{lg}"), f"{lg}_official_seasons")
    assert fn(LEAGUES[lg]).equals(_build(lg))
    assert fn(LEAGUES[lg], return_as_pandas=True).shape == _build(lg).shape
