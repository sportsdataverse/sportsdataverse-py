"""BB-D22: NBA / WNBA luck and close-game record on committed real captures.

Fixtures (READMEs in each directory; none synthetic):

* BB-D3's ``tests/fixtures/bb_team_season_rates`` -- the 2025-26 NBA and 2025 WNBA possession and
  schedule releases.
* ``tests/fixtures/bb_team_luck/{nba,wnba}_espn_close_window_*.parquet`` -- ESPN's play-by-play
  (a different source from the stats.nba.com possessions) for the same regular seasons: every
  play of the 4th quarter or an overtime with 5:00 or less left, with the score before it, and
  the stats.nba.com game id it was matched to by ET date and home team.

The close-game recount from ESPN's score is independent of the possession engine: a game is close
when the score before any play in the window is within 5. Observed agreement (gate = observed;
never loosen a gate to pass):
NBA 2026 1215 of 1229 games, WNBA 2025 284 of 286. All 14 NBA misses are games where the
possessions' made-shot points do not add up to the final margin (250 of 1230 games are off by 1-6
points, so the running margin drifts). The possession live at the 5:00 mark counts in the window
(it starts before 5:00 and ends after it); before that rule 5 more NBA games (1210) and 2 WNBA
games (282) missed, and the 2026 close games were 546, not 551. Per team, the season's close-game
count is within 3 of ESPN's (NBA; mean 0.93) and 1 (WNBA).

The ``bb-team-luck`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.team_luck import (
    CLOSE_SCHEMA,
    GAME_RESULTS_SCHEMA,
    PYTHAG_EXPONENT,
    TEAM_LUCK_SCHEMA,
    close_game_flags,
    season_records,
    team_luck,
)
from sportsdataverse._common.team_season_rates import team_season_rates

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
RATES = FIXTURES / "bb_team_season_rates"
LUCK = FIXTURES / "bb_team_luck"
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
mod = importlib.import_module("sportsdataverse.nba.nba_team_luck")
#: league -> (season, league_id, games agreeing with the ESPN recount, games, worst team |diff|).
RECOUNT = {"nba": (2026, "00", 1215, 1229, 3), "wnba": (2025, "10", 284, 286, 1)}


def _fixture_loader(name: str, expect: list[int]):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)

    def load(seasons: list[int]) -> pl.DataFrame:
        assert list(seasons) == expect, f"{name}: loader called with {seasons}, expected {expect}"
        return frame

    return load


def _luck(lg: str, season: int) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        if lg == "nba":
            # The stats loaders take the START year; the table is named by the END year.
            mp.setattr(
                tsr, "load_nba_stats_possessions", _fixture_loader(f"nba_possessions_{season}.parquet", [season - 1])
            )
            mp.setattr(tsr, "load_nba_stats_schedules", _fixture_loader(f"nba_schedule_{season}.parquet", [season - 1]))
            return mod.nba_team_luck(season)
        from sportsdataverse.wnba import wnba_loaders
        from sportsdataverse.wnba.wnba_team_luck import wnba_team_luck

        mp.setattr(
            wnba_loaders, "load_wnba_stats_possessions", _fixture_loader(f"wnba_possessions_{season}.parquet", [season])
        )
        mp.setattr(
            wnba_loaders, "load_wnba_stats_schedules", _fixture_loader(f"wnba_schedule_{season}.parquet", [season])
        )
        return wnba_team_luck(season)


def _rows(lg: str, season: int) -> pl.DataFrame:
    return tsr.nba_possession_rate_rows(
        pl.read_parquet(RATES / f"{lg}_possessions_{season}.parquet"),
        pl.read_parquet(RATES / f"{lg}_schedule_{season}.parquet"),
        league_id=RECOUNT[lg][1],
    )


@pytest.fixture(scope="module")
def nba_2026() -> pl.DataFrame:
    return _luck("nba", 2026)


def test_schema_and_record(nba_2026: pl.DataFrame) -> None:
    assert dict(nba_2026.schema) == TEAM_LUCK_SCHEMA
    assert nba_2026.height == 30 and nba_2026["team_id"].n_unique() == 30
    assert (nba_2026["games"] == 82).all() and (nba_2026["wins"] + nba_2026["losses"] == 82).all()
    assert nba_2026["wins"].sum() == nba_2026["losses"].sum() == 1230
    assert (nba_2026["pbp_games"] == nba_2026["games"]).all()  # every game has possessions
    assert nba_2026["luck_wins_rank"].null_count() == 0 and nba_2026["luck_wins_rank"].min() == 1.0
    assert (nba_2026["luck_wins_n"] == 82).all()


def test_expected_wins_by_hand(nba_2026: pl.DataFrame) -> None:
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(pl.col("season_type") == "regular-season")
    pts: dict[int, float] = {}
    opp: dict[int, float] = {}
    for h, a, hp, ap in sched.select("home_team_id", "away_team_id", "home_pts", "away_pts").iter_rows():
        pts[h], opp[h] = pts.get(h, 0) + hp, opp.get(h, 0) + ap
        pts[a], opp[a] = pts.get(a, 0) + ap, opp.get(a, 0) + hp
    x = PYTHAG_EXPONENT["nba"]
    for row in nba_2026.iter_rows(named=True):
        p, o = pts[row["team_id"]], opp[row["team_id"]]
        assert (row["pts"], row["opp_pts"]) == (p, o)
        assert row["exp_wins"] == pytest.approx(82 * p**x / (p**x + o**x), rel=1e-9)
        assert row["luck_wins"] == pytest.approx(row["wins"] - row["exp_wins"], abs=1e-9)
    # a higher points ratio means more expected wins (the ratio, not the margin, sets the curve)
    by_ratio = nba_2026.with_columns((pl.col("pts") / pl.col("opp_pts")).alias("r")).sort("r")
    assert np.all(np.diff(by_ratio["exp_wins"].to_numpy()) > 0)


def test_close_games_are_bb_d3_close_split(nba_2026: pl.DataFrame) -> None:
    rates = team_season_rates(_rows("nba", 2026), minutes_per_game=48).filter(split="close")
    joined = nba_2026.join(rates.select("team_id", pl.col("games").alias("rate_games")), on="team_id", how="left")
    assert (joined["close_games"] == joined["rate_games"].fill_null(0)).all()
    assert (nba_2026["close_wins"] + nba_2026["close_losses"] == nba_2026["close_games"]).all()
    # every close game has one NBA winner and one NBA loser
    assert nba_2026["close_wins"].sum() == nba_2026["close_losses"].sum() == 551


def _espn_recount(lg: str, season: int) -> pl.DataFrame:
    """Close from ESPN's score: within 5 before any play in the last-5:00 window (independent code)."""
    window = pl.read_parquet(LUCK / f"{lg}_espn_close_window_{season}.parquet")
    margin = (pl.col("home_before") - pl.col("away_before")).abs()
    return window.group_by("stats_game_id").agg((margin <= 5).any().alias("close_pbp"))


@pytest.mark.parametrize("lg", sorted(RECOUNT))
def test_close_games_match_an_espn_pbp_recount(lg: str) -> None:
    season, _, agree_gate, n_games, team_gate = RECOUNT[lg]
    ours = close_game_flags(_rows(lg, season))
    espn = _espn_recount(lg, season)
    assert ours.schema["game_id"] == espn.schema["stats_game_id"]
    j = ours.join(espn, left_on="game_id", right_on="stats_game_id", how="inner", validate="1:1")
    assert j.height == n_games
    assert (j["close"] == j["close_pbp"]).sum() >= agree_gate
    # per team, over the season
    luck = _luck(lg, season)
    sched = pl.read_parquet(RATES / f"{lg}_schedule_{season}.parquet").filter(pl.col("season_type") == "regular-season")
    per_team = (
        mod.nba_game_results(sched)
        .join(espn, left_on="game_id", right_on="stats_game_id", how="inner")
        .group_by("team_id")
        .agg(pl.col("close_pbp").sum().alias("pbp_close"))
    )
    k = luck.join(per_team, on="team_id", how="inner", validate="1:1")
    assert k.height == luck.height
    assert (k["close_games"] - k["pbp_close"]).abs().max() <= team_gate


def test_wnba_binding(nba_2026: pl.DataFrame) -> None:
    wnba = _luck("wnba", 2025)
    assert dict(wnba.schema) == TEAM_LUCK_SCHEMA and wnba.height == 13
    assert (wnba["games"] == 44).all()
    x = PYTHAG_EXPONENT["wnba"]
    row = wnba.row(0, named=True)
    assert row["pythag_win_pct"] == pytest.approx(1 / (1 + (row["opp_pts"] / row["pts"]) ** x), rel=1e-12)


def test_tied_rows_are_dropped_with_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(pl.col("season_type") == "regular-season")
    results = mod.nba_game_results(sched)
    # The stats schedule lists the cancelled 2013-04-16 Celtics-Pacers game (0021201214) at 0-0.
    cancelled = pl.DataFrame(
        {"season": [2026] * 2, "game_id": ["0021201214"] * 2, "team_id": [1610612738, 1610612754]}
    ).with_columns(pl.col("team_id").reverse().alias("opp_team_id"), pl.lit(0).alias("pts"), pl.lit(0).alias("opp_pts"))
    with caplog.at_level(logging.WARNING, logger="sportsdataverse._common.team_luck"):
        rec = season_records(pl.concat([results, cancelled.cast(GAME_RESULTS_SCHEMA)]))
    assert "dropped 2 tied team-games" in caplog.text
    assert (rec["games"] == 82).all()


def test_guards() -> None:
    with pytest.raises(ValueError, match="league_id"):
        mod.nba_team_luck(2026, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        mod.nba_team_luck(2026, season_type="regular")
    empty = team_luck(pl.DataFrame(schema=GAME_RESULTS_SCHEMA), close_game_flags(_rows("nba", 2026)), exponent=14.0)
    assert dict(empty.schema) == TEAM_LUCK_SCHEMA and empty.height == 0
    results = mod.nba_game_results(pl.read_parquet(RATES / "nba_schedule_2026.parquet"))
    with pytest.raises(ValueError, match="appears twice"):
        season_records(pl.concat([results, results.head(1)]))
    with pytest.raises(TypeError, match="game_id must be Utf8"):
        team_luck(
            results.with_columns(pl.col("game_id").cast(pl.Int64)), pl.DataFrame(schema=CLOSE_SCHEMA), exponent=14.0
        )
    with pytest.raises(KeyError, match="rows missing"):
        close_game_flags(pl.DataFrame({"game_id": ["1"]}))
