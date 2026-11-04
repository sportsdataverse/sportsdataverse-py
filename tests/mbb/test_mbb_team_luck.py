"""BB-D22: men's and women's college luck and close-game record on committed real captures.

Fixtures, none synthetic:

* BB-D3's ``tests/fixtures/bb_team_season_rates`` -- every 2024-25 game of a Big Ten team from the
  ``ncaa_{mbb,wbb}_possessions`` release (the close games).
* BB-D19's ``tests/fixtures/bb_team_strength`` -- the same games' ESPN schedule rows with final
  scores (the record and the Pythagorean inputs).
* ``tests/fixtures/bb_team_luck/{mbb,wbb}_espn_close_window_2025.parquet`` (README there) --
  ESPN's play-by-play of those games in the last regulation period (the 2nd half, the 4th quarter
  for women) or an overtime with 5:00 or less left, with the score before each play.

The ESPN recount is independent of the NCAA possessions: a different source, its own clock, keyed
straight on ESPN's game id. Observed agreement on the committed games (gate = observed; never
loosen a gate to pass): men's
404 of 412, women's 387 of 390. On the full 2025 releases it is 5998 of 6129 (97.9%) and 5386 of
5456 (98.7%), the pbp-only side larger in both (the score at the 5:00 mark is read only when a
possession starts there).
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.team_luck import MIN_CLOSE_GAMES, PYTHAG_EXPONENT, TEAM_LUCK_SCHEMA, team_luck

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
RATES = FIXTURES / "bb_team_season_rates"
STRENGTH = FIXTURES / "bb_team_strength"
LUCK = FIXTURES / "bb_team_luck"
tsr = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")
mod = importlib.import_module("sportsdataverse.mbb.mbb_team_luck")
LEAGUE = {"mbb": "mens", "wbb": "womens"}
#: league -> (games agreeing with the ESPN recount, games recounted).
RECOUNT = {"mbb": (404, 412), "wbb": (387, 390)}


def _loader(path: Path):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(path)

    def load(seasons: object) -> pl.DataFrame:
        assert seasons == 2025, seasons
        return frame

    return load


def _luck(lg: str) -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        if lg == "mbb":
            mp.setattr(tsr, "load_ncaa_mbb_possessions", _loader(RATES / "ncaa_mbb_possessions_2025.parquet"))
            mp.setattr(tsr, "load_mbb_schedule", _loader(STRENGTH / "mbb_schedule_2025.parquet"))
            return mod.mbb_team_luck(2025)
        from sportsdataverse.wbb import wbb_loaders
        from sportsdataverse.wbb.wbb_team_luck import wbb_team_luck

        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", _loader(RATES / "ncaa_wbb_possessions_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_schedule", _loader(STRENGTH / "wbb_schedule_2025.parquet"))
        return wbb_team_luck(2025)


@pytest.fixture(scope="module", params=["mbb", "wbb"])
def college(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _luck(request.param)


def test_schema_and_cohort(college: tuple[str, pl.DataFrame]) -> None:
    lg, luck = college
    assert dict(luck.schema) == TEAM_LUCK_SCHEMA
    ranked = luck.filter(pl.col("luck_wins_rank").is_not_null())
    # The 18 Big Ten teams; their one-to-three-game opponents stay out of the cohort.
    assert ranked.height == 18 and ranked["games"].min() >= 0.5 * luck["games"].max()
    assert (luck["wins"] + luck["losses"] == luck["games"]).all()
    assert (luck["close_wins"] + luck["close_losses"] == luck["close_games"]).all()
    assert (luck["pbp_games"] <= luck["games"]).all()


def test_expected_wins_by_hand(college: tuple[str, pl.DataFrame]) -> None:
    lg, luck = college
    sched = (
        pl.read_parquet(STRENGTH / f"{lg}_schedule_2025.parquet")
        .filter(pl.col("status_type_completed") == True)  # noqa: E712
        .unique("game_id")
    )
    x = PYTHAG_EXPONENT[lg]
    for row in luck.filter(pl.col("luck_wins_rank").is_not_null()).iter_rows(named=True):
        home = sched.filter(pl.col("home_id") == row["team_id"])
        away = sched.filter(pl.col("away_id") == row["team_id"])
        p = home["home_score"].sum() + away["away_score"].sum()
        o = home["away_score"].sum() + away["home_score"].sum()
        wins = (home["home_score"] > home["away_score"]).sum() + (away["away_score"] > away["home_score"]).sum()
        assert (row["pts"], row["opp_pts"], row["wins"], row["games"]) == (p, o, wins, home.height + away.height)
        assert row["exp_wins"] == pytest.approx(row["games"] / (1 + (o / p) ** x), rel=1e-9)


def test_close_games_match_an_espn_pbp_recount(college: tuple[str, pl.DataFrame]) -> None:
    lg, luck = college
    poss = pl.read_parquet(RATES / f"ncaa_{lg}_possessions_2025.parquet")
    rows = tsr.ncaa_possession_rate_rows(
        poss, pl.read_parquet(STRENGTH / f"{lg}_schedule_2025.parquet"), league=LEAGUE[lg]
    )
    ours = mod._espn_close_flags(poss, rows)
    window = pl.read_parquet(LUCK / f"{lg}_espn_close_window_2025.parquet")
    espn = window.group_by("game_id").agg(
        ((pl.col("home_before") - pl.col("away_before")).abs() <= 5).any().alias("pbp")
    )
    assert ours.schema["game_id"] == espn.schema["game_id"] == pl.Utf8
    j = ours.join(espn, on="game_id", how="inner", validate="1:1")
    agree, n = RECOUNT[lg]
    assert j.height == n and (j["close"] == j["pbp"]).sum() >= agree
    # the table's close games are those flags, joined on ESPN's game id to each side's result
    results = mod.espn_game_results(pl.read_parquet(STRENGTH / f"{lg}_schedule_2025.parquet"))
    classed = results.join(ours, on="game_id", how="inner")
    sides = classed.filter(pl.col("close"))
    assert sides.height > 0 and luck["close_games"].sum() == sides.height
    # pbp_games counts only the classed games: four women's Big Ten games ESPN carries have no
    # NCAA possessions (every men's game has them)
    assert luck["pbp_games"].sum() == classed.height
    assert luck["games"].sum() - classed.height == {"mbb": 0, "wbb": 8}[lg]


def test_close_win_pct_rank_needs_classed_close_games() -> None:
    sched = pl.read_parquet(STRENGTH / "mbb_schedule_2025.parquet")
    poss = pl.read_parquet(RATES / "ncaa_mbb_possessions_2025.parquet")
    flags = mod._espn_close_flags(poss, tsr.ncaa_possession_rate_rows(poss, sched, league="mens"))
    results = mod.espn_game_results(sched)
    # Keep Purdue's (ESPN 2509) possessions for 17 of its 36 games, its close ones among them: fewer
    # than half its games are classed, though it keeps every close game.
    purdue = (
        results.filter(pl.col("team_id") == 2509)
        .join(flags, on="game_id", how="inner")
        .sort("close", "game_id", descending=[True, False])["game_id"]
    )
    dropped = flags.filter(~pl.col("game_id").is_in(purdue.slice(17).to_list()))
    luck = team_luck(results, dropped, exponent=PYTHAG_EXPONENT["mbb"])
    row = luck.filter(pl.col("team_id") == 2509).row(0, named=True)
    assert row["pbp_games"] == 17 < 0.5 * row["games"] and row["close_games"] >= MIN_CLOSE_GAMES
    assert row["luck_wins_rank"] is not None and row["close_win_pct_rank"] is None
    others = luck.filter(pl.col("luck_wins_rank").is_not_null() & (pl.col("team_id") != 2509))
    assert others.height == 17 and others["close_win_pct_rank"].null_count() == 0


def test_bad_league_raises() -> None:
    with pytest.raises(ValueError, match="league"):
        mod.mbb_team_luck(2025, league="coed")
