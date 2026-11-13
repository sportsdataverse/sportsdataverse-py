"""Per-36 box rates and the usage-adjusted box expectation, on real one-team-season captures."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.player_box_expected import (
    EXPECTED_COEFS,
    MODEL_FIRST_SEASON,
    PER36_MIN_N,
    PLAYER_GAME_EXPECTED_SCHEMA,
    PLAYER_SEASON_BOX_RATES_SCHEMA,
    player_game_box_rows,
    player_game_expected_box,
    player_season_box_rates,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_player_box_expected"
CAPTURES = {"nba": "nba_player_box_2025_team2", "wnba": "wnba_player_box_2025_team17",
            "mbb": "mbb_player_box_2026_team150", "wbb": "wbb_player_box_2026_team41"}  # fmt: skip
LEAGUES = list(CAPTURES)


def _rows(league: str) -> pl.DataFrame:
    return player_game_box_rows(pl.read_parquet(FIX / f"{CAPTURES[league]}.parquet"), league=league)


def _fit(league: str) -> dict:
    return json.loads((FIX / f"{league}_fit.json").read_text())


@pytest.mark.parametrize("league", LEAGUES)
def test_constants_are_the_committed_fit(league: str) -> None:
    fit = _fit(league)
    assert {s: tuple(v) for s, v in fit["coefs"].items()} == EXPECTED_COEFS[league]
    assert fit["per36_gates"] == PER36_MIN_N[league]
    # published only from the first holdout season: every expectation comes from earlier seasons' fit
    assert MODEL_FIRST_SEASON[league] == int(fit["holdout"].split("-")[0])
    assert int(fit["train"].split("-")[1]) < MODEL_FIRST_SEASON[league]


@pytest.mark.parametrize("league", LEAGUES)
def test_holdout_model_beats_the_per_minute_baseline_on_points(league: str) -> None:
    sc = _fit(league)["holdout_scores"]
    assert sc["pts"]["exp_mae"] < 0.85 * sc["pts"]["base_mae"]
    assert sc["pts"]["exp_rmse"] < sc["pts"]["base_rmse"]
    for s in ("reb", "ast"):  # usage says little about these: the model is at worst level with the baseline
        assert sc[s]["exp_mae"] <= sc[s]["base_mae"] * 1.005


@pytest.mark.parametrize("league", LEAGUES)
def test_usage_shares_add_up_per_team_game(league: str) -> None:
    r = _rows(league).filter(pl.col("usg_pct").is_not_null())
    assert r.height > 0
    tot = r.group_by("game_id").agg((pl.col("usg_pct") * pl.col("minutes") / (pl.col("team_minutes") / 5)).sum())
    assert tot["usg_pct"].min() == pytest.approx(100.0) and tot["usg_pct"].max() == pytest.approx(100.0)


@pytest.mark.parametrize("league", LEAGUES)
def test_baseline_leaves_the_game_out(league: str) -> None:
    rows = _rows(league)
    e = player_game_expected_box(rows, league=league)
    reg = rows.filter(pl.col("in_rate"))
    star = reg.group_by("athlete_id").agg(pl.col("minutes").sum()).sort("minutes").tail(1)["athlete_id"].item()
    g = reg.filter(pl.col("athlete_id") == star).sort("game_id").row(3, named=True)
    season = reg.filter(pl.col("athlete_id") == star)
    rate = (season["points"].sum() - g["points"]) / (season["minutes"].sum() - g["minutes"])
    row = e.filter((pl.col("game_id") == g["game_id"]) & (pl.col("athlete_id") == star)).row(0, named=True)
    assert row["rate_games"] == season.height - 1
    assert row["pts_base"] == pytest.approx(g["minutes"] * rate)
    # the game's own points never move its baseline
    bumped = rows.with_columns(
        pl.when((pl.col("game_id") == g["game_id"]) & (pl.col("athlete_id") == star))
        .then(pl.col("points") + 30)
        .otherwise(pl.col("points"))
        .alias("points")
    )
    again = player_game_expected_box(bumped, league=league)
    key = (pl.col("game_id") == g["game_id"]) & (pl.col("athlete_id") == star)
    assert again.filter(key)["pts_base"].item() == pytest.approx(row["pts_base"])
    assert (
        again.filter(~key & (pl.col("athlete_id") == star))["pts_base"].sum()
        > e.filter(~key & (pl.col("athlete_id") == star))["pts_base"].sum()
    )


@pytest.mark.parametrize("league", LEAGUES)
def test_postseason_is_measured_against_the_season(league: str) -> None:
    rows = _rows(league)
    e = player_game_expected_box(rows, league=league).filter(pl.col("season_type") == 3)
    assert e.height > 0
    games = rows.filter(pl.col("in_rate")).group_by("athlete_id").len()
    j = e.join(games, on="athlete_id")
    if league in ("nba", "wnba"):  # the pros' postseason is not in the rate, so nothing is left out
        assert (j["rate_games"] == j["len"]).all()
    else:  # college: every game is the season
        assert (j["rate_games"] == j["len"] - 1).all()


@pytest.mark.parametrize("league", LEAGUES)
def test_model_is_applied_from_its_first_season_only(league: str) -> None:
    rows = _rows(league)
    e = player_game_expected_box(rows, league=league)
    on = e.filter(pl.col("pts_exp").is_not_null())
    assert on.height > 0.5 * e.height
    assert (on["pts_exp"] - on["pts_base"]).abs().max() > 1.0  # the coefficients changed the output
    season = rows["season"].max()
    late = player_game_expected_box(rows, league=league, first_season=season + 1)
    assert late["pts_exp"].null_count() == late.height and late["pts_base"].null_count() < late.height


@pytest.mark.parametrize("league", LEAGUES)
def test_season_rates_and_ranks(league: str) -> None:
    s = player_season_box_rates(_rows(league), league=league)
    assert s.schema == pl.Schema(PLAYER_SEASON_BOX_RATES_SCHEMA)
    assert s["athlete_id"].is_unique().all()
    assert (s["pts_per36"] - 36 * s["pts"] / s["minutes"]).abs().max() < 1e-9
    gate = PER36_MIN_N[league]["pts_per36"]
    assert (
        s.filter(pl.col("pts_per36_n") < gate)["pts_per36_rank"].null_count()
        == s.filter(pl.col("pts_per36_n") < gate).height
    )
    ranked = s.filter(pl.col("pts_per36_rank").is_not_null())
    if s.filter(pl.col("pts_per36_n") >= gate).height < 10:  # one team's rotation can sit under the cohort floor
        assert ranked.height == 0
        return
    assert ranked.height >= 10
    best = ranked.sort("pts_per36_rank").row(0, named=True)
    assert best["pts_per36"] == ranked["pts_per36"].max() and best["pts_per36_pct"] > 90


def test_empty_frames_keep_the_schema() -> None:
    rows = player_game_box_rows(pl.DataFrame(), league="nba")
    assert player_game_expected_box(rows, league="nba").schema == pl.Schema(PLAYER_GAME_EXPECTED_SCHEMA)
    assert player_season_box_rates(rows, league="nba").schema == pl.Schema(PLAYER_SEASON_BOX_RATES_SCHEMA)
    with pytest.raises(ValueError, match="league"):
        player_game_box_rows(pl.DataFrame(), league="nhl")
