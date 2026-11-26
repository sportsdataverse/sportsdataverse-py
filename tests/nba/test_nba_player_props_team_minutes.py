"""A projected game hands each team exactly 5 x game_minutes: 240 in the NBA, 200 in the WNBA.

Runs ``nba_player_props`` end to end on the committed 2024 box logs, schedule and team box
(real ESPN captures, see ``tests/fixtures/nba_prediction/README.md``): the logs are stubbed in
with each game's date, and the ratings come from the real ``nba_team_ratings`` over the
committed schedule and team box.
"""

import importlib

from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"
props = importlib.import_module("sportsdataverse.nba.nba_player_props")
ratings_mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")

LEAGUES = pytest.mark.parametrize(("league_id", "prefix", "team_minutes"), [("00", "", 240.0), ("10", "wnba_", 200.0)])


def _stub_league(monkeypatch, league_id: str, prefix: str) -> pl.DataFrame:
    results = pl.read_parquet(FIXTURES / f"{prefix}results_2024.parquet")
    team_box = pl.read_parquet(FIXTURES / f"{prefix}team_box_2024.parquet")
    logs = (
        pl.read_parquet(FIXTURES / f"{prefix}player_box_logs_2024.parquet")
        .join(results.select("game_id", pl.col("date").alias("game_date")), on="game_id", how="inner")
        .with_columns(pl.lit(False).alias("starter"), pl.lit(2).alias("season_type"))
    )  # what NBA shrinkage reads
    monkeypatch.setattr(props, "_load_player_logs", lambda season, lg: logs)
    monkeypatch.setattr(ratings_mod, "_league_loaders", lambda lg: (lambda s: results, lambda s: team_box))
    return results.sort("date", "game_id")


@LEAGUES
def test_projected_minutes_sum_to_five_times_game_minutes_per_team(monkeypatch, league_id, prefix, team_minutes):
    results = _stub_league(monkeypatch, league_id, prefix)
    # five games spread over the season, each projected as of its own date
    for g in (
        results.slice(results.height // 6, results.height // 2)
        .gather_every(results.height // 10)
        .head(5)
        .rows(named=True)
    ):
        out = props.nba_player_props(2024, g["game_id"], g["home_team_id"], g["away_team_id"], league_id=league_id)
        totals = out.group_by("team_id").agg(pl.col("minutes_exp").sum())
        assert sorted(totals["team_id"]) == sorted([g["home_team_id"], g["away_team_id"]]), g["game_id"]
        assert totals["minutes_exp"].to_list() == pytest.approx([team_minutes, team_minutes]), g["game_id"]
        assert (out["minutes_exp"] >= 0).all()
        # the conditional mean minutes behind the stat lines over-allocate: the share is what sums right
        mean_minutes = props.player_rates(
            props._load_player_logs(2024, league_id).filter(pl.col("game_date") < g["date"])
        )
        mean_minutes = mean_minutes.filter(pl.col("team_id").is_in(totals["team_id"].to_list()))
        assert mean_minutes.group_by("team_id").agg(pl.col("minutes_pg").sum())["minutes_pg"].min() > team_minutes
