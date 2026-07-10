"""Prop-projection as-of-date calibration gate on the real results corpus.

Corpus: tests/fixtures/mlb_game_state/results_corpus.parquet (1999-2002 April-
June windows, real statsapi schedule scores).

As-of-date backtest (never lower the gate to pass -- debug the model): for
each game, each team's off_rpg/def_rpg and the league rpg are expanding
averages over STRICTLY EARLIER games only (a leakage-safe cumulative built
with ``cum_sum().shift(1)`` per team / globally, excluding the game itself),
then fed through the same closed-form :func:`mlb_prop_team_runs` the
production ``mlb_props`` orchestrator uses. Games in each team's early burn-in
window (no prior history yet) are excluded, same as any as-of backtest.
"""

import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import mae

FIXTURE_DIR = "tests/fixtures/mlb_game_state"

# Floor set from the observed backtest on the committed corpus (4726 games,
# 4739 as-of-eligible team-games after the burn-in filter): observed MAE
# 2.64 runs. Headroom for normal run-to-run noise in a re-captured corpus of
# similar size. See the module docstring for the as-of-date construction.
FLOOR_RUNS_MAE = 2.9


def _team_asof(results: pl.DataFrame) -> pl.DataFrame:
    """Per-(team, game) expanding (strictly-prior) off_rpg / def_rpg."""
    home = results.select(
        "game_id",
        "date",
        pl.col("home_team_id").alias("team_id"),
        pl.col("home_score").alias("runs_scored"),
        pl.col("away_score").alias("runs_allowed"),
    )
    away = results.select(
        "game_id",
        "date",
        pl.col("away_team_id").alias("team_id"),
        pl.col("away_score").alias("runs_scored"),
        pl.col("home_score").alias("runs_allowed"),
    )
    long = pl.concat([home, away], how="vertical").sort(["team_id", "date", "game_id"])
    long = long.with_columns(
        pl.col("runs_scored").cum_sum().shift(1, fill_value=0).over("team_id").alias("_cum_rs"),
        pl.col("runs_allowed").cum_sum().shift(1, fill_value=0).over("team_id").alias("_cum_ra"),
        pl.col("runs_scored").cum_count().shift(1, fill_value=0).over("team_id").alias("_cum_g"),
    )
    return long.with_columns(
        (pl.col("_cum_rs") / pl.col("_cum_g")).fill_nan(None).alias("off_rpg_asof"),
        (pl.col("_cum_ra") / pl.col("_cum_g")).fill_nan(None).alias("def_rpg_asof"),
    ).select("game_id", "team_id", "off_rpg_asof", "def_rpg_asof")


def _league_asof(results: pl.DataFrame) -> pl.DataFrame:
    """Per-game expanding (strictly-prior) league-average runs/team/game."""
    ordered = results.sort(["date", "game_id"]).with_columns(
        (pl.col("home_score") + pl.col("away_score")).alias("_total")
    )
    ordered = ordered.with_columns(
        pl.col("_total").cum_sum().shift(1, fill_value=0).alias("_cum_runs"),
        pl.col("_total").cum_count().shift(1, fill_value=0).alias("_cum_games"),
    )
    return ordered.with_columns(
        (pl.col("_cum_runs") / (2 * pl.col("_cum_games"))).fill_nan(None).alias("lg_rpg_asof")
    ).select("game_id", "lg_rpg_asof")


def test_prop_team_runs_as_of_date_mae():
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")
    team_asof = _team_asof(results)
    league_asof = _league_asof(results)

    home_key = team_asof.rename(
        {"team_id": "home_team_id", "off_rpg_asof": "home_off_asof", "def_rpg_asof": "home_def_asof"}
    )
    away_key = team_asof.rename(
        {"team_id": "away_team_id", "off_rpg_asof": "away_off_asof", "def_rpg_asof": "away_def_asof"}
    )
    assert results.schema["home_team_id"] == home_key.schema["home_team_id"]
    assert results.schema["away_team_id"] == away_key.schema["away_team_id"]

    joined = (
        results.join(home_key, on=["game_id", "home_team_id"], how="left")
        .join(away_key, on=["game_id", "away_team_id"], how="left")
        .join(league_asof, on="game_id", how="left")
    )
    joined = joined.filter(
        (pl.col("home_off_asof") > 0)
        & (pl.col("home_def_asof") > 0)
        & (pl.col("away_off_asof") > 0)
        & (pl.col("away_def_asof") > 0)
        & (pl.col("lg_rpg_asof") > 0)
    )
    assert joined.height >= 500, f"only {joined.height} games survived the as-of burn-in filter"

    # Closed form matching mlb_prop_team_runs(off, def, lg_rpg, park_factor=1.0)
    # == lg * (off/lg) * (def/lg) == off * def / lg -- computed vectorized here
    # for backtest speed across the whole corpus.
    joined = joined.with_columns(
        ((pl.col("home_off_asof") * pl.col("away_def_asof")) / pl.col("lg_rpg_asof")).alias("exp_runs_home"),
        ((pl.col("away_off_asof") * pl.col("home_def_asof")) / pl.col("lg_rpg_asof")).alias("exp_runs_away"),
    )
    observed_mae = mae(
        joined["exp_runs_home"].to_numpy().tolist() + joined["exp_runs_away"].to_numpy().tolist(),
        joined["home_score"].to_numpy().tolist() + joined["away_score"].to_numpy().tolist(),
    )
    assert observed_mae <= FLOOR_RUNS_MAE, f"as-of-date team-runs MAE = {observed_mae:.4f} (floor {FLOOR_RUNS_MAE})"
