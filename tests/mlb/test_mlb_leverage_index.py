"""``leverage_index`` credits each state with the swing of the PA that STARTS there.

Corpus: ``tests/fixtures/mlb_game_state/pbp_corpus.parquet`` + ``results_corpus.parquet`` (statsapi,
1999-2002 April-June windows, 4,726 games; README in that folder).

The swing of a plate appearance is |WE after it - WE before it|. WE before it is the public
``mlb_win_expectancy`` value of its own row, WE after it the next row's, and after a game's last
PA the result (1 if the home team won, 0 if it lost). The previous estimator credited each PA with
the swing of the PA before it, so every game's first PA got the constant |WE(s0) - 0.5| and the
2023-2024 first-PA leverage came out at 0.41 against a commonly published 0.86-0.9.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import (
    _WE_KEY,
    _bucket,
    build_we_table,
    leverage_index,
    mlb_win_expectancy,
)

FIXTURE_DIR = "tests/fixtures/mlb_game_state"


@pytest.fixture(scope="module")
def corpus() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    pbp = pl.read_parquet(f"{FIXTURE_DIR}/pbp_corpus.parquet")
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")
    states = pbp_base_out_states(pbp)
    return pbp, results, states


def test_each_state_carries_the_swing_of_the_pa_that_starts_there(corpus) -> None:
    pbp, results, states = corpus
    li = leverage_index(states, build_we_table(states, results))

    keys = ["game_id", "at_bat_index"]
    # the PA rows only (mlb_win_expectancy also appends a terminal row per game)
    we = mlb_win_expectancy(pbp, results).join(states.select(keys), on=keys, how="semi").sort(keys)
    # a game called tied (3930, 2000-04-03, 3-3 after six) ends at 0.5, neither team's win
    won = results.unique("game_id").select(
        "game_id",
        pl.when(pl.col("home_score") > pl.col("away_score"))
        .then(1.0)
        .when(pl.col("home_score") < pl.col("away_score"))
        .then(0.0)
        .otherwise(0.5)
        .alias("result"),
    )
    assert we.schema["game_id"] == won.schema["game_id"] == states.schema["game_id"]
    nxt = pl.col("home_win_exp").shift(-1).over("game_id")
    swings = (
        we.join(won, on="game_id", how="inner")
        .with_columns(
            (pl.when(nxt.is_null()).then(pl.col("result")).otherwise(nxt) - pl.col("home_win_exp")).abs().alias("s")
        )
        .join(_bucket(states).select(*keys, *_WE_KEY), on=keys)
    )
    # every state the corpus reaches, from these swings alone
    expected = swings.group_by(_WE_KEY).agg((pl.col("s").mean() / swings["s"].mean()).alias("expected"))
    j = li.join(expected, on=_WE_KEY, how="inner")
    assert j.height == li.height == expected.height
    assert (j["leverage_index"] - j["expected"]).abs().max() < 1e-9


def test_the_games_first_pa_and_the_late_tie(corpus) -> None:
    # Every game opens in the first state, so the backward-looking estimator gave it the constant
    # |WE(s0) - 0.5| (0.79 on this corpus); the PA's own swing puts it near the published 0.86.
    _, results, states = corpus
    li = leverage_index(states, build_we_table(states, results))

    def at(inning: int, half: str) -> float:
        row = li.filter(
            (pl.col("inning_capped") == inning)
            & (pl.col("half") == half)
            & (pl.col("base_state") == "___")
            & (pl.col("outs_start") == 0)
            & (pl.col("score_diff_bucket") == 0)
        )
        assert row.height == 1
        return row["leverage_index"].item()

    first = at(1, "top")
    assert 0.80 <= first <= 0.92  # observed 0.858
    assert at(9, "bottom") >= 1.5 * first  # observed 1.626
