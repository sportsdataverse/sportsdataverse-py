"""WE/WPA/LI oracle gate vs statsapi win-probability + the WPA-sum identity.

Corpus: tests/fixtures/mlb_game_state/pbp_corpus.parquet includes game 7746
(2001-06-21), a game already inside the 1999-2002 RE24-era capture windows,
so this test can compare against tests/fixtures/mlb_game_state/winprob_game.
parquet (statsapi's own homeTeamWinProbability / leverageIndex for that same
game) on an era-matched basis. An earlier draft reused a modern 2024 game for
convenience, but applying a 1999-2002-built WE table to a 2024 game showed a
real, reproducible cross-era gap (several mid-game states off by ~0.09-0.13),
not a model bug -- see tests/fixtures/mlb_game_state/README.md.

Gate (never lower to pass -- debug the model):
  - corr(home_win_exp, statsapi home_team_win_probability) >= 0.95
  - per-game |sum(home_wpa) - (+-0.5)| <= 0.02 (WPA-sum identity), over the
    games with recorded play-by-play (statsapi ships a handful of historical
    games -- verified live, not a capture artifact -- with an empty
    ``allPlays`` array; see the join-count floor below)
"""

import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import spearman_corr
from sportsdataverse.mlb.mlb_win_expectancy import mlb_win_expectancy, mlb_win_probability_added

FIXTURE_DIR = "tests/fixtures/mlb_game_state"
WE_GAME_ID = "7746"


def test_we_matches_statsapi_concurrent_validity():
    pbp = pl.read_parquet(f"{FIXTURE_DIR}/pbp_corpus.parquet")
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")
    oracle = pl.read_parquet(f"{FIXTURE_DIR}/winprob_game.parquet")

    we = mlb_win_expectancy(pbp, results)
    one = we.filter(pl.col("game_id") == WE_GAME_ID)
    assert one.height > 0, f"game {WE_GAME_ID} missing from pbp_corpus -- capture regression"

    assert one.schema["at_bat_index"] == oracle.schema["at_bat_index"]
    j = one.join(oracle, on="at_bat_index", how="inner")
    assert j.height >= 60, f"joined only {j.height} plays (expected >= 60 for a full 9-inning game)"

    corr = spearman_corr(
        j["home_win_exp"].to_numpy(),
        j["home_team_win_probability"].to_numpy() / 100.0,
    )
    assert corr >= 0.95, f"corr(home_win_exp, statsapi WP) = {corr:.4f} (floor 0.95)"


def test_wpa_sum_identity_per_game():
    pbp = pl.read_parquet(f"{FIXTURE_DIR}/pbp_corpus.parquet")
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")

    we = mlb_win_expectancy(pbp, results)
    wpa = mlb_win_probability_added(we)
    per_game = wpa.group_by("game_id").agg(pl.col("wpa").sum().alias("s"))
    won = results.select("game_id", (pl.col("home_score") > pl.col("away_score")).alias("home_won"))
    assert per_game.schema["game_id"] == won.schema["game_id"]
    chk = per_game.join(won, on="game_id", how="inner")
    # 27/4726 games in this corpus have a verified-live-empty statsapi
    # playByPlay (allPlays == []) -- a real historical data gap, not a
    # capture-retry artifact (re-fetched directly and confirmed empty).
    # Floor set from that observed count with a little headroom.
    assert chk.height >= results.height - 35, f"only {chk.height}/{results.height} games matched on join"

    target = chk["home_won"].cast(pl.Float64).map_elements(lambda w: 0.5 if w else -0.5, return_dtype=pl.Float64)
    max_gap = (chk["s"] - target).abs().max()
    assert max_gap <= 0.02, f"max |sum(wpa) - target| = {max_gap:.4f} (floor 0.02)"
