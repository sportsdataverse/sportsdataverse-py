"""One-off recon: is primary-assist-rate split-half stability actually
higher/more powered than secondary-assist-rate on the full 120-game
committed corpus? (informs T5.2 model 3 flesh-out; not committed logic)
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_expected_assists import extract_goals_with_assists
from sportsdataverse.nhl.nhl_microstat_constants import fit_shot_xg, split_half_stability
from tests.nhl.conftest import games_appeared

pbp = pl.read_parquet("tests/fixtures/nhl_microstat/pbp_2024_slice.parquet")
goals = extract_goals_with_assists(pbp, xg_model=fit_shot_xg(pbp))
games = pbp.select("game_id").unique().sort("game_id").with_row_index("g")
half_of = games.with_columns((pl.col("g") % 2).alias("half")).select("game_id", "half")
gp = (
    games_appeared(pbp)
    .join(half_of, on="game_id")
    .group_by(["player_id", "half"])
    .agg(pl.col("game_id").n_unique().alias("gp"))
)

for min_games in (2, 3):
    eligible = (
        gp.filter(pl.col("gp") >= min_games)
        .group_by("player_id")
        .agg(pl.len().alias("halves"))
        .filter(pl.col("halves") == 2)["player_id"]
    )
    print(f"--- min_games_per_half={min_games}, eligible pool (any assist role): {eligible.len()} ---")

    for slot, col in (("primary", "assist1_player_id"), ("secondary", "assist2_player_id")):
        involve = goals.select(pl.col(col).alias("player_id"), "game_id").filter(pl.col("player_id").is_not_null())
        involve = involve.join(half_of, on="game_id").filter(pl.col("player_id").is_in(eligible.implode()))
        n_players = involve["player_id"].n_unique()
        rate_frame = (
            involve.group_by(["player_id", "half"]).agg(pl.len().alias("cnt")).join(gp, on=["player_id", "half"])
        )
        # only players present in both halves for THIS slot
        wide_check = rate_frame.group_by("player_id").agg(pl.len().alias("halves")).filter(pl.col("halves") == 2)
        stability = split_half_stability(rate_frame, id_col="player_id", half_col="half", num_col="cnt", den_col="gp")
        print(
            f"  {slot}: n_players_any_half={n_players}, n_players_both_halves={wide_check.height}, stability={stability:.4f}"
        )

# Population-level (not per-player) check: relative danger of goals with a
# credited secondary assist vs goals with only a primary assist.
only_primary = goals.filter(pl.col("assist1_player_id").is_not_null() & pl.col("assist2_player_id").is_null())
with_secondary = goals.filter(pl.col("assist2_player_id").is_not_null())
print(f"\nonly-primary goals: n={only_primary.height}, mean_goal_xg={only_primary['goal_xg'].mean():.4f}")
print(f"with-secondary goals: n={with_secondary.height}, mean_goal_xg={with_secondary['goal_xg'].mean():.4f}")
