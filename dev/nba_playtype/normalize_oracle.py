"""Normalize the raw captures (capture_oracle.py) into the committed oracle-corpus contract
+ build the sampled RAPM snapshot. Scratch script (dev/, not committed).

Run: SDV_PY_NBA_STATS_LIVE=1 uv run python dev/nba_playtype/normalize_oracle.py
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_rapm import nba_rapm

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_playtype"

N_RAPM_GAMES = 25


def _normalize_synergy(raw: pl.DataFrame, *, has_player: bool) -> pl.DataFrame:
    cols = {
        "team_id": pl.col("team_id").cast(pl.Int64),
        "play_type": pl.col("play_type"),
        "poss": pl.col("poss").cast(pl.Float64),
        "pts": pl.col("pts").cast(pl.Float64),
        "ppp": pl.col("ppp").cast(pl.Float64),
        "freq": pl.col("poss_pct").cast(pl.Float64),
        "turnover_freq": pl.col("tov_poss_pct").cast(pl.Float64),
        "ft_freq": pl.col("ft_poss_pct").cast(pl.Float64),
    }
    if has_player:
        cols["player_id"] = pl.col("player_id").cast(pl.Int64)
    return raw.select(**cols)


def main() -> None:
    off_team = _normalize_synergy(pl.read_parquet(FIX / "synergy_off_team_raw_2024.parquet"), has_player=False)
    def_team = _normalize_synergy(pl.read_parquet(FIX / "synergy_def_team_raw_2024.parquet"), has_player=False)
    off_player = _normalize_synergy(pl.read_parquet(FIX / "synergy_off_player_raw_2024.parquet"), has_player=True)
    off_team.write_parquet(FIX / "synergy_off_team_2024.parquet")
    def_team.write_parquet(FIX / "synergy_def_team_2024.parquet")
    off_player.write_parquet(FIX / "synergy_off_player_2024.parquet")
    print(f"synergy off_team={off_team.height} def_team={def_team.height} off_player={off_player.height}")

    matchups_raw = pl.read_parquet(FIX / "matchups_raw_2024.parquet")
    # matchup_min ships as "MM:SS" (string), not a numeric minutes value.
    mm_ss = pl.col("matchup_min").cast(pl.Utf8).str.split_exact(":", 1)
    matchups = matchups_raw.select(
        pl.col("off_player_id").cast(pl.Int64),
        pl.col("def_player_id").cast(pl.Int64),
        pl.col("partial_poss").cast(pl.Float64),
        pl.col("player_pts").cast(pl.Float64),
        (mm_ss.struct.field("field_0").cast(pl.Float64) + mm_ss.struct.field("field_1").cast(pl.Float64) / 60.0).alias(
            "matchup_min"
        ),
    )
    matchups.write_parquet(FIX / "matchups_2024.parquet")
    print(f"matchups={matchups.height}")

    base_raw = pl.read_parquet(FIX / "leaguedash_base_raw_2024.parquet")
    adv_raw = pl.read_parquet(FIX / "leaguedash_adv_raw_2024.parquet")
    poss_by_player = adv_raw.select(pl.col("player_id").cast(pl.Int64), pl.col("poss").cast(pl.Float64))
    base = base_raw.select(
        pl.col("player_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("gp").cast(pl.Int64),
        pl.col("min").cast(pl.Float64),
        pl.col("fga").cast(pl.Float64),
        pl.col("fta").cast(pl.Float64),
        pl.col("tov").cast(pl.Float64),
        pl.col("pf").cast(pl.Float64),
        pl.col("pfd").cast(pl.Float64),
    ).join(poss_by_player, on="player_id", how="left")
    base.write_parquet(FIX / "leaguedash_base_2024.parquet")
    adv = adv_raw.select(
        pl.col("player_id").cast(pl.Int64),
        pl.col("usg_pct").cast(pl.Float64),
        pl.col("poss").cast(pl.Float64),
    ).join(base.select("player_id", "pfd"), on="player_id", how="left")
    adv.write_parquet(FIX / "leaguedash_adv_2024.parquet")
    print(f"base={base.height} adv={adv.height}")

    gamelog_raw = pl.read_parquet(FIX / "gamelog_raw_2024.parquet")
    g = gamelog_raw.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Int64),
    )
    # each game_id has exactly 2 team rows; pair them for opp_team_id
    pairs = g.join(g, on="game_id", suffix="_opp").filter(pl.col("team_id") != pl.col("team_id_opp"))
    gamelog = pairs.select(
        pl.col("game_id"),
        pl.col("team_id"),
        pl.col("team_id_opp").alias("opp_team_id"),
    )
    gamelog.write_parquet(FIX / "gamelog_2024.parquet")
    print(f"gamelog={gamelog.height}")

    # --- sampled RAPM snapshot ---
    game_ids = sorted(gamelog_raw["game_id"].unique().to_list())[:N_RAPM_GAMES]
    print(f"Sampling {len(game_ids)} games for RAPM snapshot: {game_ids}")
    from sportsdataverse.nba.nba_possessions import nba_possessions

    frames = []
    for i, gid in enumerate(game_ids):
        try:
            poss = nba_possessions(gid, "00")
            print(f"  [{i + 1}/{len(game_ids)}] {gid}: {poss.height} possessions")
            frames.append(poss)
        except Exception as e:
            print(f"  [{i + 1}/{len(game_ids)}] {gid}: FAILED {type(e).__name__} {e}")
        time.sleep(0.4)

    all_poss = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    rapm = nba_rapm(all_poss)
    rapm.write_parquet(FIX / "rapm_2024.parquet")
    print(f"rapm={rapm.height} (from {all_poss.height} sampled possessions)")

    print("Done.")


if __name__ == "__main__":
    main()
