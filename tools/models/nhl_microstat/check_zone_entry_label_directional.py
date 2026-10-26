"""One-off recon: observed controlled-vs-dump post-entry same-team shot-follow
rate on the committed 120-game corpus, using the fleshed-out event-sequence
controlled/dump heuristic. Informs the floor for
test_zone_entry_label_directional_sanity (not committed logic).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_zone_transitions import infer_zone_transitions

POST_ENTRY_SHOT_WINDOW_S = 5.0


def _secs_expr() -> pl.Expr:
    parts = pl.col("time_in_period").str.split(":")
    return parts.list.get(0).cast(pl.Int64) * 60 + parts.list.get(1).cast(pl.Int64)


def main() -> None:
    pbp = pl.read_parquet("tests/fixtures/nhl_microstat/pbp_2024_slice.parquet").with_columns(
        _secs_expr().alias("_secs")
    )
    tr = infer_zone_transitions(pbp)
    entries = tr.filter(pl.col("transition_type") == "entry").join(
        pbp.select(pl.col("game_id").cast(pl.Utf8), "event_idx", "period", "_secs"),
        on=["game_id", "event_idx"],
        how="left",
    )
    shots = pbp.filter(pl.col("type_desc_key").is_in(["goal", "shot-on-goal", "missed-shot", "blocked-shot"])).select(
        pl.col("game_id").cast(pl.Utf8),
        "period",
        pl.col("_secs").alias("shot_secs"),
        pl.col("event_owner_team_id").cast(pl.Utf8).alias("shot_team"),
    )
    joined = entries.join(shots, on=["game_id", "period"], how="left")
    joined = joined.with_columns(
        (
            (pl.col("shot_secs") >= pl.col("_secs"))
            & (pl.col("shot_secs") <= pl.col("_secs") + POST_ENTRY_SHOT_WINDOW_S)
            & (pl.col("shot_team") == pl.col("team_id"))
        ).alias("_shot_follows")
    )
    per_entry = joined.group_by(["game_id", "event_idx", "controlled"]).agg(
        pl.col("_shot_follows").any().alias("shot_follows")
    )
    print("per_entry height:", per_entry.height)
    n_controlled = per_entry.filter(pl.col("controlled") == True).height
    n_dump = per_entry.filter(pl.col("controlled") == False).height
    rate_controlled = per_entry.filter(pl.col("controlled") == True)["shot_follows"].mean()
    rate_dump = per_entry.filter(pl.col("controlled") == False)["shot_follows"].mean()
    print(f"n_controlled={n_controlled}, rate_controlled={rate_controlled:.4f}")
    print(f"n_dump={n_dump}, rate_dump={rate_dump:.4f}")
    print(f"diff={rate_controlled - rate_dump:.4f}")


if __name__ == "__main__":
    main()
