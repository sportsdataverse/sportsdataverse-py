from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value
from sportsdataverse.nhl.nhl_microstat_constants import spearman_corr

edge = pl.read_parquet("tests/fixtures/nhl_microstat/edge_skater_detail_sample.parquet")

for method in ("zscore", "percentile"):
    for izb in (False, True):
        out = nhl_edge_skating_value(season=2024, detail_frames=edge, method=method, include_zone_balance=izb)
        print(f"method={method} include_zone_balance={izb} height={out.height} cols={out.columns}")
        for comp in ("top_speed", "distance_km", "speed_bursts_20", "oz_time_pct"):
            corr = spearman_corr(out["skating_value"].to_numpy(), out[comp].to_numpy())
            print(f"  corr(skating_value, {comp}) = {corr:.4f}")

# Joint face-validity: top-decile skating_value (default zscore, no zone balance)
# players should also be jointly high (not just marginally correlated) in BOTH
# top_speed AND distance_km.
out = nhl_edge_skating_value(season=2024, detail_frames=edge)
n = out.height
decile_n = max(1, round(n * 0.1))
top_decile = out.sort("skating_value", descending=True).head(decile_n)
pct = out.select(
    "player_id",
    (pl.col("top_speed").rank(method="average") - 1) / (n - 1),
    (pl.col("distance_km").rank(method="average") - 1) / (n - 1),
).rename({"top_speed": "top_speed_pctile", "distance_km": "distance_km_pctile"})
joined = top_decile.join(pct, on="player_id")
joined = joined.with_columns(pl.min_horizontal("top_speed_pctile", "distance_km_pctile").alias("joint_min_pctile"))
print("\ntop-decile n:", decile_n)
print("joint_min_pctile mean:", joined["joint_min_pctile"].mean())
print("joint_min_pctile min:", joined["joint_min_pctile"].min())
print(joined.select("player_id", "top_speed_pctile", "distance_km_pctile", "joint_min_pctile"))
