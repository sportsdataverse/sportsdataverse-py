"""Observe Task-4.3 transfer-impact gate numbers (offline, fixture-driven)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import spearman_corr

FIX = "tests/fixtures/cfb_projection"
net = pl.read_parquet(f"{FIX}/net_transfer_2018_2023.parquet")
teams = pl.read_parquet(f"{FIX}/teams_2023.parquet")
res = pl.read_parquet(f"{FIX}/results_2016_2023.parquet")

tk = teams.with_columns(pl.col("school").map_elements(_norm_team, return_dtype=pl.Utf8).alias("school_key")).select(
    "school_key", pl.col("team_id").alias("espn_id"), "classification"
)
netk = net.with_columns(pl.col("team_id").map_elements(_norm_team, return_dtype=pl.Utf8).alias("school_key")).join(
    tk, on="school_key", how="inner"
)
print("mapped:", netk.height, "of", net.height, "| fbs:", netk.filter(pl.col("classification") == "fbs").height)

home = res.select(
    "season",
    pl.col("home_team_id").alias("eid"),
    (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("w"),
)
away = res.select(
    "season",
    pl.col("away_team_id").alias("eid"),
    (pl.col("away_score") > pl.col("home_score")).cast(pl.Int64).alias("w"),
)
wins = pl.concat([home, away]).group_by("season", "eid").agg(pl.col("w").sum().alias("wins"), pl.len().alias("g"))
delta = (
    wins.join(
        wins.with_columns((pl.col("season") + 1).alias("season")).rename({"wins": "pw", "g": "pg"}),
        on=["season", "eid"],
        how="inner",
    )
    .filter((pl.col("g") >= 6) & (pl.col("pg") >= 6))
    .with_columns((pl.col("wins") - pl.col("pw")).cast(pl.Float64).alias("win_delta"))
)

j = netk.filter(pl.col("classification") == "fbs").join(
    delta, left_on=["season", "espn_id"], right_on=["season", "eid"], how="inner"
)
print("joined FBS team-seasons:", j.height)
rho = spearman_corr(j["net_transfer_talent"].to_numpy(), j["win_delta"].to_numpy())
print(f"spearman(net_transfer_talent, win_delta): {rho:.4f}")
q = j.with_columns((pl.col("net_transfer_talent").rank("average") / pl.len()).alias("pct"))
top = q.filter(pl.col("pct") >= 0.75)["win_delta"].mean()
bot = q.filter(pl.col("pct") <= 0.25)["win_delta"].mean()
print(f"direction: top-quartile mean win_delta {top:.3f} vs bottom {bot:.3f}")
# portal era only (2021+)
j21 = j.filter(pl.col("season") >= 2021)
rho21 = spearman_corr(j21["net_transfer_talent"].to_numpy(), j21["win_delta"].to_numpy())
print(f"portal-era (2021+) spearman: {rho21:.4f} (n={j21.height})")
