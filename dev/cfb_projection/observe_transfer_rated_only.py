"""Diagnostic: net transfer talent counting only star-RATED movers (default 0)."""

from __future__ import annotations

import importlib

import polars as pl

from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import spearman_corr

imp = importlib.import_module("sportsdataverse.cfb.cfb_transfer_impact")
FIX = "tests/fixtures/cfb_projection"
recruits = pl.read_parquet(f"{FIX}/recruits_2014_2023.parquet")
imp.load_recruit_classes = lambda seasons, **k: recruits.filter(
    pl.col("season").is_in([seasons] if isinstance(seasons, int) else list(seasons))
)

moves = imp.cfb_transfer_moves(list(range(2018, 2024)))
# rated movers = name-matched to a recruit record with stars>=3 (points > default 20)
rated = moves.filter(pl.col("talent_points") > 20.0)
print("moves:", moves.height, "| rated movers:", rated.height)
signed = rated.with_columns(
    pl.when(pl.col("direction") == "in").then(pl.col("talent_points")).otherwise(-pl.col("talent_points")).alias("sp")
)
net = signed.group_by("season", "team_id").agg(pl.col("sp").sum().alias("net_rated"))

teams = pl.read_parquet(f"{FIX}/teams_2023.parquet")
res = pl.read_parquet(f"{FIX}/results_2016_2023.parquet")
tk = teams.with_columns(pl.col("school").map_elements(_norm_team, return_dtype=pl.Utf8).alias("k")).select(
    "k", pl.col("team_id").alias("eid"), "classification"
)
netk = (
    net.with_columns(pl.col("team_id").map_elements(_norm_team, return_dtype=pl.Utf8).alias("k"))
    .join(tk, on="k", how="inner")
    .filter(pl.col("classification") == "fbs")
)

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
    .with_columns((pl.col("wins") - pl.col("pw")).cast(pl.Float64).alias("wd"))
)

j = netk.join(delta, on=["season", "eid"], how="inner")
for label, jj in (("2018-2023", j), ("portal era 2021+", j.filter(pl.col("season") >= 2021))):
    r = spearman_corr(jj["net_rated"].to_numpy(), jj["wd"].to_numpy())
    q = jj.with_columns((pl.col("net_rated").rank("average") / pl.len()).alias("pct"))
    top = q.filter(pl.col("pct") >= 0.75)["wd"].mean()
    bot = q.filter(pl.col("pct") <= 0.25)["wd"].mean()
    print(f"{label}: n={jj.height} spearman={r:.4f} top-q {top:.3f} vs bottom-q {bot:.3f}")
