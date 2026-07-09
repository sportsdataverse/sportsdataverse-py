"""Observe Task-3.3 backtest numbers (offline, fixture-driven) to set the gate floor."""

from __future__ import annotations

import numpy as np
import polars as pl

import importlib

proj = importlib.import_module("sportsdataverse.cfb.cfb_recruiting_projection")
tal_mod = importlib.import_module("sportsdataverse.cfb.cfb_roster_talent")
from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import mae

FIX = "tests/fixtures/cfb_projection"
recruits = pl.read_parquet(f"{FIX}/recruits_2014_2023.parquet")
teams = pl.read_parquet(f"{FIX}/teams_2023.parquet")
returning = pl.read_parquet(f"{FIX}/returning_2017_2023.parquet")
results_g = pl.read_parquet(f"{FIX}/results_2016_2023.parquet")

tal_mod.load_recruit_classes = lambda seasons, **k: recruits.filter(
    pl.col("season").is_in([seasons] if isinstance(seasons, int) else list(seasons))
)

name_map = teams.with_columns(
    (pl.col("school") + " " + pl.col("mascot").fill_null(""))
    .map_elements(_norm_team, return_dtype=pl.Utf8)
    .alias("_full")
).select("_full", pl.col("team_id").alias("espn_id"), "classification")


def load_talent(seasons, division):
    t = tal_mod.cfb_roster_talent(seasons, division=division)
    return (
        t.with_columns(pl.col("team").map_elements(_norm_team, return_dtype=pl.Utf8).alias("_full"))
        .join(name_map, on="_full", how="inner")
        .filter(pl.col("classification") == "fbs")
        .drop("team_id", "_full", "classification")
        .rename({"espn_id": "team_id"})
    )


def load_returning(seasons, division):
    return returning.filter(pl.col("season").is_in(list(seasons)) & pl.col("team_id").is_not_null()).select(
        "season", "team_id", "off_returning", "def_returning"
    )


def load_results(seasons):
    done = results_g.filter(pl.col("season").is_in(list(seasons)))
    home = done.select(
        "season",
        pl.col("home_team_id").alias("team_id"),
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("win"),
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("m"),
    )
    away = done.select(
        "season",
        pl.col("away_team_id").alias("team_id"),
        (pl.col("away_score") > pl.col("home_score")).cast(pl.Int64).alias("win"),
        (pl.col("away_score") - pl.col("home_score")).cast(pl.Float64).alias("m"),
    )
    return (
        pl.concat([home, away])
        .group_by("season", "team_id")
        .agg(pl.col("win").sum().alias("wins"), pl.col("m").mean().alias("points_margin"))
    )


proj._load_talent = load_talent
proj._load_returning = load_returning
proj._load_results = load_results

rows = []
for target in range(2019, 2024):
    out = proj.cfb_recruiting_projection(target, history_seasons=list(range(2018, target)))
    realized = load_results([target]).rename({"wins": "real_wins"})
    j = out.join(realized, on=["season", "team_id"], how="inner")
    matrix = proj._build_projection_matrix(list(range(2018, target + 1)))
    prior = matrix.filter(pl.col("season") == target).select("team_id", "prior_wins")
    j = j.join(prior, on="team_id", how="left").drop_nulls(["prior_wins"])
    m_model = mae(j["pred_wins"].to_numpy(), j["real_wins"].to_numpy())
    m_prior = mae(j["prior_wins"].to_numpy(), j["real_wins"].to_numpy())
    m_mean = mae(np.full(j.height, 6.0), j["real_wins"].to_numpy())
    rows.append((target, j.height, m_model, m_prior, m_mean))
    print(f"{target}: n={j.height} model={m_model:.3f} prior={m_prior:.3f} mean={m_mean:.3f}")

n = sum(r[1] for r in rows)


def w(i: int) -> float:
    return sum(r[1] * r[i] for r in rows) / n


print(f"POOLED: n={n} model={w(2):.4f} prior={w(3):.4f} mean={w(4):.4f}")
