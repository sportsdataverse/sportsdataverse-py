"""Observe Task-5.3 draft-projection gate numbers (offline, fixture-driven)."""

from __future__ import annotations

import importlib

import polars as pl

from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_projection_constants import roc_auc, spearman_corr

proj = importlib.import_module("sportsdataverse.cfb.cfb_draft_projection")
tal_mod = importlib.import_module("sportsdataverse.cfb.cfb_roster_talent")

FIX = "tests/fixtures/cfb_projection"
recruits = pl.read_parquet(f"{FIX}/recruits_2014_2023.parquet")
draft = pl.read_parquet(f"{FIX}/draft_2017_2024.parquet")
teams = pl.read_parquet(f"{FIX}/teams_2023.parquet")

tal_mod.load_recruit_classes = lambda seasons, **k: recruits.filter(
    pl.col("season").is_in([seasons] if isinstance(seasons, int) else list(seasons))
)
proj.load_draft_outcomes = lambda years, **k: draft.filter(
    pl.col("draft_year").is_in([years] if isinstance(years, int) else list(years))
)
production = pl.read_parquet(f"{FIX}/player_production_2016_2023.parquet")
proj._season_production = lambda season: production.filter(pl.col("season") == season)

out = proj.cfb_draft_projection(2024)
players, team_proj = out["players"], out["teams"]
print("players:", players.height, "teams:", team_proj.height)

# per-player AUC on the target year (labels via the same name match the trainer used)
feat = proj._player_feature_frame([2024], "fbs")
j = players.join(feat.select("player_id", "drafted"), on="player_id", how="inner")
auc = roc_auc(j["drafted"].to_numpy(), j["draft_prob"].to_numpy())
print(f"player AUC 2024: {auc:.4f}  (drafted in pool: {j['drafted'].sum()})")

# blue-chip sanity
bc = players.join(
    recruits.select(pl.col("recruit_id").alias("player_id"), "stars"), on="player_id", how="left"
).with_columns((pl.col("stars") >= 4).alias("blue"))
means = bc.group_by("blue").agg(pl.col("draft_prob").mean()).sort("blue")
print("mean prob by blue-chip:", means.to_dicts())


# team-level: realized picks per college (PFR name -> school key -> 247 full-name key)
def _pfr_school(c: str) -> str:
    c = c.replace("St.", "State").replace("st.", "State")
    return _norm_team(c)


teams_k = teams.with_columns(
    pl.col("school").map_elements(_norm_team, return_dtype=pl.Utf8).alias("school_key"),
    (pl.col("school") + " " + pl.col("mascot").fill_null(""))
    .map_elements(_norm_team, return_dtype=pl.Utf8)
    .alias("full_key"),
)
realized = (
    draft.filter(pl.col("draft_year") == 2024)
    .with_columns(pl.col("college").map_elements(_pfr_school, return_dtype=pl.Utf8).alias("school_key"))
    .join(teams_k.select("school_key", "team_id"), on="school_key", how="left")
)
print("pick->espn match:", realized.filter(pl.col("team_id").is_not_null()).height, "of", realized.height)
real_counts = realized.drop_nulls("team_id").group_by("team_id").agg(pl.len().alias("realized_picks"))

# proj teams (247 key) -> espn id via recruit team full name
rec_names = (
    recruits.select(pl.col("team_id"), pl.col("team"))
    .unique(subset=["team_id"])
    .with_columns(pl.col("team").map_elements(_norm_team, return_dtype=pl.Utf8).alias("full_key"))
)
tp = (
    team_proj.join(rec_names, on="team_id", how="left")
    .join(
        teams_k.select("full_key", pl.col("team_id").alias("espn_id"), "classification"),
        on="full_key",
        how="left",
    )
    .drop_nulls("espn_id")
    .filter(pl.col("classification") == "fbs")
)
tj = tp.join(real_counts, left_on="espn_id", right_on="team_id", how="left").with_columns(
    pl.col("realized_picks").fill_null(0)
)
rho = spearman_corr(tj["proj_draft_picks"].to_numpy(), tj["realized_picks"].to_numpy().astype(float))
print(f"team spearman 2024 (final-college attribution): {rho:.4f} (n={tj.height})")

# apples-to-apples: realized picks attributed to the SIGNING school (same pool)
feat24 = proj._player_feature_frame([2024], "fbs")
sign_real = feat24.group_by("team_id").agg(pl.col("drafted").sum().alias("realized_signing"))
tp2 = team_proj.join(sign_real, on="team_id", how="left").with_columns(pl.col("realized_signing").fill_null(0))
tp2 = (
    tp2.join(rec_names, on="team_id", how="left")
    .join(teams_k.select("full_key", "classification"), on="full_key", how="left")
    .filter(pl.col("classification") == "fbs")
)
rho2 = spearman_corr(tp2["proj_draft_picks"].to_numpy(), tp2["realized_signing"].to_numpy().astype(float))
print(f"team spearman 2024 (signing-school attribution): {rho2:.4f} (n={tp2.height})")


# multi-year pooled team gate (2022-2024) + per-year

all_pairs = []
for target in (2022, 2023, 2024):
    o = proj.cfb_draft_projection(target)
    t = o["teams"]
    realized_y = (
        draft.filter(pl.col("draft_year") == target)
        .with_columns(pl.col("college").map_elements(_pfr_school, return_dtype=pl.Utf8).alias("school_key"))
        .join(teams_k.select("school_key", "team_id"), on="school_key", how="left")
        .drop_nulls("team_id")
        .group_by("team_id")
        .agg(pl.len().alias("realized_picks"))
    )
    tpx = (
        t.join(rec_names, on="team_id", how="left")
        .join(
            teams_k.select("full_key", pl.col("team_id").alias("espn_id"), "classification"),
            on="full_key",
            how="left",
        )
        .drop_nulls("espn_id")
        .filter(pl.col("classification") == "fbs")
        .join(realized_y, left_on="espn_id", right_on="team_id", how="left")
        .with_columns(pl.col("realized_picks").fill_null(0))
    )
    r = spearman_corr(tpx["proj_draft_picks"].to_numpy(), tpx["realized_picks"].to_numpy().astype(float))
    print(f"  {target}: spearman {r:.4f} (n={tpx.height})")
    all_pairs.append(tpx.select("proj_draft_picks", "realized_picks"))
pooled = pl.concat(all_pairs)
rp_ = spearman_corr(pooled["proj_draft_picks"].to_numpy(), pooled["realized_picks"].to_numpy().astype(float))
print(f"POOLED 2022-2024 team spearman: {rp_:.4f} (n={pooled.height})")


for target in (2022, 2023, 2024):
    o = proj.cfb_draft_projection(target)
    f = proj._player_feature_frame([target], "fbs")
    jj = o["players"].join(f.select("player_id", "drafted"), on="player_id", how="inner")
    print(
        f"  AUC {target}: {roc_auc(jj['drafted'].to_numpy(), jj['draft_prob'].to_numpy()):.4f} (drafted {jj['drafted'].sum()})"
    )
