"""Bridge PFF NCAA team grades -> ESPN team_id and report the cfb_ratings correlation.

Reads the captured teams_overview payload, parses via the shipped pff parser,
name-matches to ESPN team_info (via the crosswalk normalizer), writes the
committed oracle fixture, and prints observed Spearman vs the committed pbp
ratings so the gate floor can be set from the data.
"""

from __future__ import annotations
import json
import pathlib
import polars as pl
from sportsdataverse.nfl.pff_parsers import parse_pff_report
from sportsdataverse.cfb.cfb_crosswalk import _norm_team
from sportsdataverse.cfb.cfb_loaders import load_cfb_team_info
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
from sportsdataverse.cfb.cfb_prediction_constants import spearman_corr

HERE = pathlib.Path(__file__).parent
FIX = HERE.parent.parent / "tests" / "fixtures" / "cfb_pff"
SRC = FIX / "teams_overview_ncaa_2023.json"  # tracked rebuild input (committed alongside the parquet)
with open(SRC, encoding="utf-8") as _f:
    raw = json.load(_f)
pff = (
    parse_pff_report(raw)
    .select(
        pl.col("name").cast(pl.Utf8),
        pl.col("grades_overall").cast(pl.Float64).alias("pff_overall"),
        pl.col("grades_offense").cast(pl.Float64).alias("pff_offense"),
        pl.col("grades_defense").cast(pl.Float64).alias("pff_defense"),
    )
    .with_columns(pl.col("name").map_elements(_norm_team, return_dtype=pl.Utf8).alias("k"))
)

ti = load_cfb_team_info(2023)
tk = (
    ti.select(
        (pl.col("school").cast(pl.Utf8) + " " + pl.col("mascot").cast(pl.Utf8).fill_null(""))
        .map_elements(_norm_team, return_dtype=pl.Utf8)
        .alias("k"),
        pl.col("team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        pl.col("classification").cast(pl.Utf8),
    )
    .drop_nulls()
    .unique(subset=["k"])
)

merged = pff.join(tk, on="k", how="inner").filter(pl.col("classification") == "fbs")
out = merged.select("team_id", "pff_overall", "pff_offense", "pff_defense").unique(subset=["team_id"])
print(f"PFF 479 -> FBS name-matched to ESPN id: {out.height} teams")

FIX.mkdir(parents=True, exist_ok=True)
out.write_parquet(FIX / "pff_team_grades_2023.parquet")

# observed correlation vs the committed pbp ratings sample (if present)
pbp_fp = HERE.parent.parent / "tests" / "fixtures" / "cfb_prediction" / "pbp_2023_sample.parquet"
if pbp_fp.exists():
    e = efficiency_ratings(pl.read_parquet(pbp_fp)).join(out, on="team_id", how="inner")
    print(f"joined to ratings: {e.height} teams")
    for a, b in (("adj_net", "pff_overall"), ("adj_off_epa", "pff_offense"), ("adj_def_epa", "pff_defense")):
        r = spearman_corr(e[a].to_numpy(), e[b].to_numpy())
        print(f"  spearman({a}, {b}) = {r:.4f}")
