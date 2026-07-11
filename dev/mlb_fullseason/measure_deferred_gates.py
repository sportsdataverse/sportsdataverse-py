"""Full-season observed-correlation measurement for the deferred MLB T6.3
fielding/catching/baserunning oracle gates.

The offline oracle gates (``tests/mlb/test_mlb_*_oracle.py``) compare a
ONE-MONTH pitch/BIP fixture against the FULL-SEASON Savant leaderboards --
a scope mismatch that caps the observed correlation well below each model's
design target (framing 0.90, blocking/throwing/OAA 0.85, baserunning/SB
0.80). This script closes that gap: it pulls the full 2024 regular season
ONCE and correlates each model's full-season output against the same
committed leaderboards, like-for-like, so the coordinator can read the
observed floors off a log and finalize the ``@skip_if_no_live`` gates.

Each model is invoked EXACTLY as its offline oracle gate invokes it (same
function, same id casts, same oracle column) so the numbers are apples-to-
apples with those gates -- see the per-block comments citing each test.

Run (from repo root; network required -- pulls one full MLB season, ~55 min):

    SDV_PY_LIVE_TESTS=1 PYTHONIOENCODING=utf-8 uv run python dev/mlb_fullseason/measure_deferred_gates.py

The full-season pull is cached to ``dev/mlb_fullseason/_pitches_2024_full.parquet``
so re-runs (or a crash mid-measurement) don't re-hit Savant.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_baserunning import mlb_baserunning_value
from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking, mlb_catcher_throwing
from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
from sportsdataverse.mlb.mlb_fielding_oaa import mlb_fielding_oaa
from sportsdataverse.mlb.mlb_run_values import pearson_corr
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search
from sportsdataverse.mlb.mlb_stolen_base import mlb_stolen_base_value, sb_attempts_from_pitches

FIX = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "mlb_fielding"
CACHE = Path(__file__).parent / "_pitches_2024_full.parquet"

# 2024 MLB regular season (Opening Day .. Game 162), per the task brief.
SEASON_START, SEASON_END = "2024-03-28", "2024-09-29"


def _load_or_pull() -> pl.DataFrame:
    if CACHE.exists():
        print(f"Loading cached full-season pitches from {CACHE}", flush=True)
        return pl.read_parquet(CACHE)
    print(f"Pulling 2024 full regular season {SEASON_START}..{SEASON_END} (one pull) ...", flush=True)
    pitches = mlb_statcast_search(SEASON_START, SEASON_END, season=2024)
    print(f"  pulled {pitches.height} pitches", flush=True)
    pitches.write_parquet(CACHE)
    return pitches


def _report(label: str, mine_vals, sav_vals) -> None:
    r = pearson_corr(mine_vals, sav_vals)
    print(f"{label} full-season Pearson={r:.4f} n={len(mine_vals)}", flush=True)


def main() -> None:
    print("=== MLB T6.3 deferred-gate full-season measurement (2024) ===", flush=True)
    pitches = _load_or_pull()

    sprint = pl.read_parquet(FIX / "lb_sprint_speed_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )
    poptime = pl.read_parquet(FIX / "lb_poptime_2024.parquet").with_columns(
        pl.col("entity_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )

    # -- ① framing (mirror test_mlb_catcher_framing_oracle) -----------------
    mine = mlb_catcher_framing(pitches).filter(pl.col("takes") >= 500)
    sav = pl.read_parquet(FIX / "lb_catcher_framing_2024.parquet").with_columns(
        pl.col("id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    j = mine.join(sav.select("catcher_id", "rv_tot"), on="catcher_id", how="inner")
    _report("FRAMING", j["framing_runs"].to_numpy(), j["rv_tot"].to_numpy())

    # -- ③ OAA (mirror test_mlb_fielding_oaa_oracle; bip = type==X) ---------
    bip = pitches.filter(pl.col("type") == "X")
    mine = (
        mlb_fielding_oaa(bip)
        .group_by("fielder_id")
        .agg(pl.col("oaa").sum().alias("oaa"), pl.col("opportunities").sum().alias("opportunities"))
    )
    sav = pl.read_parquet(FIX / "lb_oaa_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("fielder_id")
    )
    j = mine.join(sav.select("fielder_id", "outs_above_average"), on="fielder_id", how="inner")
    _report("OAA", j["oaa"].to_numpy(), j["outs_above_average"].to_numpy())

    # -- ④ baserunning (mirror test_mlb_baserunning_oracle) -----------------
    mine = mlb_baserunning_value(pitches, sprint)
    sav = pl.read_parquet(FIX / "lb_baserunning_rv_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )
    j = mine.join(sav.select("runner_id", "runner_runs_tot"), on="runner_id", how="inner")
    _report("BASERUNNING", j["baserunning_runs"].to_numpy(), j["runner_runs_tot"].to_numpy())

    # -- ⑤ stolen base (mirror test_mlb_stolen_base_oracle) -----------------
    sb_attempts = sb_attempts_from_pitches(pitches)
    print(f"  sb_attempts detected (des-text): {sb_attempts.height}", flush=True)
    mine = mlb_stolen_base_value(sb_attempts, sprint, poptime)
    sav = pl.read_parquet(FIX / "lb_basestealing_rv_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("runner_id")
    )
    j = mine.join(sav.select("runner_id", "runs_stolen_on_running_act"), on="runner_id", how="inner")
    _report("STOLEN_BASE", j["sb_run_value"].to_numpy(), j["runs_stolen_on_running_act"].to_numpy())

    # -- ② catcher blocking (mirror test_mlb_catcher_defense_oracle) --------
    mine = mlb_catcher_blocking(pitches)
    sav = pl.read_parquet(FIX / "lb_catcher_blocking_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    j = mine.join(sav.select("catcher_id", "catcher_blocking_runs"), on="catcher_id", how="inner")
    _report("CATCHER_BLOCKING", j["blocking_runs"].to_numpy(), j["catcher_blocking_runs"].to_numpy())

    # -- ② catcher throwing (mirror test_mlb_catcher_defense_oracle) --------
    mine = mlb_catcher_throwing(sb_attempts)
    sav = pl.read_parquet(FIX / "lb_catcher_throwing_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    j = mine.join(sav.select("catcher_id", "catcher_stealing_runs"), on="catcher_id", how="inner")
    _report("CATCHER_THROWING", j["throwing_runs"].to_numpy(), j["catcher_stealing_runs"].to_numpy())

    print("=== done -- set each FLOOR_* from the observed Pearson above ===", flush=True)


if __name__ == "__main__":
    main()
