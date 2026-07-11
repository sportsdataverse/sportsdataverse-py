"""Full-season live oracle gates for the T6.3 fielding/catching models.

These pull the **full 2024 regular season** pitch-by-pitch from Baseball
Savant once and correlate each model's full-season output against the
committed full-season leaderboards, like-for-like -- closing the month-vs-season
scope gap the offline gates carry (see each offline test's docstring). Gated by
``@skip_if_no_live`` (env ``SDV_PY_LIVE_TESTS=1``); the pull takes ~1 hour, so
these run only when a contributor explicitly enables the live suite.

FLOORS are the **observed full-season Pearson**, rounded down with margin --
NOT the design targets (which need Savant's proprietary tracking data this
public per-pitch feed lacks). Observed values from
``dev/mlb_fullseason/measure_deferred_gates.py`` on the 2024 season
(710,878 pitches):

    OAA               Pearson 0.605  (n=272)  -> floor 0.55
    FRAMING           Pearson 0.468  (n=44)   -> floor 0.40
    CATCHER_THROWING  Pearson 0.073  (n=52)   -> floor 0.03

The framing/OAA ceilings are feature-capped: the public feed has no pitch
movement / release / receiving (framing) nor fielder start coordinates (OAA).
Catcher-throwing is data-capped: only ~401 of ~1773 real SB/CS attempts are
narrated in the ``des`` text this feed exposes (``events`` carries none), so
per-catcher samples are thin -- the model's pop-time self-cancellation bug is
fixed (it read ~-0.01 before), leaving a small but clearly positive signal.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_throwing
from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
from sportsdataverse.mlb.mlb_fielding_oaa import mlb_fielding_oaa
from sportsdataverse.mlb.mlb_run_values import pearson_corr
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search
from sportsdataverse.mlb.mlb_stolen_base import sb_attempts_from_pitches
from tests.conftest import skip_if_no_live

FIXTURE_DIR = "tests/fixtures/mlb_fielding"
SEASON_START, SEASON_END = "2024-03-28", "2024-09-29"


@skip_if_no_live
def test_fielding_models_full_season_concurrent_validity_live() -> None:
    pitches = mlb_statcast_search(SEASON_START, SEASON_END, season=2024)
    assert pitches.height > 500_000, f"full-season pull returned only {pitches.height} pitches"

    # -- OAA: per-position catch-probability logistic vs Savant OAA -----------
    bip = pitches.filter(pl.col("type") == "X")
    oaa = (
        mlb_fielding_oaa(bip)
        .group_by("fielder_id")
        .agg(pl.col("oaa").sum().alias("oaa"), pl.col("opportunities").sum().alias("opportunities"))
    )
    sav_oaa = pl.read_parquet(f"{FIXTURE_DIR}/lb_oaa_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("fielder_id")
    )
    j = oaa.join(sav_oaa.select("fielder_id", "outs_above_average"), on="fielder_id", how="inner")
    assert j.height >= 200, f"OAA join too sparse: {j.height}"
    r_oaa = pearson_corr(j["oaa"].to_numpy(), j["outs_above_average"].to_numpy())
    assert r_oaa >= 0.55, f"OAA full-season corr {r_oaa:.3f} < 0.55 (observed 0.605)"

    # -- FRAMING: smooth logistic + shadow zone vs Savant rv_tot --------------
    framing = mlb_catcher_framing(pitches).filter(pl.col("takes") >= 500)
    sav_fr = pl.read_parquet(f"{FIXTURE_DIR}/lb_catcher_framing_2024.parquet").with_columns(
        pl.col("id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    jf = framing.join(sav_fr.select("catcher_id", "rv_tot"), on="catcher_id", how="inner")
    assert jf.height >= 30, f"framing join too sparse: {jf.height}"
    r_fr = pearson_corr(jf["framing_runs"].to_numpy(), jf["rv_tot"].to_numpy())
    assert r_fr >= 0.40, f"framing full-season corr {r_fr:.3f} < 0.40 (observed 0.468)"

    # -- CATCHER_THROWING: caught-stealing above per-base baseline ------------
    sb_attempts = sb_attempts_from_pitches(pitches)
    throwing = mlb_catcher_throwing(sb_attempts)
    sav_th = pl.read_parquet(f"{FIXTURE_DIR}/lb_catcher_throwing_2024.parquet").with_columns(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8).alias("catcher_id")
    )
    jt = throwing.join(sav_th.select("catcher_id", "catcher_stealing_runs"), on="catcher_id", how="inner")
    assert jt.height >= 30, f"throwing join too sparse: {jt.height}"
    r_th = pearson_corr(jt["throwing_runs"].to_numpy(), jt["catcher_stealing_runs"].to_numpy())
    # Data-capped (see module docstring); the bug fix moved this from ~-0.01 to clearly positive.
    assert r_th >= 0.03, f"catcher-throwing full-season corr {r_th:.3f} < 0.03 (observed 0.073)"
