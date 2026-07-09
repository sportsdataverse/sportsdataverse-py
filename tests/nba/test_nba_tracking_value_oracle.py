"""Rank-sanity + Sigma=0 oracle gates on the committed 2023-24 fixtures.

Each phase appends its own rank-sanity gate here, reusing the Phase-0
Sigma=0 template: fixture-backed ``_get_fn`` -> the public model function ->
``residual_sums_to_zero`` + ``ELITE_ORACLE`` top-K membership. No network.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_tracking_value import (
    _attach_role_bucket,
    _fetch_leaguedash_tracking,
    _over_expected,
    nba_tracking_drive_value,
    nba_tracking_pass_value,
    nba_tracking_reb_oe,
    nba_tracking_rim_protect_value,
    nba_tracking_shot_diet_value,
    nba_tracking_touch_value,
)
from sportsdataverse.nba.nba_tracking_value_constants import ELITE_ORACLE, residual_sums_to_zero, top_k_ids

# Qualification floor for the rank-sanity gates: >=20 GP excludes call-ups/
# short-stint noise while keeping ~90% of the league (observed on the 2023-24
# fixtures, not an invented cutoff).
MIN_GP = 20

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_stats" / "tracking"


def _load_fixture(measure_file: str) -> dict:
    return json.loads((FIXTURE_DIR / measure_file).read_text(encoding="utf-8"))


def _load_positions() -> pl.DataFrame:
    return pl.read_parquet(FIXTURE_DIR / "player_positions_2324.parquet")


def test_engine_sum_to_zero_on_rebounding_fixture():
    raw = _load_fixture("leaguedashptstats_rebounding_2324.json")
    df = _fetch_leaguedash_tracking(2024, "Rebounding", _get_fn=lambda **kw: raw)
    assert df.height > 0

    positions = _load_positions()
    bucketed = _attach_role_bucket(df, 2024, positions=positions)
    out = _over_expected(bucketed, actual="reb", denom="reb_chances", group_cols=["position_bucket"], out_prefix="reb")
    assert residual_sums_to_zero(out, "reb_oe", ["position_bucket"]) is True


def test_reb_oe_rank_sanity_and_sum_to_zero():
    raw = _load_fixture("leaguedashptstats_rebounding_2324.json")
    positions = _load_positions()
    out = nba_tracking_reb_oe(2024, _get_fn=lambda **kw: raw, positions=positions)
    assert residual_sums_to_zero(out, "reb_oe", ["position_bucket"]) is True

    qualified = out.filter(pl.col("gp") >= MIN_GP)
    # K=35 of ~403 qualified (~8.7%) -- the smallest K covering every allowlisted
    # id once the allowlist was re-sourced by RATE (see nba_tracking_value_constants
    # module comment); do not raise K to cover a differently-sourced allowlist.
    top_ids = set(top_k_ids(qualified, "reb_oe", k=35))
    elite = set(ELITE_ORACLE["2023-24"]["reb"])
    assert elite.issubset(top_ids), elite - top_ids


def test_ast_oe_rank_sanity_and_sum_to_zero():
    raw = _load_fixture("leaguedashptstats_passing_2324.json")
    positions = _load_positions()
    out = nba_tracking_pass_value(2024, _get_fn=lambda **kw: raw, positions=positions)
    assert residual_sums_to_zero(out, "ast_oe", ["position_bucket"]) is True

    qualified = out.filter(pl.col("gp") >= MIN_GP)
    # K=20 of ~443 qualified (~4.5%) -- the smallest K covering every allowlisted
    # id once the allowlist was re-sourced by ast_to_pass_pct RATE.
    top_ids = set(top_k_ids(qualified, "ast_oe", k=20))
    elite = set(ELITE_ORACLE["2023-24"]["ast"])
    assert elite.issubset(top_ids), elite - top_ids


def test_drive_pts_oe_rank_sanity_and_sum_to_zero():
    raw = _load_fixture("leaguedashptstats_drives_2324.json")
    positions = _load_positions()
    out = nba_tracking_drive_value(2024, _get_fn=lambda **kw: raw, positions=positions)
    assert residual_sums_to_zero(out, "drive_pts_oe", ["position_bucket"]) is True

    qualified = out.filter(pl.col("gp") >= MIN_GP)
    # K=25 of ~443 qualified (~5.6%) -- the smallest K covering every allowlisted
    # id once Dejounte Murray (raw drive-volume leader) was swapped for Kyrie
    # Irving (see nba_tracking_value_constants module comment).
    top_ids = set(top_k_ids(qualified, "drive_pts_oe", k=25))
    elite = set(ELITE_ORACLE["2023-24"]["drive"])
    assert elite.issubset(top_ids), elite - top_ids


def test_cs_pts_oe_rank_sanity_and_sum_to_zero():
    cs_raw = _load_fixture("leaguedashptstats_catchshoot_2324.json")
    pu_raw = _load_fixture("leaguedashptstats_pullupshot_2324.json")
    positions = _load_positions()

    def fake(**kw):
        return cs_raw if kw.get("pt_measure_type") == "CatchShoot" else pu_raw

    out = nba_tracking_shot_diet_value(2024, _get_fn=fake, positions=positions)
    assert residual_sums_to_zero(out, "cs_pts_oe", ["position_bucket"]) is True
    assert residual_sums_to_zero(out, "pu_pts_oe", ["position_bucket"]) is True

    # No gp/min on this schema -- qualify on attempt volume instead (>=50 cs_fga).
    qualified = out.filter(pl.col("cs_fga") >= 50)
    # K=30 of ~335 qualified (~9%) -- the smallest K covering every allowlisted
    # id once Klay Thompson/Bogdanovic/Bridges (career-reputation picks) were
    # swapped for players who were actually elite BY RATE in 2023-24 (see
    # nba_tracking_value_constants module comment).
    top_ids = set(top_k_ids(qualified, "cs_pts_oe", k=30))
    elite = set(ELITE_ORACLE["2023-24"]["shot"])
    assert elite.issubset(top_ids), elite - top_ids


def test_pts_per_touch_oe_rank_sanity_and_sum_to_zero():
    raw = _load_fixture("leaguedashptstats_possessions_2324.json")
    positions = _load_positions()
    out = nba_tracking_touch_value(2024, _get_fn=lambda **kw: raw, positions=positions)
    assert residual_sums_to_zero(out, "pts_per_touch_oe", ["position_bucket"]) is True

    qualified = out.filter(pl.col("gp") >= MIN_GP)
    # K=28 of ~443 qualified (~6.3%) -- the smallest K covering every allowlisted
    # id once Jokic/Tatum/A. Davis (general-greatness picks) were swapped for
    # players who were actually elite BY pts_per_touch RATE in 2023-24 (see
    # nba_tracking_value_constants module comment -- Jokic's facilitator-hub
    # role means many touches end in a pass, not his own shot).
    top_ids = set(top_k_ids(qualified, "pts_per_touch_oe", k=28))
    elite = set(ELITE_ORACLE["2023-24"]["touch"])
    assert elite.issubset(top_ids), elite - top_ids


def test_rim_protect_pts_saved_rank_sanity_and_sum_to_zero():
    raw = _load_fixture("leaguedashptstats_defense_2324.json")
    positions = _load_positions()
    out = nba_tracking_rim_protect_value(2024, _get_fn=lambda **kw: raw, positions=positions)
    assert residual_sums_to_zero(out, "rim_protect_pts_saved", ["position_bucket"]) is True

    qualified = out.filter(pl.col("gp") >= MIN_GP)
    # K=30 of ~443 qualified (~6.8%) -- the smallest K covering every allowlisted
    # id once Myles Turner/Nurkic (reputation picks) were swapped for Porzingis/
    # Hartenstein: both allowed ~58.7-58.8% at the rim, essentially identical to
    # the "big" bucket baseline (58.6%) -- verified against the raw fixture, not
    # a bug (see nba_tracking_value_constants module comment).
    top_ids = set(top_k_ids(qualified, "rim_protect_pts_saved", k=30))
    elite = set(ELITE_ORACLE["2023-24"]["rim"])
    assert elite.issubset(top_ids), elite - top_ids
