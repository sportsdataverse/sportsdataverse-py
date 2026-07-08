"""Oracle gates: cfb_advanced_stats vs CFBD advanced stats + SP+ (2021, offline).

Fixture-backed (tests/fixtures/cfb_advanced/, provenance in its README).
Season 2021 because the hosted load_cfb_pbp parquet covers 2002-2021 only
(2022+ 404s -- cfb-data producer backfill escalation).

Gate floors follow the "never lower a gate -- debug, then floor from the
observed value" rule; per-gate findings are documented on each test.
"""

import polars as pl
import pytest

import sportsdataverse.cfb.cfb_advanced_stats as m
from sportsdataverse.cfb.cfb_advanced_constants import mae, spearman_corr

FIX = "tests/fixtures/cfb_advanced"


@pytest.fixture(scope="module")
def joined():
    """cfb_advanced_stats([2021]) built from the committed pbp slice, joined to CFBD + SP+."""
    pbp = pl.read_parquet(f"{FIX}/pbp_slice_2021.parquet")
    orig = m.load_cfb_pbp
    m.load_cfb_pbp = lambda s, **k: pbp
    try:
        out = m.cfb_advanced_stats([2021])
    finally:
        m.load_cfb_pbp = orig
    cfbd = pl.read_parquet(f"{FIX}/cfbd_advanced_2021.parquet")
    sp = pl.read_parquet(f"{FIX}/sp_plus_2021.parquet")
    assert out.schema["team_id"] == cfbd.schema["team_id"] == sp.schema["team_id"]
    j = out.join(cfbd, on="team_id", how="inner")
    js = out.join(sp, on="team_id", how="inner")
    # match-rate floor: every CFBD FBS team must join
    assert j.height == cfbd.height == 130
    assert js.height == sp.height == 130
    return j, js


def test_success_rate_fidelity(joined):
    """Raw success-rate fidelity vs CFBD (same 50/70/100 definition).

    Observed 2021: off spear 0.9541 / MAE 0.0284; def spear 0.9591 / MAE
    0.0314. MAE floor set from observed (plan aspiration was 0.02): CFBD
    filters garbage time with its own thresholds (not Connelly's 43/37/27/21)
    and counts plays slightly differently, so a ~0.03 level offset remains
    while the ordering agrees at 0.95+.
    """
    j, _ = joined
    for side in ("off", "def"):
        mine = j[f"{side}_success_rate"].to_numpy()
        oracle = j[f"{side}_success_rate_right"].to_numpy()
        assert spearman_corr(mine, oracle) >= 0.95
        assert mae(mine, oracle) <= 0.035


def test_epa_play_fidelity(joined):
    """EPA/play vs CFBD PPA. Observed 2021: off 0.8943, def 0.8514.

    Floor 0.85/0.80: our EPA comes from the ESPN-EP-based cfbfastR model,
    CFBD PPA from their own EP model -- the two base metrics themselves only
    correlate ~0.89, an EP-model divergence, not a formula bug (success rate,
    which is model-free, hits 0.95+ on the identical substrate).
    """
    j, _ = joined
    assert spearman_corr(j["off_epa_play"].to_numpy(), j["off_ppa"].to_numpy()) >= 0.85
    assert spearman_corr(j["def_epa_play"].to_numpy(), j["def_ppa"].to_numpy()) >= 0.80


def test_iso_ppp_fidelity(joined):
    """isoPPP (mean EPA on successful plays) vs CFBD off_explosiveness.

    Observed 2021: 0.7199. Floor 0.70: isoPPP is a tail statistic of the EP
    model -- since the base EPA-vs-PPA correlation is already ~0.89
    (different EP models), the successful-play tail mean diverges further.
    Debugged: garbage-time handling is not the cause (keeping garbage moves
    it 0.7199 -> 0.7113); the divergence is the EP model, not the filter.
    """
    j, _ = joined
    assert spearman_corr(j["off_iso_ppp"].to_numpy(), j["off_explosiveness"].to_numpy()) >= 0.70


def test_havoc_fidelity(joined):
    """Havoc rate vs CFBD def_havoc_total. Observed 2021: 0.6773, floor 0.65.

    Same components (TFL + PBU + INT + forced fumble over plays), but our
    per-play flags are text/derivation-parsed from ESPN pbp and undercount
    vs CFBD's official box-score aggregates: mean 0.111 vs CFBD 0.165 (~33%
    undercount, mostly pass breakups). Ordering survives at ~0.68; raising
    this would require official defensive box stats, not pbp parsing.
    """
    j, _ = joined
    assert spearman_corr(j["def_havoc"].to_numpy(), j["def_havoc_total"].to_numpy()) >= 0.65


def test_adjusted_ranks_vs_sp_plus(joined):
    """Opponent-adjusted EPA ranks vs SP+ component ranks (plan target 0.80).

    Observed 2021: off 0.8657, def 0.8430 -- target met. The adjustment adds
    real signal: raw off-EPA rank correlates 0.808 with SP+ offense rank,
    the adjusted rank 0.866.
    """
    _, js = joined
    assert spearman_corr(js["off_epa_rank"].to_numpy(), js["sp_offense_rank"].to_numpy()) >= 0.80
    assert spearman_corr(js["def_epa_rank"].to_numpy(), js["sp_defense_rank"].to_numpy()) >= 0.80


def test_field_position_avg_start_vs_cfbd():
    """cfb_field_position avg_start_yardline vs CFBD (2021, plan target 0.85).

    Observed 2021: Spearman 0.8974, MAE 0.67 yards (means 29.30 vs 29.04) --
    target met. Orientation note: the captured CFBD off_field_pos_avg_start
    is 100-oriented (yards TO the goal), so the oracle compares against
    ``100 - cfbd``. Drive starts come from the drive-level
    drive.start.yardLine + homeTeamId conversion -- per-play
    start.yardsToEndzone and the within-drive index are unreliable in some
    released games (junk clock, flipped orientation), which capped this
    correlation at ~0.19 before the drive-level fix.
    """
    import sportsdataverse.cfb.cfb_field_position as fp

    pbp = pl.read_parquet(f"{FIX}/pbp_slice_2021.parquet")
    orig = fp.load_cfb_pbp
    fp.load_cfb_pbp = lambda s, **k: pbp
    try:
        out = fp.cfb_field_position([2021])
    finally:
        fp.load_cfb_pbp = orig
    cfbd = pl.read_parquet(f"{FIX}/cfbd_advanced_2021.parquet")
    assert out.schema["team_id"] == cfbd.schema["team_id"]
    j = out.join(cfbd.select(["team_id", "avg_start_yardline"]), on="team_id", how="inner")
    assert j.height == 130
    mine = j["avg_start_yardline"].to_numpy()
    oracle = 100.0 - j["avg_start_yardline_right"].to_numpy()
    assert spearman_corr(mine, oracle) >= 0.85
    assert mae(mine, oracle) <= 1.0
