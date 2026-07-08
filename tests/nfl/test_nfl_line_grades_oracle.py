"""Phase-5 oracle gates: PFR concurrent validity + rank sanity.

Fixture provenance: tests/fixtures/nfl_scheme/README.md.
"""

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_scheme_constants import spearman_corr

lg = importlib.import_module("sportsdataverse.nfl.nfl_line_grades")

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_scheme"


@pytest.fixture(scope="module")
def grades_2023() -> pl.DataFrame:
    pbp = pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet").filter(pl.col("season") == 2023)
    return lg._line_grades_from(lg.adjust_pressure_pairs(lg.pressure_pairs(pbp)))


@pytest.fixture(scope="module")
def pfr_team_2023() -> pl.DataFrame:
    pfr = pl.read_parquet(FIXTURES / "pfr_advstats_2023.parquet")
    return (
        pfr.filter(~pl.col("tm").str.contains("TM"))
        .group_by(pl.col("tm").alias("team"))
        .agg(pl.col("prss").sum().alias("pfr_pressures"))
    )


def test_pressure_concurrent_validity_vs_pfr(grades_2023, pfr_team_2023):
    """Gate: Spearman(pbp pressures_generated, PFR prss) >= 0.7 across 2023 teams.

    PFR's independently-charted pressure totals are the concurrent-validity
    measurement.  Counts are the clean contrast: PFR publishes no team
    exposure, so any "pressure pct" would reuse the identical pbp
    dropbacks_def denominator on both sides and only reshuffle ranks
    (observed rate-form value 0.62).  Observed at gate time: 0.7943 over 32
    teams (floor 0.7 per plan, never lowered).  Traded players (2TM/3TM
    rows, ~3% of pressures) are excluded as team-unattributable.
    """
    assert grades_2023.schema["team"] == pfr_team_2023.schema["team"]
    j = grades_2023.join(pfr_team_2023, on="team", how="inner")
    assert j.height == 32
    rho = spearman_corr(j["pressures_generated"].to_numpy(), j["pfr_pressures"].to_numpy())
    assert rho >= 0.7, f"pbp-vs-PFR pressure Spearman {rho}"


def test_known_elite_pass_rush_top_quartile(grades_2023):
    """Gate: CLE (Myles Garrett, 2023 AP Defensive Player of the Year) ranks
    top-quartile in dl_pass_rush_grade.  Observed at gate time: rank 6 of 32.
    BAL (2023 league sack leader) observed rank 8 of 32.
    """
    order = grades_2023.sort("dl_pass_rush_grade", descending=True)["team"].to_list()
    assert order.index("CLE") + 1 <= 8, f"CLE DL rank {order.index('CLE') + 1}"
