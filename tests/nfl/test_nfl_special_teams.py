"""Unit tests for special-teams EPA + punter value (Tasks 4.1/4.2/4.3).

NOTE: the module name nfl_special_teams is distinct from its public function
nfl_special_teams_epa, so plain imports are safe.
"""

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.nfl_special_teams import (
    _punter_value_from,
    _special_teams_epa_from_pbp,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_scheme"


def _st_pbp() -> pl.DataFrame:
    # nflverse semantics: on kickoffs posteam is the RECEIVING team.
    return pl.DataFrame(
        {
            "game_id": ["G", "G", "G", "G"],
            "season": [2023] * 4,
            "posteam": ["A", "B", "A", "A"],
            "defteam": ["B", "A", "B", "B"],
            "play_type": ["punt", "kickoff", "field_goal", "extra_point"],
            "epa": [0.4, -0.2, 0.15, 0.05],
        }
    )


def test_unit_epa_signs():
    out = _special_teams_epa_from_pbp(_st_pbp())
    punt_a = out.filter((pl.col("team") == "A") & (pl.col("unit") == "punt")).row(0, named=True)
    ret_b = out.filter((pl.col("team") == "B") & (pl.col("unit") == "punt_return")).row(0, named=True)
    assert abs(punt_a["epa"] - 0.4) < 1e-9
    assert abs(ret_b["epa"] - (-0.4)) < 1e-9
    # kickoff: posteam B is receiving -> B kickoff_return = epa, A kickoff = -epa
    ko_ret_b = out.filter((pl.col("team") == "B") & (pl.col("unit") == "kickoff_return")).row(0, named=True)
    ko_a = out.filter((pl.col("team") == "A") & (pl.col("unit") == "kickoff")).row(0, named=True)
    assert abs(ko_ret_b["epa"] - (-0.2)) < 1e-9
    assert abs(ko_a["epa"] - 0.2) < 1e-9
    # kicking-only units credit posteam only
    fg_a = out.filter((pl.col("team") == "A") & (pl.col("unit") == "field_goal")).row(0, named=True)
    assert abs(fg_a["epa"] - 0.15) < 1e-9
    assert out.filter((pl.col("team") == "B") & (pl.col("unit") == "field_goal")).height == 0


def test_st_epa_empty():
    out = _special_teams_epa_from_pbp(_st_pbp().head(0))
    assert out.height == 0
    assert out.columns == ["season", "team", "unit", "plays", "epa", "epa_per_play"]


def _punt_pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": ["G"] * 2,
            "season": [2023] * 2,
            "posteam": ["A"] * 2,
            "play_type": ["punt"] * 2,
            "punter_player_id": ["P1", "P1"],
            "yardline_100": [50.0, 50.0],
            "kick_distance": [45.0, 50.0],
            "return_yards": [5.0, 0.0],
            "touchback": [0.0, 1.0],
            "epa": [0.1, -0.1],
        }
    )


def _punt_dist() -> pl.DataFrame:
    # from the 50: half the mass lands at the 12, half at the 8 -> E[after]=10
    return pl.DataFrame(
        {
            "yardline_100": [50.0, 50.0],
            "yardline_after": [12.0, 8.0],
            "pct": [0.5, 0.5],
            "muff": [0.0, 0.0],
        }
    )


def test_punter_value_exp_net():
    out = _punter_value_from(_punt_pbp(), _punt_dist())
    row = out.row(0, named=True)
    assert row["punts"] == 2
    # exp_net = 50 - 10 = 40
    assert abs(row["exp_net_avg"] - 40.0) < 1e-9
    # nets: 45-5=40 ; 50-0-20=30 -> avg 35 -> noe = -5
    assert abs(row["net_avg"] - 35.0) < 1e-9
    assert abs(row["net_over_expected"] - (-5.0)) < 1e-9


def test_punter_value_empty():
    out = _punter_value_from(_punt_pbp().head(0), _punt_dist())
    assert out.height == 0
    assert "net_over_expected" in out.columns


# --------------------------------------------------------------------------- #
# Task 4.3 gates (committed fixture; provenance tests/fixtures/nfl_scheme)
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def oracle_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet")


_ST_TYPES = ["punt", "kickoff", "field_goal", "extra_point"]


def test_unit_epa_sums_to_team_st_epa(oracle_pbp):
    """Gate: sum of per-unit EPA over a team's units == direct team ST-play EPA.

    Kicking units carry the play epa signed to the kicking team and return
    units its negation, so the direct aggregate must credit the same sign.
    Exact to float tolerance (observed max abs diff ~1e-9 at gate time).
    """
    st = _special_teams_epa_from_pbp(oracle_pbp.filter(pl.col("season") == 2023))
    team_sum = st.group_by("team").agg(pl.col("epa").sum().alias("unit_epa_sum"))

    d = oracle_pbp.filter(
        (pl.col("season") == 2023) & pl.col("play_type").is_in(_ST_TYPES) & pl.col("epa").is_not_null()
    )
    kick_side = d.with_columns(
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("defteam")).otherwise(pl.col("posteam")).alias("team"),
        pl.when(pl.col("play_type") == "kickoff").then(-pl.col("epa")).otherwise(pl.col("epa")).alias("signed_epa"),
    )
    ret_side = d.filter(pl.col("play_type").is_in(["punt", "kickoff"])).with_columns(
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("posteam")).otherwise(pl.col("defteam")).alias("team"),
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("epa")).otherwise(-pl.col("epa")).alias("signed_epa"),
    )
    direct = (
        pl.concat(
            [
                kick_side.select("team", "signed_epa"),
                ret_side.select("team", "signed_epa"),
            ]
        )
        .group_by("team")
        .agg(pl.col("signed_epa").sum().alias("direct_epa"))
    )
    j = team_sum.join(direct, on="team", how="inner")
    assert j.height == 32
    assert np.allclose(j["unit_epa_sum"].to_numpy(), j["direct_epa"].to_numpy(), atol=1e-9)


def test_punter_noe_spearman_and_elite_unit(oracle_pbp):
    """Gates: punter net_over_expected is a stable skill signal — season-to-season
    rank stability (Spearman >= 0.35 per transition, punters with 20+ punts both
    years) — and a known-elite 2023 punt unit ranks top-quartile in punt-unit EPA.

    Observed at gate time (2026-07-08 fixture): NOE year-over-year Spearman
    0.6205 (2021->2022, n=26) and 0.5549 (2022->2023, n=26); floor 0.35 set
    below observed, never raised to pass.  (A same-season NOE-vs-net_avg
    correlation is near-tautological — NOE = net_avg - exp_net_avg with a
    nearly constant exp_net_avg — so stability is the meaningful gate.)
    Elite-unit anchor: JAX — Logan Cooke, 2023 AP first-team All-Pro punter —
    observed punt-unit EPA rank 1 of 32; assert top-quartile (<= 8).
    """
    from sportsdataverse.nfl.nfl_fourth_down import _load_punt_data
    from sportsdataverse.nfl.nfl_scheme_constants import spearman_corr

    punt_data = _load_punt_data()
    assert punt_data is not None
    pv = _punter_value_from(oracle_pbp, punt_data).filter(pl.col("punts") >= 20)
    for year in (2021, 2022):
        a = pv.filter(pl.col("season") == year).select("punter_player_id", "net_over_expected")
        b = pv.filter(pl.col("season") == year + 1).select(
            "punter_player_id", pl.col("net_over_expected").alias("noe_next")
        )
        assert a.schema["punter_player_id"] == b.schema["punter_player_id"]
        j = a.join(b, on="punter_player_id", how="inner")
        assert j.height >= 20
        rho = spearman_corr(j["net_over_expected"].to_numpy(), j["noe_next"].to_numpy())
        assert rho >= 0.35, f"punter NOE stability {year}->{year + 1} Spearman {rho}"

    st = _special_teams_epa_from_pbp(oracle_pbp.filter(pl.col("season") == 2023))
    punt_units = st.filter(pl.col("unit") == "punt").sort("epa", descending=True)
    rank = punt_units["team"].to_list().index("JAX") + 1
    assert rank <= 8, f"JAX punt unit rank {rank}"
