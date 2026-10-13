"""The four PWHL player-impact functions on real published PWHL pbp + shifts (C-NF-5, task 2.13).

Before 2.13 every one raised ``ColumnNotFoundError: "secondary_type"`` on ``load_pwhl_pbp`` output;
the shim tests only ever fed them NHL-shaped fixtures. The slices are three 2025 games from
fastRhockey-pwhl-data (see ``tests/fixtures/pwhl_player_impact/README.md``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_rapm import build_stints
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.pwhl._impact_contract import adapt_pwhl_impact_inputs
from sportsdataverse.pwhl.pwhl_player_impact import (
    pwhl_skater_rapm,
    pwhl_skater_war,
    pwhl_special_teams_value,
    pwhl_unit_ratings,
)

FIX = Path(__file__).parent.parent / "fixtures" / "pwhl_player_impact"
MODELS = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact" / "xg_models"

# build_stints compares game_seconds to boundary values with `==`; polars warns when one is None.
pytestmark = pytest.mark.filterwarnings("ignore:Comparisons with None always result in null:UserWarning")


@pytest.fixture(scope="module")
def pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "play_by_play_2025_slice.parquet")


@pytest.fixture(scope="module")
def shifts() -> pl.DataFrame:
    return pl.read_parquet(FIX / "shifts_2025_slice.parquet")


def _skaters(pbp: pl.DataFrame, shifts: pl.DataFrame) -> set[int]:
    goalies = set(pbp["goalie_id"].drop_nulls().to_list())
    return set(shifts["player_id"].to_list()) - goalies


def test_skater_rapm_fits_shift_chart_skaters(pbp, shifts):
    rapm = pwhl_skater_rapm(pbp, shifts, model_dir=MODELS, lam=100.0)

    assert rapm.height > 0
    assert set(rapm["player_id"].to_list()) <= _skaters(pbp, shifts)
    assert rapm["toi_minutes"].min() > 0
    # three games, ~10 skaters a side on the ice for 11,400 s: skater-minutes ~= 3 * 65 min * 10
    assert 1700 < rapm["toi_minutes"].sum() < 2200
    assert rapm["xg_rapm"].abs().sum() > 0


def test_unit_ratings_label_units_with_pwhl_teams(pbp, shifts):
    units = pwhl_unit_ratings(pbp, shifts, model_dir=MODELS, min_toi=0.0)

    assert units.height > 0
    teams = set(pbp["home_team"].to_list()) | set(pbp["away_team"].to_list())
    assert set(units["team"].to_list()) <= teams
    assert (units["on_ice_xgf"] + units["on_ice_xga"]).sum() > 0


def test_special_teams_value_reads_feed_strength_on_every_stint(pbp, shifts):
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    stints = build_stints(chart, nhl_xg(events, model_dir=MODELS, league="pwhl"))
    # every second of the three games (3600 + 3900 + 3900; two went to 5-min OT) is in a stint --
    # build_stints closes each game one second past its last event -- and every stint carries
    # the feed's strength
    assert stints["duration"].sum() == 3600 + 3900 + 3900 + 3
    assert stints.filter(pl.col("strength_state").is_null()).height == 0

    st = pwhl_special_teams_value(pbp, shifts, model_dir=MODELS)
    assert st.height > 0
    assert st["pp_toi_minutes"].sum() > 0
    assert st["pk_toi_minutes"].sum() > 0


def test_skater_war_faceoffs_and_penalties_match_the_feed(pbp, shifts):
    war = pwhl_skater_war(pbp, shifts, model_dir=MODELS)

    assert war.height > 0
    assert war["player_id"].is_null().sum() == 0
    assert 0 not in war["player_id"].to_list()  # no phantom player from unnamed faceoff sides
    assert war["ev_off"].abs().sum() > 0

    cfg = get_constants("pwhl")
    fo = (
        pbp.filter(pl.col("event") == "faceoff")
        .group_by("player_id")
        .agg(won=(pl.col("home_win") == "1").sum().cast(pl.Int64), total=pl.len().cast(pl.Int64))
    )
    taken = pbp.filter(pl.col("event") == "penalty").group_by("player_id").agg(taken=pl.len().cast(pl.Int64))
    expected = (
        fo.join(taken, on="player_id", how="full", coalesce=True)
        .fill_null(0)
        .with_columns(
            faceoffs=(pl.col("won") - 0.5 * pl.col("total")) * cfg.faceoff_goal_weight,
            pens=-pl.col("taken") * cfg.penalty_goal_weight,
        )
    )
    assert war.schema["player_id"] == expected.schema["player_id"]
    got = expected.join(war.select("player_id", "faceoffs", "pens"), on="player_id", how="left", suffix="_war")
    assert got.height == expected.height > 0
    assert got.filter(
        ((pl.col("faceoffs") - pl.col("faceoffs_war")).abs() > 1e-9)
        | ((pl.col("pens") - pl.col("pens_war")).abs() > 1e-9)
    ).is_empty()
