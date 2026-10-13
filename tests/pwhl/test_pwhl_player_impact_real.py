"""The four PWHL player-impact functions on real published PWHL pbp + shifts (C-NF-5, task 2.13).

Before 2.13 every one raised ``ColumnNotFoundError: "secondary_type"`` on ``load_pwhl_pbp`` output;
the shim tests only ever fed them NHL-shaped fixtures. The slices are five 2025 games from
fastRhockey-pwhl-data (see ``tests/fixtures/pwhl_player_impact/README.md``): 296 on-net shots, so
``pwhl_shot_xg`` fits its coordinate logistic rather than the under-200-shot constant fallback.

xG level gate (R846-1, measured 2026-10-12 on these five games): sum(xg) = 24.998 for 25 goals,
xG / goals = 0.9999 (an in-sample fit; the full 2025 season gives 499.01 / 499 = 1.00). The NHL
boosters on the same PWHL shots read 0.66-0.74 of goals. The band below is derived from that
measurement; never lower the gate.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_rapm import build_stints, nhl_skater_rapm
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
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
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    # every typed HockeyTech shot carries its NHL shot type; the feed's "Default" stays null
    shots = pbp.filter(pl.col("event") == "shot")
    typed = shots.filter(pl.col("event_type").is_not_null() & (pl.col("event_type") != "Default")).height
    adapted = events.filter(pl.col("event_type").is_in(["SHOT", "GOAL"]))
    assert adapted.height == shots.height
    assert adapted["secondary_type"].is_not_null().sum() == typed > 0.5 * shots.height

    rapm = pwhl_skater_rapm(pbp, shifts, model_dir=MODELS, lam=100.0)

    assert rapm.height > 0
    assert set(rapm["player_id"].to_list()) <= _skaters(pbp, shifts)
    assert rapm["toi_minutes"].min() > 0
    # five games, ~10 skaters on the ice for 18,600 s: skater-minutes ~= 18,600 / 60 * 10
    assert 2800 < rapm["toi_minutes"].sum() < 3500
    assert rapm["xg_rapm"].abs().sum() > 0
    # the shim's fit is the engine's fit on the adapted frame's own (PWHL) xG, not a re-score
    direct = nhl_skater_rapm(
        pl.DataFrame(), pl.DataFrame(), league="pwhl", lam=100.0, _stints=build_stints(chart, events)
    )
    assert rapm.equals(direct)


def test_unit_ratings_label_units_with_pwhl_teams(pbp, shifts):
    units = pwhl_unit_ratings(pbp, shifts, model_dir=MODELS, min_toi=0.0)

    assert units.height > 0
    teams = set(pbp["home_team"].to_list()) | set(pbp["away_team"].to_list())
    assert set(units["team"].to_list()) <= teams
    assert (units["on_ice_xgf"] + units["on_ice_xga"]).sum() > 0
    # the engine keeps the adapter's (PWHL) xG: measured 24.725 on-ice xGF over all units vs 24.998
    # adapter shot xG, ratio 0.989 (a stint shared by several unit combos splits its xG between
    # them); the floor 0.95 is set from that observation, the ceiling 1.0 holds by construction
    # (units only split the stints' xG). The NHL boosters re-scoring the adapted frame gave
    # 68.162 (review R846-9). Never lower the gate.
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    shot_xg = events.filter(pl.col("event_type").is_in(["SHOT", "GOAL"]))["xg"].sum()
    assert 0.95 <= units["on_ice_xgf"].sum() / shot_xg <= 1.0 + 1e-9
    # nhl_unit_ratings is itself run-to-run unstable in row order and in unit_value at 1e-16
    direct = nhl_unit_ratings(events, chart, league="pwhl", xg_scored=True, min_toi=0.0)

    a, b = units.sort("team", "unit_ids"), direct.sort("team", "unit_ids")
    assert a.columns == b.columns and a.height == b.height
    for col in a.columns:
        if a.schema[col] == pl.Float64:
            assert (a[col] - b[col]).abs().max() < 1e-9, col
        else:
            assert a[col].equals(b[col]), col


def test_special_teams_value_reads_feed_strength_on_every_stint(pbp, shifts):
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    stints = build_stints(chart, events)
    # every second of the five games (3600 + 3900 + 3900 + 3600 + 3600; two went to 5-min OT) is in
    # a stint -- build_stints closes each game one second past its last event -- and every stint
    # carries the feed's strength
    assert stints["duration"].sum() == 3600 + 3900 + 3900 + 3600 + 3600 + 5
    assert stints.filter(pl.col("strength_state").is_null()).height == 0

    # xG level: pwhl_shot_xg on every on-net shot, all of it inside the stints (see module docstring)
    shots = events.filter(pl.col("event_type").is_in(["SHOT", "GOAL"]))
    goals = shots.filter(pl.col("event_type") == "GOAL").height
    assert shots["xg"].null_count() == 0
    assert 0.95 <= shots["xg"].sum() / goals <= 1.05
    assert abs(stints["xgf_home"].sum() + stints["xgf_away"].sum() - shots["xg"].sum()) < 1e-9
    # and it ranks: goals carry more xG than saved shots
    by_outcome = shots.group_by("event_type").agg(pl.col("xg").mean())
    xg_mean = dict(zip(by_outcome["event_type"].to_list(), by_outcome["xg"].to_list()))
    assert xg_mean["GOAL"] > 1.3 * xg_mean["SHOT"]

    st = pwhl_special_teams_value(pbp, shifts, model_dir=MODELS)
    assert st.height > 0
    assert st["pp_toi_minutes"].sum() > 0
    assert st["pk_toi_minutes"].sum() > 0
    # the engine keeps the adapter's xG when told it is scored (no NHL-booster re-score)
    assert st.equals(nhl_special_teams_value(events, chart, league="pwhl", xg_scored=True))
    assert st.equals(nhl_special_teams_value(pl.DataFrame(), pl.DataFrame(), league="pwhl", _stints=stints))


def test_skater_war_faceoffs_and_penalties_match_the_feed(pbp, shifts):
    war = pwhl_skater_war(pbp, shifts, model_dir=MODELS)

    assert war.height > 0
    assert war["player_id"].is_null().sum() == 0
    assert 0 not in war["player_id"].to_list()  # no phantom player from unnamed faceoff sides
    assert war["ev_off"].abs().sum() > 0

    cfg = get_constants("pwhl")
    # even-strength components come from the 5v5 RAPM on the adapter's own (PWHL) xG
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    ev5 = nhl_skater_rapm(
        pl.DataFrame(), pl.DataFrame(), league="pwhl", strength_states=["5v5"], _stints=build_stints(chart, events)
    )
    ev = ev5.join(war.select("player_id", "ev_off"), on="player_id", how="inner")
    assert ev.height == ev5.height > 0
    expected_ev_off = (pl.col("xg_rapm_off") - cfg.replacement_ev_off) * pl.col("toi_minutes") / 60.0
    assert ev.filter((pl.col("ev_off") - expected_ev_off).abs() > 1e-9).is_empty()

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
