"""``build_stints`` credits a shot's xG to the line on the ice when it was taken (WB-hockey-1, task 2.27).

The shift chart changes lines at the goal second, and the half-open ``[start_s, end_s)`` xG window
put the goal in the interval that STARTS there: the line that came on after it. Measured on the
committed captures before the fix: the scorer was on the ice in the credited interval for 11 of the
PWHL slice's 25 goals and 1 of the NHL fixture's 14; after it, 22 / 25 and 13 / 14 (the remaining
goals have no shift for the scorer at that second, a shift-chart gap, not an attribution one). Over
all on-net shots the shooter is on the ice for 228 / 296 PWHL and 151 / 166 NHL before, 259 and 163
after. The floors below are those post-fix measurements; never lower them.

Each shot's xG is isolated by zeroing every other event's ``xg``, so the interval that carries it is
read from ``build_stints``' own output, not re-derived from a window rule.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_rapm import build_stints
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.pwhl._impact_contract import adapt_pwhl_impact_inputs

FIX = Path(__file__).parent.parent / "fixtures"

# build_stints compares game_seconds to boundary values with `==`; polars warns when one is None.
pytestmark = pytest.mark.filterwarnings("ignore:Comparisons with None always result in null:UserWarning")

_BITS = 40  # shots tagged per build_stints call: 2**40 is exact in Float64


def _credited(events: pl.DataFrame, chart: pl.DataFrame) -> list[dict]:
    """Per on-net shot: its type, whether it is on a change second, whether the credited interval ends
    at it, and whether the shooter is on the ice in that interval."""
    events = events.with_row_index("_row")
    shots = events.filter(pl.col("event_type").is_in(["SHOT", "GOAL"])).with_columns(
        _k=pl.int_range(pl.len()).over("game_id")
    )
    out = []
    # shot k of a batch carries xG 2**k and every other event 0, so each interval's xGF is a bitmask
    # of the shots it was credited with
    for lo in range(0, int(shots["_k"].max()) + 1, _BITS):
        batch = shots.filter((pl.col("_k") >= lo) & (pl.col("_k") < lo + _BITS)).with_columns(_bit=pl.col("_k") - lo)
        tagged = events.join(batch.select("_row", "_bit"), on="_row", how="left").with_columns(
            xg=(2.0 ** pl.col("_bit")).fill_null(0.0)
        )
        stints = build_stints(chart, tagged)
        starts = set(zip(stints["game_id"].to_list(), stints["start_s"].to_list()))
        for g in batch.to_dicts():
            home = g["event_team_abbr"] == g["home_abbr"]
            xgf = "xgf_home" if home else "xgf_away"
            hit = stints.filter(
                (pl.col("game_id") == g["game_id"]) & ((pl.col(xgf).cast(pl.Int64) // 2 ** g["_bit"]) % 2 == 1)
            )
            assert hit.height == 1, (g["game_id"], g["game_seconds"])
            s = hit.row(0, named=True)
            out.append(
                {
                    "goal": g["event_type"] == "GOAL",
                    "on_change": (g["game_id"], g["game_seconds"]) in starts,
                    "ends_at_shot": s["end_s"] == g["game_seconds"],
                    "on_ice": g["event_player_1_id"] in (s["home_ids"] if home else s["away_ids"]),
                }
            )
    return out


def test_pwhl_goal_on_a_change_second_credits_the_pre_change_line():
    pbp = pl.read_parquet(FIX / "pwhl_player_impact" / "play_by_play_2025_slice.parquet")
    shifts = pl.read_parquet(FIX / "pwhl_player_impact" / "shifts_2025_slice.parquet")
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    got = _credited(events, chart)
    goals = [g for g in got if g["goal"]]

    assert len(got) == 296 and len(goals) == 25
    on_change = [g for g in got if g["on_change"]]
    assert len(on_change) == 106 and sum(g["goal"] for g in on_change) == 19
    # every change-second shot lands in the interval that ENDS at it (the pre-change line)
    assert all(g["ends_at_shot"] for g in on_change)
    assert sum(g["on_ice"] for g in goals) >= 22  # pre-fix: 11
    assert sum(g["on_ice"] for g in got) >= 259  # pre-fix: 228


def test_nhl_goal_on_a_change_second_credits_the_pre_change_line():
    pbp = pl.read_parquet(FIX / "nhl_player_impact" / "pbp_sample.parquet")
    shifts = pl.read_parquet(FIX / "nhl_player_impact" / "shifts_sample.parquet")
    scored = nhl_xg(pbp, model_dir=str(FIX / "nhl_player_impact" / "xg_models"))
    got = _credited(scored, shifts)
    goals = [g for g in got if g["goal"]]

    assert len(got) == 166 and len(goals) == 14
    on_change = [g for g in got if g["on_change"]]
    assert len(on_change) == 19 and all(g["ends_at_shot"] for g in on_change)
    assert all(g["on_change"] for g in goals)
    assert sum(g["on_ice"] for g in goals) >= 13  # pre-fix: 1
    assert sum(g["on_ice"] for g in got) >= 163  # pre-fix: 151

    # A stint whose only event is the shot at its end second still reads strength and goalies
    # (off the shot): a null strength_state would drop its xG from every strength-filtered engine
    # (R960-1: 9 stints / 0.4586 xG without the fallback). The goalie-null stints left are the
    # pulled-goalie ones.
    stints = build_stints(shifts, scored).filter((pl.col("xgf_home") + pl.col("xgf_away")) > 0)
    assert stints.filter(pl.col("strength_state").is_null()).is_empty()
    no_goalie = stints.filter(pl.col("home_goalie").is_null() | pl.col("away_goalie").is_null())
    assert set(no_goalie["strength_state"].to_list()) <= {"5v6", "6v5"}
