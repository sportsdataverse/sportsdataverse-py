"""``build_stints`` credits a goal's xG to the line on the ice when it was scored (WB-hockey-1, task 2.27).

The shift chart changes lines at the goal second, and the half-open ``[start_s, end_s)`` xG window
put the goal in the interval that STARTS there: the line that came on after it. Measured on the
committed captures before the fix: the scorer was on the ice in the credited interval for 11 of the
PWHL slice's 25 goals and 1 of the NHL fixture's 14; after it, 22 / 25 and 13 / 14 (the remaining
goals have no shift for the scorer at that second, a shift-chart gap, not an attribution one).
The floors below are those post-fix measurements; never lower them.

Each goal's xG is isolated by zeroing every other event's ``xg``, so the interval that carries it is
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


def _credited(events: pl.DataFrame, chart: pl.DataFrame) -> list[tuple[bool, bool, bool]]:
    """Per goal: (on a change second, credited interval ends at the goal second, scorer on ice in it)."""
    events = events.with_row_index("_row")
    goals = events.filter(pl.col("event_type") == "GOAL")
    # goal k of a game carries xG 2**k and every other event 0, so each interval's xGF is a bitmask
    # of the goals it was credited with (one build_stints call per game)
    tag = goals.with_columns(_bit=pl.int_range(pl.len()).over("game_id")).select("_row", "_bit")
    tagged = events.join(tag, on="_row", how="left").with_columns(xg=(2.0 ** pl.col("_bit")).fill_null(0.0))
    stints = build_stints(chart, tagged)
    starts = set(zip(stints["game_id"].to_list(), stints["start_s"].to_list()))
    out = []
    for g in goals.join(tag, on="_row").to_dicts():
        home = g["event_team_abbr"] == g["home_abbr"]
        xgf = "xgf_home" if home else "xgf_away"
        hit = stints.filter(
            (pl.col("game_id") == g["game_id"]) & ((pl.col(xgf).cast(pl.Int64) // 2 ** g["_bit"]) % 2 == 1)
        )
        assert hit.height == 1, (g["game_id"], g["game_seconds"])
        s = hit.row(0, named=True)
        out.append(
            (
                (g["game_id"], g["game_seconds"]) in starts,
                s["end_s"] == g["game_seconds"],
                g["event_player_1_id"] in (s["home_ids"] if home else s["away_ids"]),
            )
        )
    return out


def test_pwhl_goal_on_a_change_second_credits_the_pre_change_line():
    pbp = pl.read_parquet(FIX / "pwhl_player_impact" / "play_by_play_2025_slice.parquet")
    shifts = pl.read_parquet(FIX / "pwhl_player_impact" / "shifts_2025_slice.parquet")
    events, chart = adapt_pwhl_impact_inputs(pbp, shifts)
    got = _credited(events, chart)

    assert len(got) == 25
    on_change = [g for g in got if g[0]]
    assert len(on_change) == 19
    # every change-second goal lands in the interval that ENDS at the goal (the pre-change line)
    assert all(ends_at_goal for _, ends_at_goal, _ in on_change)
    assert sum(on_ice for *_, on_ice in got) >= 22  # pre-fix: 11


def test_nhl_goal_on_a_change_second_credits_the_pre_change_line():
    pbp = pl.read_parquet(FIX / "nhl_player_impact" / "pbp_sample.parquet")
    shifts = pl.read_parquet(FIX / "nhl_player_impact" / "shifts_sample.parquet")
    scored = nhl_xg(pbp, model_dir=str(FIX / "nhl_player_impact" / "xg_models"))
    got = _credited(scored, shifts)

    assert len(got) == 14
    assert all(on_change and ends_at_goal for on_change, ends_at_goal, _ in got)
    assert sum(on_ice for *_, on_ice in got) >= 13  # pre-fix: 1
