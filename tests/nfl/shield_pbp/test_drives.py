"""Tests for fixed_drive / fixed_drive_result / drive_* detail columns.

Reference: ``docs/superpowers/plans/2026-07-03-nflfastr-parity-reference.md`` §8
(``helper_add_fixed_drives.R :: add_drive_results``) -- the 9-rule ``new_drive``
cascade, ``fixed_drive_result`` end-of-half resolution, and the aggregate-from-
plays ``drive_*`` detail columns (see :mod:`sportsdataverse.nfl.shield_pbp.drives`'s module
docstring for the documented divergence from nflfastR's raw-provider-sourced
drive detail fields).
"""

from __future__ import annotations

from typing import Any, Optional

import polars as pl
from sportsdataverse.nfl.shield_pbp.drives import _OUTPUT_SCHEMA, add_drive_detail
from polars.testing import assert_frame_equal


def _row(
    *,
    play_seq: float,
    posteam: Optional[str],
    defteam: Optional[str],
    game_id: str = "2023_01_BUF_NYJ",
    game_half: str = "Half1",
    qtr: int = 1,
    down: Optional[int] = None,
    ydstogo: Optional[int] = None,
    yardline_100: Optional[int] = None,
    play_type: Optional[str] = None,
    desc: str = "",
    touchdown: int = 0,
    td_team: Optional[str] = None,
    fumble_lost: int = 0,
    safety: int = 0,
    kickoff_attempt: int = 0,
    own_kickoff_recovery: int = 0,
    field_goal_result: Optional[str] = None,
    punt_attempt: int = 0,
    interception: int = 0,
    first_down_rush: int = 0,
    first_down_pass: int = 0,
    first_down_penalty: int = 0,
    penalty_yards: int = 0,
    play_id: Optional[int] = None,
    quarter_seconds_remaining: Optional[int] = None,
    game_seconds_remaining: Optional[int] = None,
    yards_gained: int = 0,
    shield_play_type: Optional[str] = None,
) -> dict[str, Any]:
    return {
        "game_id": game_id,
        "game_half": game_half,
        "play_seq": play_seq,
        "posteam": posteam,
        "defteam": defteam,
        "qtr": qtr,
        "down": down,
        "ydstogo": ydstogo,
        "yardline_100": yardline_100,
        "play_type": play_type,
        "desc": desc,
        "touchdown": touchdown,
        "td_team": td_team,
        "fumble_lost": fumble_lost,
        "safety": safety,
        "kickoff_attempt": kickoff_attempt,
        "own_kickoff_recovery": own_kickoff_recovery,
        "field_goal_result": field_goal_result,
        "punt_attempt": punt_attempt,
        "interception": interception,
        "first_down_rush": first_down_rush,
        "first_down_pass": first_down_pass,
        "first_down_penalty": first_down_penalty,
        "penalty_yards": penalty_yards,
        "play_id": play_id if play_id is not None else int(play_seq),
        "quarter_seconds_remaining": quarter_seconds_remaining,
        "game_seconds_remaining": game_seconds_remaining,
        "yards_gained": yards_gained,
        "shield_play_type": shield_play_type,
    }


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows, infer_schema_length=None)


# ---------------------------------------------------------------------------
# Rule 1 + Rule 6: base possession change / first play of the half.
# ---------------------------------------------------------------------------


def test_first_play_of_half_is_new_drive():
    df = _frame([_row(play_seq=1, posteam="BUF", defteam="NYJ")])
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1]


def test_possession_change_increments_fixed_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run"),
            _row(play_seq=2, posteam="NYJ", defteam="BUF", play_type="punt", punt_attempt=1),
            _row(play_seq=3, posteam="NYJ", defteam="BUF", play_type="run"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 2, 2]


# ---------------------------------------------------------------------------
# Rule 2: PAT after a defensive TD is NOT a new drive.
# ---------------------------------------------------------------------------


def test_pat_after_defensive_touchdown_stays_same_drive():
    df = _frame(
        [
            # BUF fumbles, NYJ returns it for a touchdown.
            _row(
                play_seq=1,
                posteam="BUF",
                defteam="NYJ",
                play_type="run",
                touchdown=1,
                td_team="NYJ",
                fumble_lost=1,
            ),
            # NYJ (the scoring team) attempts the PAT.
            _row(play_seq=2, posteam="NYJ", defteam="BUF", play_type="extra_point"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1]


# ---------------------------------------------------------------------------
# Rule 5: same team retains the ball after its own lost fumble -> new drive,
# even though ``posteam`` looks unchanged from the prior play.
# ---------------------------------------------------------------------------


def test_fumble_retained_by_same_team_starts_new_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", fumble_lost=1, touchdown=0),
            _row(play_seq=2, posteam="BUF", defteam="NYJ", play_type="run"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# Rule 7: recovered onside kick / muffed kickoff return -> new drive.
# ---------------------------------------------------------------------------


def test_own_kickoff_recovery_starts_new_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run"),
            _row(
                play_seq=2,
                posteam="BUF",
                defteam="NYJ",
                play_type="kickoff",
                kickoff_attempt=1,
                own_kickoff_recovery=1,
            ),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# Rule 8: kickoff immediately after a safety -> new drive.
# ---------------------------------------------------------------------------


def test_kickoff_after_safety_starts_new_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", safety=1),
            _row(play_seq=2, posteam="BUF", defteam="NYJ", play_type="kickoff", kickoff_attempt=1),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# Rule 3: PAT after a defensive TD stays the same drive even with exactly ONE
# intervening Timeout / Two-Minute-Warning row (lag-2 TD, lag-1 timeout desc).
# ---------------------------------------------------------------------------


def test_rule3_pat_after_defensive_td_with_one_timeout_gap_stays_same_drive():
    df = _frame(
        [
            # BUF has the ball; NYJ picks it off and scores (defensive TD).
            _row(
                play_seq=1,
                posteam="BUF",
                defteam="NYJ",
                play_type="pass",
                interception=1,
                touchdown=1,
                td_team="NYJ",
            ),
            # ONE timeout marker row in between (null posteam, timeout desc).
            _row(
                play_seq=2,
                posteam=None,
                defteam=None,
                play_type=None,
                desc="Timeout #1 by NYJ at 05:00.",
                shield_play_type="TIMEOUT",
            ),
            # NYJ (the scoring team) attempts the PAT: without rule 3 the base
            # rule's tier-2 lookback (posteam != lag2, lag1 null) would flag this
            # as a new drive.
            _row(play_seq=3, posteam="NYJ", defteam="BUF", play_type="extra_point"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1, 1]


# ---------------------------------------------------------------------------
# Rule 4: same exception with exactly TWO intervening Timeout/2-min-warning
# rows (lag-3 TD, timeout descs at lag-1 AND lag-2).
# ---------------------------------------------------------------------------


def test_rule4_pat_after_defensive_td_with_two_timeout_gaps_stays_same_drive():
    df = _frame(
        [
            _row(
                play_seq=1,
                posteam="BUF",
                defteam="NYJ",
                play_type="pass",
                interception=1,
                touchdown=1,
                td_team="NYJ",
            ),
            _row(
                play_seq=2,
                posteam=None,
                defteam=None,
                play_type=None,
                desc="Timeout #1 by NYJ at 05:00.",
                shield_play_type="TIMEOUT",
            ),
            _row(
                play_seq=3,
                posteam=None,
                defteam=None,
                play_type=None,
                desc="Two-Minute Warning.",
                shield_play_type="TIMEOUT",
            ),
            # Without rule 4 the base rule's tier-3 lookback (posteam != lag3,
            # lag1 AND lag2 null) would flag the PAT as a new drive.
            _row(play_seq=4, posteam="NYJ", defteam="BUF", play_type="extra_point"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1, 1, 1]


# ---------------------------------------------------------------------------
# Rule 5, 2-play-back mirror: fumble retained by the same team, with ONE
# intervening null-posteam row between the fumble and the retaining play.
# ---------------------------------------------------------------------------


def test_rule5_mirror_fumble_retained_across_null_posteam_gap_starts_new_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", fumble_lost=1, touchdown=0),
            # Intervening null-posteam row (a timeout; NOT a TD at lag-2, so
            # rules 3/4 stay silent).
            _row(
                play_seq=2,
                posteam=None,
                defteam=None,
                play_type=None,
                desc="Timeout #1 by BUF at 08:00.",
                shield_play_type="TIMEOUT",
            ),
            # BUF still has the ball: base rule sees posteam == lag2 (no change),
            # so only rule 5's mirror branch can flag the new drive.
            _row(play_seq=3, posteam="BUF", defteam="NYJ", play_type="run"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1, 2]


# ---------------------------------------------------------------------------
# Rule 8, 2-play-back mirror: kickoff after a safety with a null/"no_play" row
# (timeout) between the safety and the kickoff.
# ---------------------------------------------------------------------------


def test_rule8_mirror_kickoff_after_safety_with_no_play_gap_starts_new_drive():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", safety=1),
            # Intervening "no_play" row: rule 8's direct branch (safety at lag-1)
            # does NOT fire on the kickoff; only the lag-2 mirror can.
            _row(
                play_seq=2,
                posteam="BUF",
                defteam="NYJ",
                play_type="no_play",
                desc="Timeout #2 by NYJ at 02:00.",
                shield_play_type="TIMEOUT",
            ),
            _row(play_seq=3, posteam="BUF", defteam="NYJ", play_type="kickoff", kickoff_attempt=1),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1, 2]


# ---------------------------------------------------------------------------
# fixed_drive_result category strings.
# ---------------------------------------------------------------------------


def test_fixed_drive_result_touchdown():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run"),
            _row(play_seq=2, posteam="BUF", defteam="NYJ", play_type="run", touchdown=1, td_team="BUF"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive_result"].to_list() == ["Touchdown", "Touchdown"]
    assert out["drive_ended_with_score"].to_list() == [1, 1]


def test_fixed_drive_result_turnover_and_punt_and_fg():
    df = _frame(
        [
            # Drive 1: turnover (lost fumble).
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", fumble_lost=1),
            # Drive 2 (NYJ, possession changed): punt.
            _row(play_seq=2, posteam="NYJ", defteam="BUF", play_type="punt", punt_attempt=1),
            # Drive 3 (BUF): made field goal.
            _row(play_seq=3, posteam="BUF", defteam="NYJ", play_type="field_goal", field_goal_result="made"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 2, 3]
    assert out["fixed_drive_result"].to_list() == ["Turnover", "Punt", "Field goal"]
    assert out["drive_ended_with_score"].to_list() == [0, 0, 1]


def test_fixed_drive_result_missing_fg_and_turnover_on_downs():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="field_goal", field_goal_result="missed"),
            _row(
                play_seq=2,
                posteam="NYJ",
                defteam="BUF",
                play_type="run",
                down=4,
                ydstogo=5,
                yards_gained=1,
            ),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive_result"].to_list() == ["Missed field goal", "Turnover on downs"]


def test_end_of_half_resolution_uses_first_non_null_result():
    # Same drive: an earlier play is a safety (non-null tmp_result), a later
    # play is the end-of-half marker (also non-null). The LAST non-null value
    # is "End of half" -> per §8's resolution rule, fall back to the FIRST
    # non-null value ("Safety") instead of reporting "End of half" itself.
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="run", safety=1),
            _row(play_seq=2, posteam="BUF", defteam="NYJ", play_type="no_play", desc="(:00) END QUARTER 2"),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive_result"].to_list() == ["Safety", "Safety"]


# ---------------------------------------------------------------------------
# drive_* aggregate columns.
# ---------------------------------------------------------------------------


def test_drive_summary_aggregates():
    df = _frame(
        [
            _row(
                play_seq=1,
                posteam="BUF",
                defteam="NYJ",
                play_type="run",
                qtr=1,
                yardline_100=75,
                penalty_yards=5,
                play_id=101,
                quarter_seconds_remaining=900,
                game_seconds_remaining=3600,
                first_down_rush=1,
            ),
            _row(
                play_seq=2,
                posteam="BUF",
                defteam="NYJ",
                play_type="pass",
                qtr=1,
                yardline_100=15,
                penalty_yards=0,
                play_id=102,
                quarter_seconds_remaining=840,
                game_seconds_remaining=3540,
            ),
            _row(
                play_seq=3,
                posteam="BUF",
                defteam="NYJ",
                play_type="run",
                qtr=1,
                yardline_100=1,
                touchdown=1,
                td_team="BUF",
                play_id=103,
                quarter_seconds_remaining=780,
                game_seconds_remaining=3480,
            ),
        ]
    )
    out = add_drive_detail(df)
    assert out["drive_play_count"].to_list() == [3, 3, 3]
    assert out["drive_first_downs"].to_list() == [1, 1, 1]
    assert out["drive_inside20"].to_list() == [1, 1, 1]  # yardline_100 hit 15/1 <= 20
    assert out["drive_quarter_start"].to_list() == [1, 1, 1]
    assert out["drive_quarter_end"].to_list() == [1, 1, 1]
    assert out["drive_yards_penalized"].to_list() == [5, 5, 5]
    assert out["drive_start_yard_line"].to_list() == [75, 75, 75]
    assert out["drive_end_yard_line"].to_list() == [1, 1, 1]
    assert out["drive_play_id_started"].to_list() == [101, 101, 101]
    assert out["drive_play_id_ended"].to_list() == [103, 103, 103]
    assert out["drive_game_clock_start"].to_list() == ["15:00", "15:00", "15:00"]
    assert out["drive_game_clock_end"].to_list() == ["13:00", "13:00", "13:00"]
    # 3600 - 3480 = 120s -> "2:00"
    assert out["drive_time_of_possession"].to_list() == ["2:00", "2:00", "2:00"]
    assert out["drive_end_transition"].to_list() == ["Touchdown", "Touchdown", "Touchdown"]


def test_drive_start_transition_is_previous_drives_end_transition():
    df = _frame(
        [
            _row(play_seq=1, posteam="BUF", defteam="NYJ", play_type="punt", punt_attempt=1),
            _row(play_seq=2, posteam="NYJ", defteam="BUF", play_type="run"),
        ]
    )
    out = add_drive_detail(df)
    assert out["drive_start_transition"].to_list() == [None, "Punt"]


def test_marker_rows_excluded_from_drive_summary_aggregates():
    # A drive of 2 real plays followed by a TIMEOUT marker row (the rows
    # build.py drops AFTER add_drive_detail runs): the marker must not count
    # toward drive_play_count, and the drive's "last" aggregates (clock end,
    # play_id ended) must come from the last REAL play, not the marker row.
    df = _frame(
        [
            _row(
                play_seq=1,
                posteam="BUF",
                defteam="NYJ",
                play_type="run",
                play_id=101,
                quarter_seconds_remaining=900,
                game_seconds_remaining=3600,
            ),
            _row(
                play_seq=2,
                posteam="BUF",
                defteam="NYJ",
                play_type="pass",
                play_id=102,
                quarter_seconds_remaining=840,
                game_seconds_remaining=3540,
            ),
            _row(
                play_seq=3,
                posteam=None,
                defteam=None,
                play_type=None,
                desc="Timeout #1 by BUF at 13:20.",
                shield_play_type="TIMEOUT",
                play_id=103,
                quarter_seconds_remaining=800,
                game_seconds_remaining=3500,
            ),
        ]
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [1, 1, 1]  # marker stays IN the drive...
    assert out["drive_play_count"].to_list() == [2, 2, 2]  # ...but is not a play
    # Last aggregates come from play_id 102 (the last real play), not the marker.
    assert out["drive_play_id_ended"].to_list() == [102, 102, 102]
    assert out["drive_game_clock_end"].to_list() == ["14:00", "14:00", "14:00"]
    assert out["drive_time_of_possession"].to_list() == ["1:00", "1:00", "1:00"]


# ---------------------------------------------------------------------------
# Degenerate input — never raise, stable schema.
# ---------------------------------------------------------------------------


def test_add_drive_detail_noop_shape_on_empty_frame():
    df = pl.DataFrame({"game_id": []}, schema={"game_id": pl.Utf8})
    out = add_drive_detail(df)
    for col, dtype in _OUTPUT_SCHEMA.items():
        assert col in out.columns
        assert out.schema[col] == dtype


def test_add_drive_detail_noop_without_required_columns():
    df = pl.DataFrame({"game_id": ["2023_01_BUF_NYJ"], "desc": ["some play"]})
    out = add_drive_detail(df)
    assert_frame_equal(out.select("game_id", "desc"), df)
    for col in _OUTPUT_SCHEMA:
        assert col in out.columns


def test_add_drive_detail_degraded_rerun_preserves_existing_columns():
    # A frame already carrying fixed_drive (e.g. a re-run after a column was
    # dropped) must NOT have its real values nulled out by the degraded path.
    df = pl.DataFrame(
        {
            "game_id": ["2023_01_BUF_NYJ"],
            "desc": ["some play"],
            "fixed_drive": pl.Series([3], dtype=pl.Int32),
        }
    )
    out = add_drive_detail(df)
    assert out["fixed_drive"].to_list() == [3]
    for col in _OUTPUT_SCHEMA:
        assert col in out.columns
