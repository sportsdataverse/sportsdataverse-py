"""Tests for series / series_result / series_success.

Reference: ``docs/superpowers/plans/2026-07-03-nflfastr-parity-reference.md`` §7
(``helper_add_series_data.R :: add_series_data``) -- the lag-based
``new_series`` rule (first down on the prior play, a new drive, or the first
play of the half), the exact ``series_result`` category vocabulary, and the
end-of-half resolution rule shared with §8's ``fixed_drive_result``.

These tests supply ``fixed_drive`` directly on the synthetic frame (rather
than routing through :func:`sportsdataverse.nfl.shield_pbp.drives.add_drive_detail`) so the
``new_series`` cascade is exercised in isolation, matching the documented
input contract of :func:`sportsdataverse.nfl.shield_pbp.series.add_series_data`.
"""

from __future__ import annotations

from typing import Any, Optional

import polars as pl
from sportsdataverse.nfl.shield_pbp.series import _OUTPUT_SCHEMA, add_series_data
from polars.testing import assert_frame_equal


def _row(
    *,
    play_seq: float,
    fixed_drive: int,
    posteam: Optional[str] = "BUF",
    defteam: Optional[str] = "NYJ",
    game_id: str = "2023_01_BUF_NYJ",
    game_half: str = "Half1",
    down: Optional[int] = None,
    ydstogo: Optional[int] = None,
    play_type: Optional[str] = "run",
    desc: str = "",
    touchdown: int = 0,
    td_team: Optional[str] = None,
    fumble_lost: int = 0,
    safety: int = 0,
    kickoff_attempt: int = 0,
    field_goal_result: Optional[str] = None,
    punt_attempt: int = 0,
    interception: int = 0,
    first_down_rush: int = 0,
    first_down_pass: int = 0,
    first_down_penalty: int = 0,
    qb_kneel: int = 0,
    yards_gained: int = 0,
) -> dict[str, Any]:
    return {
        "game_id": game_id,
        "game_half": game_half,
        "play_seq": play_seq,
        "fixed_drive": fixed_drive,
        "posteam": posteam,
        "defteam": defteam,
        "down": down,
        "ydstogo": ydstogo,
        "play_type": play_type,
        "desc": desc,
        "touchdown": touchdown,
        "td_team": td_team,
        "fumble_lost": fumble_lost,
        "safety": safety,
        "kickoff_attempt": kickoff_attempt,
        "field_goal_result": field_goal_result,
        "punt_attempt": punt_attempt,
        "interception": interception,
        "first_down_rush": first_down_rush,
        "first_down_pass": first_down_pass,
        "first_down_penalty": first_down_penalty,
        "qb_kneel": qb_kneel,
        "yards_gained": yards_gained,
    }


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows, infer_schema_length=None)


# ---------------------------------------------------------------------------
# new_series: first down on the prior play increments series (same drive).
# ---------------------------------------------------------------------------


def test_first_down_on_prior_play_increments_series():
    df = _frame(
        [
            _row(play_seq=1, fixed_drive=1, first_down_pass=1),
            _row(play_seq=2, fixed_drive=1),  # follows a first down -> new series
            _row(play_seq=3, fixed_drive=1),  # no first down on row2 -> same series
        ]
    )
    out = add_series_data(df)
    assert out["series"].to_list() == [1, 2, 2]


def test_first_down_on_a_touchdown_play_does_not_start_a_new_series():
    # lag(first_down_pass) == 1 but that play was ALSO the touchdown -> excluded
    # by "... & lag(touchdown) == 0".
    df = _frame(
        [
            _row(play_seq=1, fixed_drive=1, first_down_pass=1, touchdown=1, td_team="BUF"),
            _row(play_seq=2, fixed_drive=1, play_type="extra_point"),
        ]
    )
    out = add_series_data(df)
    assert out["series"].to_list() == [1, 1]


# ---------------------------------------------------------------------------
# new_series: a new drive always starts a new series.
# ---------------------------------------------------------------------------


def test_new_drive_increments_series():
    df = _frame(
        [
            _row(play_seq=1, fixed_drive=1, posteam="BUF", defteam="NYJ"),
            _row(play_seq=2, fixed_drive=2, posteam="NYJ", defteam="BUF"),
        ]
    )
    out = add_series_data(df)
    assert out["series"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# series_result category strings + series_success.
# ---------------------------------------------------------------------------


def test_series_result_turnover():
    df = _frame([_row(play_seq=1, fixed_drive=1, fumble_lost=1)])
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["Turnover"]
    assert out["series_success"].to_list() == [0]


def test_series_result_touchdown_is_a_success():
    df = _frame([_row(play_seq=1, fixed_drive=1, touchdown=1, td_team="BUF", posteam="BUF")])
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["Touchdown"]
    assert out["series_success"].to_list() == [1]


def test_series_result_first_down_is_a_success():
    df = _frame([_row(play_seq=1, fixed_drive=1, first_down_rush=1)])
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["First down"]
    assert out["series_success"].to_list() == [1]


def test_series_result_qb_kneel():
    df = _frame([_row(play_seq=1, fixed_drive=1, qb_kneel=1, play_type="qb_kneel")])
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["QB kneel"]
    assert out["series_success"].to_list() == [0]


def test_series_result_opp_touchdown_is_not_a_success():
    df = _frame([_row(play_seq=1, fixed_drive=1, touchdown=1, td_team="NYJ", posteam="BUF")])
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["Opp touchdown"]
    assert out["series_success"].to_list() == [0]


def test_series_result_end_of_half_resolves_to_first_non_null():
    # Same series: a Turnover-on-downs play, then an end-of-half marker in the
    # same series (fixed_drive unchanged, no first down in between). The LAST
    # non-null tmp_result is "End of half" -> resolve to the FIRST non-null
    # value ("Turnover on downs") instead.
    df = _frame(
        [
            _row(play_seq=1, fixed_drive=1, down=4, ydstogo=5, yards_gained=1, play_type="run"),
            _row(play_seq=2, fixed_drive=1, play_type="no_play", desc="(:00) END QUARTER 2"),
        ]
    )
    out = add_series_data(df)
    assert out["series_result"].to_list() == ["Turnover on downs", "Turnover on downs"]


# ---------------------------------------------------------------------------
# Degenerate input — never raise, stable schema.
# ---------------------------------------------------------------------------


def test_add_series_data_noop_shape_on_empty_frame():
    df = pl.DataFrame({"game_id": []}, schema={"game_id": pl.Utf8})
    out = add_series_data(df)
    for col, dtype in _OUTPUT_SCHEMA.items():
        assert col in out.columns
        assert out.schema[col] == dtype


def test_add_series_data_noop_without_required_columns():
    df = pl.DataFrame({"game_id": ["2023_01_BUF_NYJ"], "desc": ["some play"]})
    out = add_series_data(df)
    assert_frame_equal(out.select("game_id", "desc"), df)
    for col in _OUTPUT_SCHEMA:
        assert col in out.columns


def test_add_series_data_degraded_rerun_preserves_existing_columns():
    # A frame already carrying series (e.g. a re-run after a required input
    # column was dropped) must NOT have its real values nulled out.
    df = pl.DataFrame(
        {
            "game_id": ["2023_01_BUF_NYJ"],
            "desc": ["some play"],
            "series": pl.Series([5], dtype=pl.Int32),
        }
    )
    out = add_series_data(df)
    assert out["series"].to_list() == [5]
    for col in _OUTPUT_SCHEMA:
        assert col in out.columns
