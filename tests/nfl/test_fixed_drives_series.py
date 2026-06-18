"""Offline synthetic tests for ``NFLPlayProcess.__add_fixed_drives_series``.

The method ports nflfastR's drive- and series-numbering scheme onto the ESPN
``NFLPlayProcess`` frame so the ESPN-constructed output reaches nflverse parity
on ``fixed_drive`` / ``fixed_drive_result`` / ``series`` / ``series_success`` /
``series_result``.  The rules are translated from:

* ``nflfastR/R/helper_add_fixed_drives.R`` (``add_drive_results``) -- a new
  ``fixed_drive`` increments on a real possession change, with the special
  cases: a PAT/2pt after a *defensive* touchdown stays in the SAME drive
  (L45-54), a recovered onside/muffed kick is a NEW drive (L117-122), and a
  kickoff after a safety is a NEW drive (L124-134).
* ``nflfastR/R/helper_add_series_data.R`` (``add_series_data``) -- ``series``
  increments on a new drive, a non-touchdown first down on the prior play, or
  the first play of the half (L35-47); ``series_success`` is 1 iff the series
  ended in a touchdown or first down (L86-90).

Each scenario below is a hand-built play sequence with possession / down /
flags set so the expected ``fixed_drive`` / ``series`` values can be computed
directly from those R rules (cited inline).  The frames carry only the ~29
input columns the method reads; everything else is irrelevant to the logic.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.nfl_pbp import NFLPlayProcess

# Two synthetic team ids used throughout.
A = 1  # "offense first"
B = 2

# The exact input columns the method consumes.
_DEFAULT = {
    "game_id": 1,
    "half": 1,
    "pos_team": A,
    "def_pos_team": B,
    "start.pos_team.id": A,
    "end.pos_team.id": A,
    "down": 1,
    "end.down": 2,
    "start.distance": 10,
    "statYardage": 3,
    "text": "play.",
    "type.text": "Rush",
    "touchdown": False,
    "safety": False,
    "fumble_lost": False,
    "fumble_recovered": False,
    "field_goal_result": None,
    "kickoff_play": False,
    "kickoff_onside": False,
    "kickoff_downed": False,
    "punt": False,
    "punt_play": False,
    "fg_attempt": False,
    "scrimmage_play": True,
    "int_td": False,
    "penalty_1st_conv": False,
    "pass": False,
    "rush": True,
}

# Explicit dtypes so empty / boolean columns are never inferred as Null.
_SCHEMA = {
    "game_id": pl.Int64,
    "half": pl.Int64,
    "pos_team": pl.Int64,
    "def_pos_team": pl.Int64,
    "start.pos_team.id": pl.Int64,
    "end.pos_team.id": pl.Int64,
    "down": pl.Int64,
    "end.down": pl.Int64,
    "start.distance": pl.Int64,
    "statYardage": pl.Int64,
    "text": pl.Utf8,
    "type.text": pl.Utf8,
    "touchdown": pl.Boolean,
    "safety": pl.Boolean,
    "fumble_lost": pl.Boolean,
    "fumble_recovered": pl.Boolean,
    "field_goal_result": pl.Utf8,
    "kickoff_play": pl.Boolean,
    "kickoff_onside": pl.Boolean,
    "kickoff_downed": pl.Boolean,
    "punt": pl.Boolean,
    "punt_play": pl.Boolean,
    "fg_attempt": pl.Boolean,
    "scrimmage_play": pl.Boolean,
    "int_td": pl.Boolean,
    "penalty_1st_conv": pl.Boolean,
    "pass": pl.Boolean,
    "rush": pl.Boolean,
}


def _frame(rows: list[dict]) -> pl.DataFrame:
    """Build a play frame from partial rows, filling defaults + dtypes."""
    full = [{**_DEFAULT, **r} for r in rows]
    return pl.DataFrame(full, schema=_SCHEMA)


def _run(df: pl.DataFrame) -> pl.DataFrame:
    proc = NFLPlayProcess(gameId=1)
    method = getattr(proc, "_NFLPlayProcess__add_fixed_drives_series")
    return method(df)


# --------------------------------------------------------------------------- #
# fixed_drive                                                                  #
# --------------------------------------------------------------------------- #
def test_fixed_drive_increments_on_possession_change_and_is_additive():
    """A simply alternating possession sequence: each posteam flip is a new
    drive (helper_add_fixed_drives.R L32-44).  First play of the half is a new
    drive (L114-115).  Frame must be strictly additive (5 new cols only)."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {"pos_team": A, "def_pos_team": B, "pass": True, "rush": False},
            {
                "pos_team": A,
                "def_pos_team": B,
                "punt": True,
                "punt_play": True,
                "rush": False,
                "type.text": "Punt",
                "scrimmage_play": False,
            },
            {"pos_team": B, "def_pos_team": A, "rush": True},  # drive 2 (poss flip)
            {"pos_team": B, "def_pos_team": A, "rush": True},
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 3 (poss flip)
        ]
    )
    before = df.width
    out = _run(df)
    assert out.width == before + 5
    assert [c for c in out.columns if c.startswith("_ff_")] == []
    assert out["fixed_drive"].to_list() == [1, 1, 1, 2, 2, 3]
    # monotonic non-decreasing
    fd = out["fixed_drive"].to_list()
    assert all(fd[i] <= fd[i + 1] for i in range(len(fd) - 1))
    # drive 1 ended on a punt
    assert out["fixed_drive_result"].to_list()[0] == "Punt"


def test_pat_after_defensive_td_stays_in_same_drive():
    """Team A is on offense and B returns an interception for a TD; the ensuing
    2pt try is run by B (posteam flips to B) but is NOT a new drive
    (helper_add_fixed_drives.R L45-54: PAT after a defensive TD).  The kickoff
    afterward, run by A, IS a new drive."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {
                "pos_team": A,
                "def_pos_team": B,
                "pass": True,
                "rush": False,  # B returns INT for TD
                "touchdown": True,
                "int_td": True,
                "type.text": "Interception Return Touchdown",
                "text": "intercepted and returned for a touchdown.",
                "end.pos_team.id": A,
                "end.down": 1,
            },
            {
                "pos_team": B,
                "def_pos_team": A,
                "rush": True,  # B's 2pt try -> same drive
                "type.text": "Two Point Rush",
                "scrimmage_play": False,
                "down": 1,
            },
            {
                "pos_team": A,
                "def_pos_team": B,
                "kickoff_play": True,
                "rush": False,  # kickoff by A -> new drive
                "type.text": "Kickoff",
                "scrimmage_play": False,
            },
            {"pos_team": A, "def_pos_team": B, "rush": True},
        ]
    )
    out = _run(df)
    # rows 0,1 = drive 1 (A had the ball, B scored the def TD on a play A possessed,
    # 2pt try by B folds into drive 1); kickoff by A starts drive 2.
    assert out["fixed_drive"].to_list() == [1, 1, 1, 2, 2]
    # drive 1 result is the defensive (opponent) touchdown
    assert out["fixed_drive_result"].to_list()[0] == "Opp touchdown"


def test_pat_after_defensive_td_with_one_timeout_stays_in_same_drive():
    """nflfastR L55-67: a PAT/2pt after a *defensive* TD is NOT a new drive even
    when a single standalone Timeout row is interleaved between the scoring play
    and the try.  ESPN (unlike the assumption the first port made) DOES retain
    standalone timeout rows, so the boundary must be suppressed via the
    ``lag(...,2L)`` variant -- the prior row is a timeout, the TD was 2 rows back,
    and that TD was defensive.  The result MUST equal the no-timeout case
    ``[1, 1, 1, 2, 2]`` (see ``test_pat_after_defensive_td_stays_in_same_drive``):
    the timeout must not manufacture a spurious drive increment that then
    cascades into ``series``."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {
                "pos_team": A,
                "def_pos_team": B,
                "pass": True,
                "rush": False,  # B returns INT for TD
                "touchdown": True,
                "int_td": True,
                "type.text": "Interception Return Touchdown",
                "text": "intercepted and returned for a touchdown.",
                "end.pos_team.id": A,
                "end.down": 1,
            },
            {
                "pos_team": A,  # standalone Official Timeout, possession unchanged
                "def_pos_team": B,
                "rush": False,
                "type.text": "Official Timeout",
                "text": "Timeout.",
                "scrimmage_play": False,
            },
            {
                "pos_team": B,
                "def_pos_team": A,
                "rush": True,  # B's 2pt try -> same drive (lag-2 suppression)
                "type.text": "Two Point Rush",
                "scrimmage_play": False,
                "down": 1,
            },
            {
                "pos_team": A,
                "def_pos_team": B,
                "kickoff_play": True,
                "rush": False,  # kickoff by A -> new drive
                "type.text": "Kickoff",
                "scrimmage_play": False,
            },
            {"pos_team": A, "def_pos_team": B, "rush": True},
        ]
    )
    out = _run(df)
    # The interleaved Official Timeout must NOT manufacture an extra drive: the
    # 2pt try still folds into drive 1, identical to the no-timeout case.
    assert out["fixed_drive"].to_list() == [1, 1, 1, 1, 2, 2]
    # series tracks the drive boundaries; no spurious cascade.
    assert out["series"].to_list() == [1, 1, 1, 1, 2, 2]
    assert out["fixed_drive_result"].to_list()[0] == "Opp touchdown"


def test_pat_after_defensive_td_with_two_timeouts_stays_in_same_drive():
    """nflfastR L68-82: a PAT/2pt after a *defensive* TD is NOT a new drive even
    when TWO standalone timeout rows (e.g. an Official Timeout then the
    Two-minute warning) are interleaved -- the ``lag(...,3L)`` variant.  The
    prior two rows are timeouts, the TD was 3 rows back, and that TD was
    defensive.  The 2pt try must fold into drive 1, never incrementing the
    drive/series across the two interleaved rows."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {
                "pos_team": A,
                "def_pos_team": B,
                "pass": True,
                "rush": False,  # B returns INT for TD
                "touchdown": True,
                "int_td": True,
                "type.text": "Interception Return Touchdown",
                "text": "intercepted and returned for a touchdown.",
                "end.pos_team.id": A,
                "end.down": 1,
            },
            {
                "pos_team": A,  # 1st interleaved timeout
                "def_pos_team": B,
                "rush": False,
                "type.text": "Official Timeout",
                "text": "Timeout.",
                "scrimmage_play": False,
            },
            {
                "pos_team": A,  # 2nd interleaved timeout (two-minute warning)
                "def_pos_team": B,
                "rush": False,
                "type.text": "Two-minute warning",
                "text": "Two-minute warning.",
                "scrimmage_play": False,
            },
            {
                "pos_team": B,
                "def_pos_team": A,
                "rush": True,  # B's 2pt try -> same drive (lag-3 suppression)
                "type.text": "Two Point Rush",
                "scrimmage_play": False,
                "down": 1,
            },
            {
                "pos_team": A,
                "def_pos_team": B,
                "kickoff_play": True,
                "rush": False,  # kickoff by A -> new drive
                "type.text": "Kickoff",
                "scrimmage_play": False,
            },
            {"pos_team": A, "def_pos_team": B, "rush": True},
        ]
    )
    out = _run(df)
    # Two interleaved timeout rows must still not manufacture a spurious drive.
    assert out["fixed_drive"].to_list() == [1, 1, 1, 1, 1, 2, 2]
    assert out["series"].to_list() == [1, 1, 1, 1, 1, 2, 2]
    assert out["fixed_drive_result"].to_list()[0] == "Opp touchdown"


def test_onside_kick_recovery_is_new_drive():
    """A recovered onside kick (kicking team retains) is a NEW drive
    (helper_add_fixed_drives.R L117-122).  ``own_kickoff_recovery`` is derived
    from kickoff_onside + fumble_recovered."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {
                "pos_team": A,
                "def_pos_team": B,
                "field_goal_result": "made",  # FG -> drive 1 ends
                "fg_attempt": True,
                "rush": False,
                "type.text": "Field Goal Good",
                "scrimmage_play": False,
            },
            {
                "pos_team": A,
                "def_pos_team": B,
                "kickoff_play": True,
                "rush": False,  # onside recovery by A
                "kickoff_onside": True,
                "fumble_recovered": True,
                "type.text": "Kickoff",
                "scrimmage_play": False,
                "text": "A kicks onside, recovered by A.",
            },
            {"pos_team": A, "def_pos_team": B, "rush": True},  # A keeps ball
        ]
    )
    out = _run(df)
    # The onside-recovery row opens a NEW drive (L117-122) -- this is the rule
    # under test.  Per the L18-20 posteam swap, a recovered kick's posteam is
    # flipped to the (kicking) team's defteam, so the following scrimmage play
    # by A registers as a possession change and a further new drive -- matching
    # nflfastR exactly.
    fd = out["fixed_drive"].to_list()
    assert fd == [1, 1, 2, 3]
    assert fd[2] == 2  # onside recovery is its own new drive boundary
    assert out["fixed_drive_result"].to_list()[0] == "Field goal"


def test_safety_then_kickoff_is_new_drive_and_result_is_safety():
    """A safety ends the drive with result "Safety" (L148); the kickoff on the
    following play is a NEW drive (L124-134)."""
    df = _frame(
        [
            {"pos_team": A, "def_pos_team": B, "rush": True},  # drive 1
            {
                "pos_team": A,
                "def_pos_team": B,
                "rush": True,
                "safety": True,  # safety -> drive 1 ends
                "type.text": "Safety",
            },
            # free kick after safety; ESPN labels the kickoff row with the RECEIVING
            # team's possession (B).  It opens a new drive (L124-134) and B's
            # ensuing possession continues that drive.
            {
                "pos_team": B,
                "def_pos_team": A,
                "kickoff_play": True,
                "rush": False,
                "type.text": "Kickoff",
                "scrimmage_play": False,
            },
            {"pos_team": B, "def_pos_team": A, "rush": True},  # B receives
        ]
    )
    out = _run(df)
    assert out["fixed_drive"].to_list() == [1, 1, 2, 2]
    assert out["fixed_drive_result"].to_list()[0] == "Safety"


# --------------------------------------------------------------------------- #
# series / series_success / series_result                                      #
# --------------------------------------------------------------------------- #
def test_series_first_down_conversion_increments_and_succeeds():
    """A 3rd-down rush that gains enough for a first down ends the series with
    "First down" (series_result), series_success=1, and the NEXT play opens a
    new series (helper_add_series_data.R L35-47, L86-90)."""
    df = _frame(
        [
            # 1st & 10, gain 4 -> 2nd & 6 (same series 1)
            {"down": 1, "start.distance": 10, "statYardage": 4, "end.down": 2, "rush": True},
            # 3rd & 6, gain 8 -> first down (end.down resets to 1, yards>=togo)
            {"down": 3, "start.distance": 6, "statYardage": 8, "end.down": 1, "rush": True},
            # next play is a new series (1st & 10)
            {"down": 1, "start.distance": 10, "statYardage": 2, "end.down": 2, "rush": True},
        ]
    )
    out = _run(df)
    # series increments after the converting play (the prior-play first down rule)
    assert out["series"].to_list() == [1, 1, 2]
    # series 1 ended in a first down -> success
    s1 = out.filter(pl.col("series") == 1)
    assert s1["series_result"].to_list()[0] == "First down"
    assert s1["series_success"].to_list()[0] == 1


def test_series_turnover_on_downs_fails():
    """A 4th-down play short of the sticks is a turnover on downs:
    series_result="Turnover on downs", series_success=0 (L67-69)."""
    df = _frame(
        [
            {
                "down": 3,
                "start.distance": 8,
                "statYardage": 2,
                "end.down": 4,
                "rush": True,
                "pos_team": A,
                "def_pos_team": B,
            },
            # 4th & 6, gain 2 (< togo) -> turnover on downs; possession flips next
            {
                "down": 4,
                "start.distance": 6,
                "statYardage": 2,
                "end.down": 1,
                "rush": True,
                "pos_team": A,
                "def_pos_team": B,
                "start.pos_team.id": A,
                "end.pos_team.id": B,
            },
            {
                "down": 1,
                "start.distance": 10,
                "statYardage": 3,
                "end.down": 2,
                "rush": True,
                "pos_team": B,
                "def_pos_team": A,
                "start.pos_team.id": B,
                "end.pos_team.id": B,
            },
        ]
    )
    out = _run(df)
    s1 = out.filter(pl.col("series") == 1)
    assert s1["series_result"].to_list()[-1] == "Turnover on downs"
    assert s1["series_success"].to_list()[-1] == 0


def test_series_punt_fails():
    """A punt ends the series with "Punt" and series_success=0 (L65)."""
    df = _frame(
        [
            {
                "down": 3,
                "start.distance": 9,
                "statYardage": 1,
                "end.down": 4,
                "rush": True,
                "pos_team": A,
                "def_pos_team": B,
            },
            # 4th & 8 punt
            {
                "down": 4,
                "start.distance": 8,
                "statYardage": 0,
                "end.down": 1,
                "rush": False,
                "punt": True,
                "punt_play": True,
                "type.text": "Punt",
                "scrimmage_play": False,
                "pos_team": A,
                "def_pos_team": B,
                "start.pos_team.id": A,
                "end.pos_team.id": B,
            },
            {
                "down": 1,
                "start.distance": 10,
                "statYardage": 5,
                "end.down": 2,
                "rush": True,
                "pos_team": B,
                "def_pos_team": A,
                "start.pos_team.id": B,
                "end.pos_team.id": B,
            },
        ]
    )
    out = _run(df)
    s1 = out.filter(pl.col("series") == 1)
    assert s1["series_result"].to_list()[-1] == "Punt"
    assert s1["series_success"].to_list()[-1] == 0


def test_offensive_touchdown_drive_and_series_results():
    """An offensive rushing touchdown -> drive result "Touchdown" and the
    scoring series ends "Touchdown" with success=1 (L143, L59, L86-90)."""
    df = _frame(
        [
            {
                "down": 1,
                "start.distance": 10,
                "statYardage": 6,
                "end.down": 2,
                "rush": True,
                "pos_team": A,
                "def_pos_team": B,
            },
            {
                "down": 2,
                "start.distance": 4,
                "statYardage": 4,
                "end.down": 1,
                "rush": True,
                "touchdown": True,
                "type.text": "Rushing Touchdown",
                "text": "rushes for a touchdown.",
                "pos_team": A,
                "def_pos_team": B,
            },
        ]
    )
    out = _run(df)
    assert out["fixed_drive_result"].to_list()[0] == "Touchdown"
    assert out["series_result"].to_list()[-1] == "Touchdown"
    assert out["series_success"].to_list()[-1] == 1
