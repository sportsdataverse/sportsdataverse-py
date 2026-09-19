"""Offline tests for the NCAA -> cfbfastR column mapper (``to_cfbfastr``).

Runs on the committed real stats.ncaa.org pbp captures under
``tests/fixtures/cfb_ncaa/`` (contests 5336803, 5361446, 5362431, 5362535 --
two FBS blowouts, an FBS shootout, and an FCS-at-FBS game). The running-score
assertions pin REAL 2024 final scores (verified against the public record), so
a scoring-attribution regression (defensive TDs, XP after a pick-six,
lowercase "kick attempt good") fails loudly. The field-position tests add the
2025 page variants 6386335 / 6386574 and the full pbp + box + drives bundle of
6386512 (1OT) -- five of these seven games had ``yards_to_goal`` mirrored.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
import re
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_ncaa_box import (
    parse_cfb_ncaa_drives,
    parse_cfb_ncaa_linescore,
    parse_cfb_ncaa_scoring_summary,
)
from sportsdataverse.cfb.cfb_ncaa_cfbfastr import (
    CFBFASTR_SCHEMA,
    _first_last,
    to_cfbfastr,
)
from sportsdataverse.cfb.cfb_ncaa_pbp import parse_cfb_ncaa_drive_titles, parse_cfb_ncaa_pbp

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_ncaa"

#: fixture -> (last-row pos_team, its score, def_pos_team, its score); real finals.
FINALS = {
    "5336803": ("Ohio St.", 52, "Akron", 6),
    "5361446": ("Boise St.", 56, "Ga. Southern", 45),
    "5362431": ("Cincinnati", 38, "Towson", 20),
    "5362535": ("Merrimack", 6, "Air Force", 21),
}


def _frame(cid: str) -> "pl.DataFrame":
    """The mapped frame for one committed pbp fixture (any vendored name)."""
    path = next(
        p
        for p in (
            FIX / f"mfb_pbp_{cid}.html",
            FIX / f"cfb_ncaa_pbp_{cid}.html",
            FIX / f"mfb_play_by_play_{cid}.html",
        )
        if p.exists()
    )
    html = path.read_text(encoding="utf-8")
    df = to_cfbfastr(parse_cfb_ncaa_pbp(html, contest_id=cid), season=2024)
    assert isinstance(df, pl.DataFrame)
    return df


def _bundle_frame(cid: str) -> "pl.DataFrame":
    """The mapped frame fed every per-game surface, as the ncaa-mfb-football-data build calls it."""
    pbp_html = (FIX / f"mfb_play_by_play_{cid}.html").read_text(encoding="utf-8")
    box_html = (FIX / f"mfb_box_score_{cid}.html").read_text(encoding="utf-8")
    drives = parse_cfb_ncaa_drives((FIX / f"mfb_drives_{cid}.html").read_text(encoding="utf-8"), contest_id=cid)
    df = to_cfbfastr(
        parse_cfb_ncaa_pbp(pbp_html, contest_id=cid),
        season=2025,
        drives=drives,
        linescore=parse_cfb_ncaa_linescore(box_html, contest_id=cid),
        drive_titles=parse_cfb_ncaa_drive_titles(pbp_html),
        ot_drives=drives,
        scoring_summary=parse_cfb_ncaa_scoring_summary(box_html, contest_id=cid),
    )
    assert isinstance(df, pl.DataFrame)
    return df


def test_returns_documented_schema() -> None:
    df = _frame("5336803")
    assert df.columns == list(CFBFASTR_SCHEMA.keys())
    assert df.schema == pl.Schema(CFBFASTR_SCHEMA)


def test_empty_input_zero_row_with_schema() -> None:
    df = to_cfbfastr(parse_cfb_ncaa_pbp("", contest_id=None))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert df.columns == list(CFBFASTR_SCHEMA.keys())
    assert df.schema == pl.Schema(CFBFASTR_SCHEMA)


def test_return_as_pandas() -> None:
    pdf = to_cfbfastr(parse_cfb_ncaa_pbp("", contest_id=None), return_as_pandas=True)
    assert not isinstance(pdf, pl.DataFrame)
    assert list(pdf.columns) == list(CFBFASTR_SCHEMA.keys())


@pytest.mark.parametrize("cid", sorted(FINALS))
def test_running_score_matches_real_final(cid: str) -> None:
    df = _frame(cid)
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {
        FINALS[cid][0]: FINALS[cid][1],
        FINALS[cid][2]: FINALS[cid][3],
    }


def test_structure_and_columns() -> None:
    df = _frame("5336803")
    assert df.height > 100
    assert df.get_column("period").max() >= 4
    assert df.get_column("id_play").n_unique() == df.height
    # cfbfastR flag family present and boolean
    for c in (
        "rush",
        "pass",
        "completion",
        "sack",
        "int",
        "touchdown",
        "punt",
        "kickoff_play",
    ):
        assert df.schema[c] == pl.Boolean, c
    # participant naming converted to "First Last"
    rushers = df.filter(pl.col("rusher_player_name").is_not_null())
    assert rushers.height > 0
    assert not rushers.get_column("rusher_player_name").str.contains(",").any()


def test_first_last_suffixes() -> None:
    assert _first_last("Wilborn Jr.,James") == "James Wilborn Jr."
    assert _first_last("Jordan III,Tre") == "Tre Jordan III"
    assert _first_last("Anthony,Malakai") == "Malakai Anthony"
    assert _first_last(None) is None


# --- field position -------------------------------------------------------
# ``yards_to_goal`` is measured from the drive offense's perspective, so it is
# mirrored (100 - true) for a whole game when the team -> own-yard-line-side
# map is backwards. These invariants hold on every correctly oriented game and
# fail on a mirrored one. Five of the seven fixtures were mirrored before the
# own-side vote stopped reading kickoff rows (a kickoff sits in the RECEIVING
# team's drive but is spotted on the KICKING team's side).

#: cfbfastR's post-clean ``play_type`` vocabulary: the distinct ``type.text``
#: values of every ``cfbfastR-cfb-data/cfb/pbp/parquet/play_by_play_*.parquet``
#: (2004-2026, 4.0M rows) that occur in MORE THAN ONE season, minus the two
#: pre-clean labels ``pbp_clean_pbp_dat.R`` collapses on its way to that file
#: ("Pass" -> Pass Completion/Incompletion/Sack, "Pass Interception" ->
#: Interception Return). The single-season strays are stale builds of one asset
#: and are excluded for the same reason -- "Pass Interception Return" survives
#: only in 2015 (61 rows) against 29,361 "Interception Return" rows, so a
#: downstream ``play_type == "Interception Return"`` filter is what every
#: consumer writes. Rebuild the literal with::
#:
#:     per = {}
#:     for f in sorted(glob("<...>/play_by_play_*.parquet")):
#:         for v in pl.read_parquet(f, columns=["type.text"])["type.text"].drop_nulls().unique():
#:             per.setdefault(v, set()).add(int(re.search(r"(\d{4})", f).group(1)))
#:     sorted(v for v, y in per.items() if len(y) >= 2)
CFBFASTR_PLAY_TYPES = frozenset(
    {
        "Blocked Field Goal",
        "Blocked Field Goal Touchdown",
        "Blocked Punt",
        "Blocked Punt Touchdown",
        "Defensive 2pt Conversion",
        "End Period",
        "End of Game",
        "End of Half",
        "Extra Point Good",
        "Extra Point Missed",
        "Field Goal Good",
        "Field Goal Missed",
        "Fumble",
        "Fumble Recovery (Opponent)",
        "Fumble Recovery (Opponent) Touchdown",
        "Fumble Recovery (Own)",
        "Fumble Recovery (Own) Touchdown",
        "Fumble Return Touchdown",
        "Interception Return",
        "Interception Return Touchdown",
        "Kickoff",
        "Kickoff (Safety)",
        "Kickoff Return (Offense)",
        "Kickoff Return Touchdown",
        "Kickoff Team Fumble Recovery",
        "Missed Field Goal Return",
        "Missed Field Goal Return Touchdown",
        "Pass Incompletion",
        "Pass Reception",
        "Passing Touchdown",
        "Penalty",
        "Punt",
        "Punt (Safety)",
        "Punt Return",
        "Punt Return Touchdown",
        "Punt Team Fumble Recovery",
        "Punt Team Fumble Recovery Touchdown",
        "Rush",
        "Rushing Touchdown",
        "Sack",
        "Safety",
        "Timeout",
        "Two Point Pass",
        "Two Point Rush",
        "Two-Point Conversion Good",
        "Two-Point Conversion Missed",
        "Unknown",
        # Two cfbfastR taxonomy labels (`.pbp_play_types()`; ported here in
        # `sportsdataverse/cfb/model_vars.py`) that the ESPN feed never produces,
        # so they are absent from the parquet: an unattributable touchdown, which
        # is exactly what a synthesized OT drive-summary row is, and the kicking
        # team's own fumble-recovery TD on a kickoff.
        "Uncategorized Touchdown",
        "Kickoff Team Fumble Recovery Touchdown",
    }
)

#: every committed pbp capture, whatever the vendored filename.
ALL_PBP_FIXTURES = sorted(
    {p.stem.split("_")[-1] for p in FIX.glob("*.html") if "pbp" in p.stem or "play_by_play" in p.stem}
)


def test_every_play_type_is_in_cfbfastrs_vocabulary() -> None:
    """``to_cfbfastr`` promises cfbfastR names, so every label it emits must be one.

    Sweeps every committed capture (2019 + 2024 + 2025 page generations). This is
    the guard that was missing when the mapper shipped "Pass Interception Return"
    (cfbfastR's pre-clean value) and a raw structural ``"unknown"``.
    """
    emitted: "dict[str, set[str]]" = {}
    for cid in ALL_PBP_FIXTURES:
        for label in _frame(cid).get_column("play_type").drop_nulls().unique().to_list():
            emitted.setdefault(label, set()).add(cid)
    assert len(emitted) > 15, emitted
    assert not {k: sorted(v) for k, v in emitted.items() if k not in CFBFASTR_PLAY_TYPES}


def test_a_non_touchdown_interception_takes_cfbfastrs_collapsed_label() -> None:
    """cfbfastR publishes "Interception Return", never the pre-clean "Pass Interception Return"."""
    df = _frame("5336803")
    row = df.filter(pl.col("play_text").str.contains("Finley,Ben pass intercepted by Burke,Denzel")).row(0, named=True)
    assert (row["play_type"], row["int"], row["touchdown"]) == ("Interception Return", True, False)


def test_a_returned_kickoff_and_a_blocked_field_goal_take_their_own_labels() -> None:
    """Two labels cfbfastR emits that the mapper could not reach.

    A kickoff the receiver runs back is "Kickoff Return (Offense)" (21,956
    published rows) -- a touchback / fair catch / downed kick stays "Kickoff";
    a blocked field goal is "Blocked Field Goal" (970), not "Field Goal Missed"
    (the ESPN feed's own discriminator is the word "blocked" in the text,
    78/79 blocked field-goal rows in 2024).
    """
    ko = _frame("5361446").filter(pl.col("orig_play_type") == "kickoff")
    returned = ko.filter(pl.col("yds_kickoff_return").is_not_null() & ~pl.col("touchdown"))
    touchbacks = ko.filter(pl.col("play_text").str.contains("(?i)touchback"))
    assert returned.height >= 5 and touchbacks.height >= 3
    assert returned.get_column("play_type").unique().to_list() == ["Kickoff Return (Offense)"]
    assert touchbacks.get_column("play_type").unique().to_list() == ["Kickoff"]

    fg = _bundle_frame("6386512").filter(pl.col("play_text").str.contains("NO GOOD blocked by"))
    assert fg.height == 2, fg.get_column("play_text").to_list()
    assert fg.get_column("play_type").unique().to_list() == ["Blocked Field Goal"]
    assert fg.get_column("fg_made").unique().to_list() == [False]


#: pbp-only fixtures (html) + the one full-bundle fixture (1OT, pbp+box+drives).
FIELD_POSITION_FIXTURES = [
    *sorted(FINALS),
    "6386335",
    "6386574",
]


def _field_position_frames() -> "list[tuple[str, pl.DataFrame]]":
    return [(cid, _frame(cid)) for cid in FIELD_POSITION_FIXTURES] + [("6386512", _bundle_frame("6386512"))]


@pytest.fixture(scope="module")
def field_position_frames() -> "list[tuple[str, pl.DataFrame]]":
    return _field_position_frames()


def test_offensive_td_yards_to_goal_equals_yards_gained(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    """A rushing/receiving TD gains exactly the yards it started from the goal line."""
    bad = {}
    for cid, df in field_position_frames:
        tds = df.filter(
            ((pl.col("rush_td") == True) | (pl.col("pass_td") == True))  # noqa: E712
            & (pl.col("penalty_flag") == False)  # noqa: E712
            # a fumble-return TD still carries rush_td (the rusher's play); the
            # defense scored it, so its yardage is not the offense's
            & (pl.col("turnover_vec") == False)  # noqa: E712
            & pl.col("yards_gained").is_not_null()
        )
        assert tds.height > 0, cid
        wrong = tds.filter(
            (pl.col("yards_to_goal") != pl.col("yards_gained")) | (pl.col("yards_to_goal_end") != 0)
        ).select("yards_to_goal", "yards_gained", "yards_to_goal_end", "play_text")
        if wrong.height:
            bad[cid] = wrong.rows()
    assert not bad, bad


def test_touchback_next_snap_at_receiving_25(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    """A kickoff touchback puts the receiving team's first snap at its own 25 (75 to go)."""
    n = 0
    bad = {}
    scrimmage = ("rush", "pass", "sack", "kneel")
    for cid, df in field_position_frames:
        rows = df.select("orig_play_type", "play_text", "yards_to_goal", "penalty_flag").to_dicts()
        for i, r in enumerate(rows):
            text = (r["play_text"] or "").lower()
            if r["orig_play_type"] != "kickoff" or "touchback" not in text or "penalty" in text:
                continue
            nxt = next((x for x in rows[i + 1 :] if x["orig_play_type"] in (*scrimmage, "penalty")), None)
            if nxt is None or nxt["orig_play_type"] not in scrimmage or nxt["penalty_flag"]:
                continue
            n += 1
            if nxt["yards_to_goal"] != 75:
                bad.setdefault(cid, []).append((nxt["yards_to_goal"], nxt["play_text"]))
    assert n >= len(field_position_frames), n
    assert not bad, bad


def test_yards_to_goal_bounded_and_populated(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    for cid, df in field_position_frames:
        snaps = df.filter(pl.col("orig_play_type").is_in(["rush", "pass", "sack"]) & pl.col("yard_line").is_not_null())
        assert snaps.height > 0, cid
        assert snaps.get_column("yards_to_goal").null_count() == 0, cid
        ytg = df.get_column("yards_to_goal").drop_nulls()
        assert ytg.min() >= 0 and ytg.max() <= 100, cid


def test_ot_bundle_final_and_synthesized_drives(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    """The full-bundle path (drive titles + linescore + scoring summary + OT synthesis) is unchanged."""
    df = dict(field_position_frames)["6386512"]
    assert df.filter(pl.col("ot_synthesized") == True).height > 0  # noqa: E712
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"Houston": 27, "Oregon St.": 24}


# --- end-of-play field position ---------------------------------------------
# 6386303 WestConn @ New Haven (2025) -- 6-letter side code "WSTCNN"
# 6396796 Auburn @ Oklahoma (2025)    -- play text writes "OU36" for the headers' "OKL"


def test_end_yards_to_goal_follows_a_clean_gain() -> None:
    """A clean gain of g yards ends at yards_to_goal - g (no turnover, penalty, lateral or TD).

    6386333 is the trap the learned alias exists for: the text's "TULANE30" starts with the
    other team's header code (Tulsa = TUL, Tulane = TLN).
    """
    for cid in ("6386303", "6396796", "6386333"):
        df = _frame(cid)
        clean = df.filter(
            pl.col("orig_play_type").is_in(["rush", "pass", "sack"])
            & pl.col("yards_gained").is_not_null()
            & pl.col("yards_to_goal").is_not_null()
            & (pl.col("penalty_flag") == False)  # noqa: E712
            & (pl.col("fumble_vec") == False)  # noqa: E712
            & (pl.col("touchdown") == False)  # noqa: E712
            & (pl.col("downs_turnover") == False)  # noqa: E712
            & (pl.col("int") == False)  # noqa: E712
            & pl.col("play_text").str.contains(r"to the ")
            & ~pl.col("play_text").str.contains("lateral")
            & ~pl.col("play_text").str.contains("Original Play:")  # play_text keeps the overturned call
        )
        assert clean.height > 50, cid
        bad = clean.filter(
            pl.col("yards_to_goal_end").is_null()
            | (pl.col("yards_to_goal_end") != pl.col("yards_to_goal") - pl.col("yards_gained"))
        ).select("yards_to_goal", "yards_gained", "yards_to_goal_end", "play_text")
        assert bad.height == 0, (cid, bad.rows()[:5])


# --- return touchdowns --------------------------------------------------------
# 6386449 South Carolina St. @ South Carolina (2025) -- punt return TD, blocked punt
#   return TD and a rush fumble-return TD, all by South Carolina (real final 38-10)
# 6414322 The Citadel @ Samford (2025) -- pass and rush fumble-return TDs by The Citadel

#: (fixture, play_text prefix) -> cfbfastR play_type of a TD scored by the drive's defense
RETURN_TDS = {
    ("6386449", "(05:16) Janish,Elliott punt 44 yards"): "Punt Return Touchdown",
    ("6386449", "(03:19) Janish,Elliott punt 0 yards"): "Blocked Punt Touchdown",
    ("6386449", "(06:07) No Huddle-Shotgun Pickett-Hicks,Mason rush middle"): "Fumble Recovery (Opponent) Touchdown",
    ("6414322", "No Huddle-Shotgun Crittendon,Quincy pass complete short left to Bird,Preston caught at SAM26"): (
        "Fumble Recovery (Opponent) Touchdown"
    ),
    ("6414322", "No Huddle Garner,Jake rush left for 11 yards loss"): "Fumble Recovery (Opponent) Touchdown",
    ("5336803", "No Huddle-Shotgun Bullock,Tahj rush middle for 2 yards gain"): "Fumble Recovery (Opponent) Touchdown",
}


def test_return_touchdowns_use_cfbfastr_labels_not_offensive_td_flags() -> None:
    frames = {cid: _frame(cid) for cid in {cid for cid, _ in RETURN_TDS}}
    for (cid, prefix), label in RETURN_TDS.items():
        row = frames[cid].filter(pl.col("play_text").str.starts_with(prefix))
        assert row.height == 1, (cid, prefix)
        r = row.row(0, named=True)
        assert (r["play_type"], r["touchdown"], r["rush_td"], r["pass_td"]) == (label, True, False, False), (
            cid,
            prefix,
        )
        assert r["score_pts"] == -6, (cid, prefix)  # the drive's defense scored
    # event-sourced running score (no drive-title snapping) credits the returners
    pos, pos_s, dpos, dpos_s = (
        frames["6386449"].select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    )
    assert {pos: pos_s, dpos: dpos_s} == {"South Carolina": 38, "South Carolina St.": 10}


# --- cfbfastR kickoff / end-state convention ----------------------------------
# cfbfastR (and sdv-py's ESPN CFBPlayProcess) put a kickoff on the KICKING team
# (start 65 yards to go from its own 35) and measure the end state for the team
# holding the ball after the play: a touchback is the receiver's 25 (75 to go).


def test_kickoff_stays_with_the_receiver_and_is_measured_from_the_kicker(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    """cfbfastR's kickoff row: ``pos_team`` is the RECEIVING team, 65 to go from the kicker's 35.

    ESPN/cfbfastR set ``pos_team`` to ``return_team`` on every kickoff (2024 parquet:
    2,000/2,000 sampled rows) while ``start.yardsToEndzone`` is 65 -- the spot measured in
    the KICKING team's direction. Possession and the yard-line frame are different teams,
    so this pins both: a swap of possession to the kicker keeps 65 but fails ``pos_team``.
    """
    n_touchbacks = 0
    for cid, df in field_position_frames:
        ko = df.filter(
            (pl.col("orig_play_type") == "kickoff")
            & (pl.col("penalty_flag") == False)  # noqa: E712
            & pl.col("yard_line").str.ends_with("35")
        )
        assert ko.height > 0, cid
        assert ko.get_column("yards_to_goal").to_list() == [65] * ko.height, cid
        tb = ko.filter(pl.col("play_text").str.contains("(?i)touchback"))
        n_touchbacks += tb.height
        assert tb.get_column("yards_to_goal_end").to_list() == [75] * tb.height, cid
        # the kickoff sits in the RECEIVING team's drive: possession is never the team
        # whose own side the ball was spotted on
        for r in ko.select("pos_team", "def_pos_team", "yard_line").to_dicts():
            assert r["yard_line"].startswith(("50",)) or not r["yard_line"].lower().startswith(
                (r["pos_team"] or "\0")[:3].lower()
            ), (cid, r)
        drive1 = df.filter(pl.col("orig_play_type") == "kickoff").head(1)
        after = df.filter(
            pl.col("orig_play_type").is_in(["rush", "pass", "sack", "kneel"])
            & (pl.col("game_play_number") > drive1.get_column("game_play_number")[0])
        ).head(1)
        assert drive1.get_column("pos_team")[0] == after.get_column("pos_team")[0], cid
    assert n_touchbacks > 0


def test_punt_end_state_is_the_receivers_next_snap(field_position_frames: "list[tuple[str, pl.DataFrame]]") -> None:
    """A punt's yards_to_goal_end is the receiving team's: the yards_to_goal of its next snap."""
    n = 0
    bad = {}
    for cid, df in field_position_frames:
        rows = df.filter(~pl.col("orig_play_type").is_in(["timeout", "period_marker"])).to_dicts()
        for cur, nxt in zip(rows, rows[1:]):
            text = (cur["play_text"] or "").lower()
            if (
                cur["orig_play_type"] != "punt"
                or cur["touchdown"]
                or cur["penalty_flag"]
                or nxt["penalty_flag"]
                or any(w in text for w in ("fumble", "blocked", "muff"))
                or nxt["orig_play_type"] not in ("rush", "pass", "sack")
                or nxt["pos_team"] == cur["pos_team"]
            ):
                continue
            n += 1
            if cur["yards_to_goal_end"] != nxt["yards_to_goal"]:
                bad.setdefault(cid, []).append((cur["yards_to_goal_end"], nxt["yards_to_goal"], cur["play_text"]))
    assert n > 20, n
    assert not bad, bad


#: the walk-off a page states for a penalty enforced on the play's own row
#: ("... 10 yards from FIU49 to IND41"). Older pages write it as a plain
#: "10 yards to the VU32", which the parser's end yard line already reads.
_WALK_OFF_RE = re.compile(r"\d+ yards? from \S+ to \S*\d")


def test_same_row_penalty_ends_at_the_enforcement_spot(
    field_position_frames: "list[tuple[str, pl.DataFrame]]",
) -> None:
    """A penalty enforced on the play's own row ends where its text walks the ball TO (NC11).

    The play's own end yard line is the PRE-enforcement spot, so the next snap disagreed
    with this row's end on 199 of the 202 qualifying rows in the committed corpus before
    the fix. ``yards_to_goal_end`` is framed on whoever has the ball after the play, so it
    must equal the next snap's ``yards_to_goal``; a kickoff row is framed on the KICKING
    team instead, so a row followed by one is out of frame and skipped.
    """
    n = 0
    bad = {}
    for cid, df in field_position_frames:
        rows = df.filter(~pl.col("orig_play_type").is_in(["timeout", "period_marker"])).to_dicts()
        for cur, nxt in zip(rows, rows[1:]):
            if (
                not cur["penalty_flag"]
                or not _WALK_OFF_RE.search(cur["play_text"] or "")
                or cur["touchdown"]
                or cur["safety"]
                or (nxt["play_type"] or "").startswith("Kickoff")
                or nxt["yards_to_goal"] is None
            ):
                continue
            n += 1
            if cur["yards_to_goal_end"] != nxt["yards_to_goal"]:
                bad.setdefault(cid, []).append((cur["yards_to_goal_end"], nxt["yards_to_goal"], cur["play_text"]))
    assert n > 50, n
    assert not bad, bad


def test_nullified_touchdown_is_not_a_touchdown() -> None:
    """ "TOUCHDOWN nullified by penalty" scores nothing (6414322: real final The Citadel 40, Samford 13)."""
    df = _frame("6414322")
    row = df.filter(pl.col("play_text").str.starts_with("(02:39) Platte,James punt 56 yards")).row(0, named=True)
    assert (row["touchdown"], row["scoring_play"]) == (False, False)
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"The Citadel": 40, "Samford": 13}


def test_digit_side_code_end_spots_resolve_against_the_game_codes() -> None:
    """1736435 (side code "SFA2"): "to the SFA25" is SFA2's 5, not an "SFA" 25 -- only the game's codes can tell."""
    df = _frame("1736435")
    want = {
        "Ward, Da'Leon rush for no gain to the SFA25": 95,
        "Hoy, Jordan pass complete to Walker, A.J. for 40 yards to the SFA26": 6,
    }
    for prefix, end in want.items():
        row = df.filter(pl.col("play_text").str.starts_with(prefix))
        assert row.height == 1, prefix
        assert row.item(0, "yards_to_goal_end") == end, prefix


def test_first_last_participants_reach_the_cfbfastr_frame() -> None:
    """2019 "First Last" names arrive as they are; "LAST, First" is turned around (NC3)."""
    df = _frame("1735890")
    assert "KeShawn Vaughn" in df.get_column("rusher_player_name").to_list()
    assert "Joe Burrow" in df.get_column("passer_player_name").to_list()
    assert "Derek Stingley" in df.get_column("interception_player_name").to_list()
    df = _frame("1735120")
    # was pinned as "Mike BEAUDRY": the 2019 pages shout the surname and cfbfastR
    # does not, so the mapper now title-cases it (see the shouted-surname test).
    assert "Mike Beaudry" in df.get_column("passer_player_name").to_list()


def _participant_names(df: "pl.DataFrame") -> "set[str]":
    """Every non-null value of every ``*_player_name`` column."""
    return {v for c in df.columns if c.endswith("player_name") for v in df.get_column(c).drop_nulls().to_list()}


def test_a_comma_separated_suffix_stays_a_suffix() -> None:
    """The pages print "Didio, Jr.,Mark"; cfbfastR's form is "Mark Didio Jr." -- suffix LAST.

    Verified against ``play_by_play_2024.parquet``: every suffix in
    ``rusher_player_name`` trails the surname ("Gabe Ervin Jr.", "William
    Atkins IV", "Samuel Brown V"), 5,307 rows; none leads it.
    """
    for cid, want in (("6386300", "Mark Didio Jr."), ("6386303", "Mark Didio Jr."), ("6396796", "Eric Singleton Jr.")):
        names = _participant_names(_frame(cid))
        assert want in names, cid
        assert not [n for n in names if n.startswith(("Jr.", "Sr.", "II ", "III ", "IV ", "V "))], cid


def test_a_bare_suffix_is_not_a_first_name() -> None:
    """Real page strings whose contests are outside the committed corpus.

    "Brown V,Samuel" (6412827) needs ``V`` in the parser's suffix list, and
    cfbfastR publishes exactly "Samuel Brown V"; the 2019 pages also print a
    surname with a suffix and NO first name ("GILLIAM, Jr.", 1736824), where
    splitting on the comma made the suffix the first name.
    """
    assert _first_last("Brown V,Samuel") == "Samuel Brown V"
    assert _first_last("GILLIAM, Jr.") == "Gilliam Jr."
    # a one-letter first initial is written with a period, a roman-numeral
    # suffix never is -- which is what keeps them apart
    assert _first_last("Smith,V.") == "V. Smith"


def test_a_suffix_keeps_no_sentence_period() -> None:
    """ "Jr."/"Sr." own their period; "II"/"IV"/"V" do not, so a terminal one is the sentence's.

    1735539 prints both "... to Wilbert Boyd IV for 7 yards" and the row that ends
    on the name, "Tyler Pullum pass incomplete to Wilbert Boyd IV." -- the same
    player has to come out of both as one value.
    """
    df = _frame("1735539")
    row = df.filter(pl.col("play_text") == "Tyler Pullum pass incomplete to Wilbert Boyd IV.").row(0, named=True)
    assert row["receiver_player_name"] == "Wilbert Boyd IV"
    assert [n for n in _participant_names(df) if "Boyd" in n] == ["Wilbert Boyd IV"]
    # a "Jr."/"Sr." period is the suffix's own and is kept
    assert "Garrison Johnson Sr." in _participant_names(_frame("6414322"))


def test_a_2019_shouted_surname_is_title_cased() -> None:
    """The 2019 pages shout the surname ("HARRIS, Clayton"); cfbfastR is mixed case.

    Conservative on purpose: only an all-uppercase token of three letters or
    more is touched, so a mixed-case source keeps its own internal capitals, a
    two-letter token survives, and the page's own "TEAM" is left alone.
    """
    df = _frame("1735120")
    names = _participant_names(df)
    assert {"Mike Beaudry", "Clayton Harris", "Alexander-Steve"} <= names
    assert "D McKENZIE" in names  # mixed-case source: its "McK" is the page's, not a guess
    assert [n for n in names if re.search(r"\b[A-Z]{3,}\b", n)] == ["TEAM"]
    assert "Racey McMath" in _participant_names(_frame("1735890"))


def test_a_team_rush_carries_cfbfastrs_team_marker() -> None:
    """The 2025 pages print the TEAM as the carrier on a team rush; cfbfastR writes "TEAM".

    16,008 published cfbfastR rows hold "TEAM"/"Team" in ``rusher_player_name``,
    and the 2019 NCAA pages print it that way themselves -- so a team name in a
    player column is the defect, not the marker.
    """
    df = _frame("5336803")
    row = df.filter(pl.col("play_text").str.contains("Akron rush middle for 14 yards loss")).row(0, named=True)
    assert (row["rusher_player_name"], row["pos_team"], row["rush"]) == ("TEAM", "Akron", True)
    # the page's own "TEAM rush" is already right and is not re-cased
    assert _frame("1735120").filter(pl.col("rusher_player_name") == "TEAM").height == 2
    for cid in ALL_PBP_FIXTURES:
        d = _frame(cid)
        teams = set(d.get_column("pos_team").drop_nulls().to_list())
        assert not (_participant_names(d) & teams), cid


def test_td_and_pat_in_one_row_scores_seven() -> None:
    """1735120 prints "... TOUCHDOWN, ..., HARRIS, Clayton kick attempt good." on the scoring play (NC2)."""
    df = _frame("1735120")
    row = df.filter(pl.col("play_text").str.starts_with("BEAUDRY, Mike rush for 2 yards")).row(0, named=True)
    assert (row["play_type"], row["rush_td"], row["score_pts"]) == ("Rushing Touchdown", True, 7)
    six = df.filter(pl.col("play_text").str.starts_with("BEAUDRY, Mike pass intercepted by MORRIS, Myron")).row(
        0, named=True
    )
    assert (six["play_type"], six["score_pts"]) == ("Interception Return Touchdown", -7)


def test_2019_field_goal_text_scores() -> None:
    """ "field goal attempt from 30 GOOD" (no "yards") is a made kick; 1735120's event-sourced final is 21-24."""
    df = _frame("1735120")
    fg = df.filter(pl.col("play_text").str.starts_with("HARRIS, Clayton field goal attempt from 30 GOOD")).row(
        0, named=True
    )
    assert (fg["play_type"], fg["fg_made"], fg["yds_fg"], fg["score_pts"]) == ("Field Goal Good", True, 30, 3)
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"Wagner": 21, "UConn": 24}  # no drive titles: pure event sourcing


def test_fumble_recoveries_follow_cfbfastr_labels_and_turnover_vec() -> None:
    """NC1 / NC6: own recoveries are not turnovers; the recovering team scores a return-less TD."""
    df = _frame("6386493")
    own = df.filter(pl.col("play_text").str.contains("recovered by LSU Van Buren")).row(0, named=True)
    assert (own["play_type"], own["turnover_vec"], own["fumble_vec"], own["rush"]) == (
        "Fumble Recovery (Own)",
        False,
        True,
        True,
    )
    td = df.filter(pl.col("play_text").str.contains("recovered by WKU Flowers,Dylan at WKU29 TOUCHDOWN")).row(
        0, named=True
    )
    assert (td["play_type"], td["rush_td"], td["score_pts"], td["turnover_vec"]) == (
        "Fumble Recovery (Opponent) Touchdown",
        False,
        -6,
        True,
    )
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"Western Ky.": 10, "LSU": 13}
    df = _frame("6386512")
    own = df.filter(pl.col("play_text").str.contains("recovered by OSU") & (pl.col("pos_team") == "Oregon St."))
    assert own.height >= 2 and not own.get_column("turnover_vec").any()
    assert set(own.get_column("play_type").to_list()) <= {
        "Fumble Recovery (Own)",
        "Blocked Field Goal",
        "Field Goal Missed",
        "Punt",
    }


# --- determinism (NC9) ----------------------------------------------------------

_SEED_CHILD = """
import json, sys
from pathlib import Path
from sportsdataverse.cfb.cfb_ncaa_cfbfastr import to_cfbfastr
from sportsdataverse.cfb.cfb_ncaa_pbp import parse_cfb_ncaa_drive_titles, parse_cfb_ncaa_pbp
html = Path(sys.argv[1]).read_text(encoding="utf-8")
df = to_cfbfastr(parse_cfb_ncaa_pbp(html, contest_id="6386300"), season=2025, drive_titles=parse_cfb_ncaa_drive_titles(html))
print(json.dumps(df.select("yards_to_goal", "yards_to_goal_end", "Goal_To_Go").rows()))
"""


def test_field_position_identical_across_hash_seeds() -> None:
    """New Haven @ Saginaw Valley (6386300) built under PYTHONHASHSEED 0..5 must agree.

    Two identical season builds on main disagreed on 54/1,685 games (this one flipped
    ``yards_to_goal`` between 35,87,76... and 65,13,24...) because the own-side pick
    fell out of hash order on a tied vote.
    """
    path = FIX / "mfb_play_by_play_6386300.html"

    def run(seed: int) -> list:
        env = {**os.environ, "PYTHONHASHSEED": str(seed), "PYTHONPATH": str(Path(__file__).resolve().parents[2])}
        out = subprocess.run(
            [sys.executable, "-c", _SEED_CHILD, str(path)], env=env, capture_output=True, text=True, check=True
        )
        return json.loads(out.stdout)

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(run, range(6)))
    assert len(results[0]) > 100
    assert results[0][0][0] == 65  # kickoff from the SVS 35: cfbfastR kicking-team convention
    for seed, rows in enumerate(results[1:], start=1):
        assert rows == results[0], f"seed {seed} differs from seed 0"


def test_quarter_marker_row_opens_the_period_it_names() -> None:
    """NC12: "Start of 3rd quarter, clock 15:00" is period 3, not the drive's period 2.

    The drives tab stamps the drive that straddles the break with its STARTING quarter,
    which overwrote the marker -- so the clock ran 0:00 -> 15:00 inside the old period on
    every game.
    """
    df = _bundle_frame("6386512")
    markers = df.filter(pl.col("play_text").str.contains(r"(?i)start of \dnd|start of \drd|start of \dth"))
    assert markers.height == 3
    for row in markers.iter_rows(named=True):
        named = int(re.search(r"start of (\d)", row["play_text"], re.I).group(1))
        assert (row["period"], row["clock.minutes"], row["clock.seconds"]) == (named, 15, 0)
    # and no row now runs the clock backwards inside a period
    clocked = df.filter(pl.col("clock.minutes").is_not_null()).with_columns(
        __s=pl.col("clock.minutes") * 60 + pl.col("clock.seconds")
    )
    back = clocked.filter((pl.col("period") == pl.col("period").shift(1)) & (pl.col("__s") > pl.col("__s").shift(1)))
    assert back.height == 0, back.select("game_play_number", "period", "play_text").rows()


# --- NC12-NC15: quarter markers, the score walk, OT flags, overturned calls -----------
#
# These run on the producer's own PARSED bundles (``mfb_parsed_<contest>.json.gz``,
# vendored from ``ncaa-mfb-football-raw/mfb/json``) rather than on a page: that store is
# what ``ncaa-mfb-football-data`` compiles, its rows were parsed by whichever
# ``sportsdataverse`` the sweep ran, and the mapper is the one stage every republish
# re-runs. The HTML fixtures cannot reach this path -- today's parser already cuts the
# reprinted call.


def _parsed_bundle(cid: str) -> "dict":
    import gzip

    with gzip.open(FIX / f"mfb_parsed_{cid}.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _parsed_frame(cid: str) -> "pl.DataFrame":
    """The mapped frame of a vendored parsed bundle, built as the producer builds it."""
    from sportsdataverse.cfb.cfb_ncaa_box import DRIVES_SCHEMA, LINESCORE_SCHEMA, SCORING_SUMMARY_SCHEMA
    from sportsdataverse.cfb.cfb_ncaa_pbp import DRIVE_TITLES_SCHEMA, PBP_SCHEMA

    def frame(rows: list, schema: dict) -> "pl.DataFrame":
        if not rows:
            return pl.DataFrame(schema=schema)
        df = pl.DataFrame(rows, infer_schema_length=None)
        return df.cast({k: v for k, v in schema.items() if k in df.columns}).select(
            [k for k in schema if k in df.columns]
        )

    p = _parsed_bundle(cid)
    df = to_cfbfastr(
        frame(p["pbp"], PBP_SCHEMA),
        season=p["season"],
        drives=frame(p["drives"], DRIVES_SCHEMA),
        linescore=frame(p["linescore"], LINESCORE_SCHEMA),
        drive_titles=frame(p["drive_titles"], DRIVE_TITLES_SCHEMA),
        ot_drives=frame(p["drives"], DRIVES_SCHEMA),
        scoring_summary=frame(p["scoring_summary"], SCORING_SUMMARY_SCHEMA),
    )
    assert isinstance(df, pl.DataFrame)
    return df


def _running_scores(df: "pl.DataFrame") -> "list[tuple[int, int]]":
    """(home, away) after every row."""
    return (
        df.with_columns(
            __h=pl.when(pl.col("pos_team") == pl.col("home"))
            .then(pl.col("pos_team_score"))
            .otherwise(pl.col("def_pos_team_score")),
            __a=pl.when(pl.col("pos_team") == pl.col("home"))
            .then(pl.col("def_pos_team_score"))
            .otherwise(pl.col("pos_team_score")),
        )
        .select("__h", "__a")
        .rows()
    )


def test_the_running_score_never_walks_backwards() -> None:
    """NC13: 5361987 (Boise St. 34 at Oregon 37) counted the kickoff return twice.

    The page books the points of a return touchdown into the title of the drive BEFORE
    the kickoff it happened on, so snapping to that checkpoint and then walking the
    return row put Oregon on 40 -- six ahead of a 37 final -- until the next checkpoint
    pulled it back down.
    """
    df = _parsed_frame("5361987")
    scores = _running_scores(df)
    for (ph, pa), (h, a) in zip(scores, scores[1:]):
        assert h >= ph and a >= pa, f"score walked back: {(ph, pa)} -> {(h, a)}"
    assert max(h for h, _ in scores) == 37 and max(a for _, a in scores) == 34
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"Oregon": 37, "Boise St.": 34}


def test_a_checkpoint_cap_never_takes_points_off_the_board() -> None:
    """NC13: the cap is a ceiling, never a floor (6398950, NC State 34 at Wake Forest 24).

    A checkpoint can run AHEAD for one team and BEHIND for the other, and ``_snap``
    holds the whole pair. Clamping the behind side to its level made a scoring row
    SUBTRACT: NC State's touchdown on game play 37 walked 6 -> 0.
    """
    df = _parsed_frame("6398950")
    td = df.filter(pl.col("game_play_number") == 37).row(0, named=True)
    assert (td["play_type"], td["score_pts"]) == ("Passing Touchdown", 6)
    assert td["pos_team_score"] == 6, "a scoring row must never lower the scorer's own score"
    scores = _running_scores(df)
    for (ph, pa), (h, a) in zip(scores, scores[1:]):
        assert h >= ph and a >= pa, f"score walked back: {(ph, pa)} -> {(h, a)}"
    pos, pos_s, dpos, dpos_s = df.select("pos_team", "pos_team_score", "def_pos_team", "def_pos_team_score").row(-1)
    assert {pos: pos_s, dpos: dpos_s} == {"NC State": 34, "Wake Forest": 24}


def test_a_kick_the_page_walked_back_does_not_score() -> None:
    """NC13: 5366625 prints the nullified try AND the re-kick that replaced it."""
    df = _parsed_frame("5366625")
    nullified = df.filter(pl.col("game_play_number") == 6).row(0, named=True)
    assert nullified["play_text"].endswith("NO PLAY.") and nullified["penalty_no_play"]
    assert (nullified["score_pts"], nullified["pos_team_score"]) == (0, 6)
    rekick = df.filter(pl.col("game_play_number") == 7).row(0, named=True)
    assert "kick attempt failed" in rekick["play_text"]
    assert (rekick["score_pts"], rekick["pos_team_score"]) == (0, 6)


def test_kick_result_is_read_from_the_attempt_not_a_tackler_name() -> None:
    """NC13: a bare "good" also reads out of "Gooden,Darius" (5366306, play 192)."""
    from sportsdataverse.cfb.cfb_ncaa_cfbfastr import _KICK_GOOD_RE

    blocked = (
        "Groff,Ty kick attempt failed ( blocked by Yeoman,Corey) (H: Walter,Devin, "
        "LS: Crisanti,Donato) recovered by URI Groff,Ty at Groff,Ty return 0 yards to "
        "the HAMP03 (Gooden,Darius)."
    )
    assert _KICK_GOOD_RE.search(blocked) is None
    assert _KICK_GOOD_RE.search("Massick,Sam kick attempt good (H: Clark,Brady).") is not None
    assert _KICK_GOOD_RE.search("Massick,Sam kick attempt NO GOOD.") is None


def test_a_synthesized_overtime_interception_is_a_pass_play() -> None:
    """NC14: 5367688's OT "Interception Return" row set ``int`` with ``pass`` False."""
    df = _parsed_frame("5367688")
    ot = df.filter(pl.col("ot_synthesized") & (pl.col("play_type") == "Interception Return")).row(0, named=True)
    assert (ot["int"], ot["pass"], ot["period"]) == (True, True, 5)
    # an overtime possession starts 1st & 10 at the 25, so the one-play drive has a down
    assert (ot["down"], ot["distance"]) == (1, 10)
    assert df.filter(pl.col("int") & ~pl.col("pass")).height == 0


def test_an_overturned_touchdown_neither_scores_nor_keeps_its_yardage() -> None:
    """NC15: 5361980 reprints the call the review took away.

    "... rush middle for 1 yard gain to the EIU01 ... PLAY OVERTURNED. (Original Play:
    ... for 2 yards gain to the EIU00 () TOUCHDOWN ...)" -- the mapper read the reprint,
    so the row stayed a 2-yard touchdown ending on the goal line while its own yardage
    said 1.
    """
    row = _parsed_frame("5361980").filter(pl.col("id_play") == 53619800109).row(0, named=True)
    assert "PLAY OVERTURNED" in row["play_text"]
    assert (row["play_type"], row["touchdown"], row["rush_td"]) == ("Rush", False, False)
    assert (row["yards_to_goal"], row["yards_gained"], row["yards_to_goal_end"]) == (2, 1, 1)
    assert row["score_pts"] == 0
