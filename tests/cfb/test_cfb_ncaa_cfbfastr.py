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
