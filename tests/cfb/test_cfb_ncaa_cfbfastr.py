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
    """A clean gain of g yards ends at yards_to_goal - g (no fumble, penalty, lateral or TD)."""
    for cid in ("6386303", "6396796"):
        df = _frame(cid)
        clean = df.filter(
            pl.col("orig_play_type").is_in(["rush", "pass", "sack"])
            & pl.col("yards_gained").is_not_null()
            & pl.col("yards_to_goal").is_not_null()
            & (pl.col("penalty_flag") == False)  # noqa: E712
            & (pl.col("fumble_vec") == False)  # noqa: E712
            & (pl.col("touchdown") == False)  # noqa: E712
            & pl.col("play_text").str.contains(r"to the ")
            & ~pl.col("play_text").str.contains("lateral")
        )
        assert clean.height > 50, cid
        bad = clean.filter(
            pl.col("yards_to_goal_end").is_null()
            | (pl.col("yards_to_goal_end") != pl.col("yards_to_goal") - pl.col("yards_gained"))
        ).select("yards_to_goal", "yards_gained", "yards_to_goal_end", "play_text")
        assert bad.height == 0, (cid, bad.rows()[:5])
