"""BUG-3: technical / flagrant free throws must not move the possession chain.

bigballR's chain (``all_functions.R:380-436``) has no technical or flagrant
rules, so a made technical FT — an event by the team that does NOT have the
ball — both triggers the corrective re-sync flip and arms ``poss_switch``,
inventing a possession change that never happened. ``fix_technicals=True``
(the shipped default) makes bad-foul FTs inert; ``fix_technicals=False``
reproduces R (the pbp parity tests use it).

# no fixture ORACLE covers a technical — constructed frames pin the rule
Two committed fixtures do carry a bad foul (1613299 a coach technical, 6479639
a flagrant) and both are asserted below — but the R oracle for them encodes the
BUGGY chain, so it cannot adjudicate the fixed output. There is no ground truth
for the correct behavior. The constructed sequences below are hand-built from
the event contract ``_stamp_possessions`` consumes; they pin the intended
semantics, they do not prove them against a refereed capture. Re-validate when
one exists.
"""

from __future__ import annotations

from typing import Any, Optional

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_game_pbp import _stamp_possessions, parse_ncaa_bb_game_pbp
from tests.mbb._bigballr_oracle import HTML_DIR

HOME = "Illinois"
AWAY = "Purdue"


def _ev(
    seconds: int,
    team: str,
    event_type: str,
    result: Optional[str] = None,
    desc: str = "",
) -> "dict[str, Any]":
    return {
        "period": 1,
        "game_seconds": seconds,
        "event_team": team,
        "event_type": event_type,
        "event_result": result,
        "event_description": desc or f"{team} {event_type}",
    }


def _stamp(rows: "list[dict[str, Any]]", *, fix_technicals: bool) -> "list[dict[str, Any]]":
    _stamp_possessions(rows, HOME, AWAY, fix_technicals=fix_technicals)
    return rows


def _technical_sequence() -> "list[dict[str, Any]]":
    """Illinois inbounds; Purdue's coach is whistled for a technical, Illinois
    makes both technical FTs, then Illinois scores in the run of play."""
    return [
        _ev(10, HOME, "Two Point Jumper", "missed"),
        _ev(12, HOME, "Offensive Rebound"),
        # Purdue technical; Illinois shoots two and keeps the ball.
        _ev(20, AWAY, "Commits Foul", None, "SMITH,JOHN foul technical classa"),
        _ev(20, HOME, "Free Throw", "made", "JONES,MIKE freethrow 1of2 made (2)"),
        _ev(20, HOME, "Free Throw", "made", "JONES,MIKE freethrow 2of2 made (3)"),
        _ev(30, HOME, "Layup", "made"),
        _ev(40, AWAY, "Three Point Jumper", "missed"),
        _ev(42, HOME, "Defensive Rebound"),
    ]


def test_made_technical_ft_does_not_flip_poss_team() -> None:
    rows = _stamp(_technical_sequence(), fix_technicals=True)
    ft_rows = [r for r in rows if r["event_type"] == "Free Throw"]

    # The technical FTs belong to the possession that was already in progress.
    assert [r["poss_team"] for r in ft_rows] == [HOME, HOME]
    assert {r["poss_num"] for r in ft_rows} == {rows[0]["poss_num"]}
    # The made Layup (the real possession-ender) still hands the ball to Purdue.
    assert rows[-2]["poss_team"] == AWAY  # Purdue's missed three
    assert rows[-1]["poss_team"] == AWAY


def test_technical_sequence_possession_count_unchanged_by_the_technical() -> None:
    """The technical adds no possession: the fixed chain counts exactly the
    possessions the run of play produces (Illinois, then Purdue)."""
    fixed = _stamp(_technical_sequence(), fix_technicals=True)
    assert len({r["poss_num"] for r in fixed}) == 2
    assert [r["poss_team"] for r in fixed] == [HOME] * 6 + [AWAY] * 2

    # Faithful R: the made technical FT flips the ball to Illinois' opponent
    # (corrective re-sync is a no-op here since Illinois shot it) and arms the
    # switch, so the chain manufactures extra possessions.
    faithful = _stamp(_technical_sequence(), fix_technicals=False)
    assert len({r["poss_num"] for r in faithful}) > 2


def test_offsetting_double_technical_does_not_flip_poss_team() -> None:
    """Both benches get a technical at the same clock; each shoots one FT.
    Offsetting techs net to zero (hoop-explorer ``offsetting_tech_or_flagrant``)
    — the possession in progress survives both."""
    rows = _stamp(
        [
            _ev(10, HOME, "Two Point Jumper", "missed"),
            _ev(12, HOME, "Offensive Rebound"),
            _ev(20, AWAY, "Commits Foul", None, "SMITH,JOHN foul technical classa"),
            _ev(20, HOME, "Commits Foul", None, "DOE,JANE foul technical classa"),
            _ev(20, HOME, "Free Throw", "made", "JONES,MIKE freethrow 1of1 made (2)"),
            _ev(20, AWAY, "Free Throw", "made", "ROE,RICH freethrow 1of1 made (2)"),
            _ev(30, HOME, "Layup", "made"),
        ],
        fix_technicals=True,
    )
    assert [r["poss_team"] for r in rows] == [HOME] * 7
    assert len({r["poss_num"] for r in rows}) == 1


def test_ordinary_and_one_still_works() -> None:
    """Guard: the fix must not disarm the ordinary and-1 rule (a made FG plus a
    shooting-foul FT at the same clock is ONE possession, no switch at the FG)."""
    rows = _stamp(
        [
            _ev(10, HOME, "Layup", "made"),
            _ev(10, AWAY, "Commits Foul", None, "SMITH,JOHN foul personal"),
            _ev(10, HOME, "Free Throw", "made", "JONES,MIKE freethrow 1of1 made (3)"),
            _ev(20, AWAY, "Two Point Jumper", "missed"),
            _ev(22, HOME, "Defensive Rebound"),
        ],
        fix_technicals=True,
    )
    assert [r["poss_team"] for r in rows[:3]] == [HOME] * 3
    assert len({r["poss_num"] for r in rows[:3]}) == 1
    # the and-1 FT ends the possession -> Purdue gets the next one
    assert rows[3]["poss_team"] == AWAY


@pytest.mark.parametrize(
    ("game_id", "bad_foul", "faithful_poss", "fixed_poss"),
    [
        # "Team, foul coachTechnical classa;2freethrow" — 2 phantom possessions
        ("1613299", "coach technical", 141, 139),
        # "Kylan Boswell, foul personal flagrant1;2freethrow;" — 1 phantom
        ("6479639", "flagrant", 142, 141),
    ],
)
def test_bad_foul_fixture_games_drop_phantom_possessions(
    game_id: str, bad_foul: str, faithful_poss: int, fixed_poss: int
) -> None:
    """Real-data check: the two MBB fixtures carrying a bad foul (there is no
    oracle for the FIXED output — R has the bug — so this pins the delta, not
    the truth). The fix only ever REMOVES possessions the technical/flagrant FTs
    manufactured; the parsed events themselves are untouched.

    The lineup columns and ``sub_deviate`` may also move: the lineup walk places
    substitutions at possession boundaries, so dropping a phantom boundary can
    re-home a sub. That is downstream of the fix, not a separate change.
    """
    html = (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")
    fixed = parse_ncaa_bb_game_pbp(html, game_id)
    faithful = parse_ncaa_bb_game_pbp(html, game_id, fix_technicals=False)

    assert fixed.height == faithful.height
    assert faithful["poss_num"].max() == faithful_poss, f"{bad_foul}: faithful count drifted"
    assert fixed["poss_num"].max() == fixed_poss, f"{bad_foul}: fixed count drifted"
    # the parsed event stream is identical — only the chain's reading of it moves
    events = [
        "game_id",
        "period",
        "clock",
        "game_seconds",
        "home_score",
        "away_score",
        "event_team",
        "event_description",
        "player_1",
        "player_2",
        "event_type",
        "event_result",
        "shot_value",
        "event_length",
    ]
    assert fixed.select(events).equals(faithful.select(events))


def test_games_without_bad_fouls_are_identical_in_both_modes() -> None:
    """No technical, no flagrant -> the flag is a no-op."""
    for game_id in ("6470186", "6479592"):
        html = (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")
        fixed = parse_ncaa_bb_game_pbp(html, game_id)
        faithful = parse_ncaa_bb_game_pbp(html, game_id, fix_technicals=False)
        assert isinstance(fixed, pl.DataFrame)
        assert fixed.equals(faithful), f"game {game_id} moved without a bad foul"
