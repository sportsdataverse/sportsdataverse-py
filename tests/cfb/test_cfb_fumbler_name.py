"""``fumble_player_name`` is the fumbler the play text names, never "Yd", a team nickname or the returner.

The name-shaped capture from #355 read "Yd" off ESPN's scoring summary ("Lathan Ransom 27 Yd Fumble
Return"), the team off 2004's "Cougars fumble by Michael Bumpus (WSU)", and a yard line off stats.ncaa's
"to the MSU03 fumbled by #10 T.Green" (nulled downstream, losing the named fumbler). The games are
committed real captures (tests/cfb/fixtures/README.md); the strings in the last test are published rows.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

pytestmark = [
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning"),
    pytest.mark.filterwarnings("ignore::sportsdataverse.errors.PlayOrderWarning"),
]

FIX = Path(__file__).parent / "fixtures"


def _fumblers(game_id: int) -> dict[int, str | None]:
    path = next(FIX.glob(f"summary_{game_id}*.json*"))
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        summary = json.load(fh)
    proc = CFBPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline(fourth_down_probs=False, two_pt_probs=False)
    rows = proc.plays_frame.filter(pl.col("text").str.contains("(?i)fumble"))
    return dict(zip(rows["id"].cast(pl.Int64).to_list(), rows["fumble_player_name"].to_list(), strict=True))


def test_a_fumble_return_summary_names_no_fumbler_and_the_scrimmage_fumbler_survives():
    """401628455 (2024): the summary names only the returner; #355's own case, the fumbler after a
    yardage phrase ("... to the OSU 28 Will Howard fumbled"), keeps its clean capture."""
    got = _fumblers(401628455)
    assert got[401628455103926401] is None  # "Lathan Ransom 27 Yd Fumble Return (Jayden Fielding Kick)" was "Yd"
    assert got[401628455102989506] == "Will Howard"


def test_the_2004_subject_of_fumble_by_is_the_team():
    """243040265 (2004): "<Nickname> fumble by <Player> (TEAM)" credited the nickname."""
    got = _fumblers(243040265)
    assert got[2430402650701] == "Michael Bumpus"  # "Cougars fumble by Michael Bumpus (WSU), ..."
    assert got[2430402651105] == "Reggie Bush"  # "Trojans fumble by Reggie Bush (USC), ..."
    assert not {"Cougars", "Trojans"} & set(got.values())


def test_the_stats_ncaa_yard_line_before_fumbled_by_gives_way_to_the_named_fumbler():
    """401752753 (2025): "... to the MSU03 fumbled by #10 T.Green" captured "MSU03", nulled downstream."""
    got = _fumblers(401752753)
    assert got[401752753191] == "T.Green"
    assert got[401752753397] == "A.Evans III"  # the receiver who caught it, not passer B.Shapen
    assert got[401752753700] == "B.Shapen"  # "fumble by #2 B.Shapen" (no subject) is unchanged


def _fumbler(text: str) -> str | None:
    proc = CFBPlayProcess(gameId=1)
    df = pl.DataFrame(
        [{"text": text, "rush": True, "pass": False, "sack_vec": False, "sack": False, "fumble_vec": True}]
    ).with_columns(pl.lit("Fumble Recovery (Opponent)").alias("type.text"))
    return proc._CFBPlayProcess__add_player_cols(df)["fumble_player_name"][0]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # the recoverer, not the fumbler, in the summary shapes; the legacy fallback read them as fumblers
        ("Jaelan Austin Fumble Recovery in End Zone (Cole Bunce Kick)", None),
        ("Vernon Gholston 21 yd fumble return.", None),
        # "fumble by" in the same summary line still names him
        (
            "Mason Rudolph sacked for loss of 5 yds, fumble by Mason Rudolph, recovered by Matt Galambos 2 Yd "
            "Fumble Return (Chris Blewitt Kick)",
            "Mason Rudolph",
        ),
        # the lowercase summary shapes name the returner too
        ("Justin Rigg 0 Yd fumble recovery (Chance Poore Kick)", None),
        ("Antwain Robinson 16 yard fumble return for a TOUCHDOWN.", None),
        ("Erik Ainge sacked. Worrell Williams returned fumble 44 yards for a TOUCHDOWN.", None),
        ("TEAM run for a loss of 33 yards for a SAFETY TEAM fumbled, recovered by NCaro for a SAFETY", "TEAM"),
        (
            "Jarod Neal pass complete to Ladevin Fair for 5 yds for a TD Ladevin Fair fumbled, recovered by Kent "
            "Khalid Henderson  , return for 89 yds, (Austin MacGinnis KICK)",
            "Ladevin Fair",
        ),
        # a score tag with no name behind it names nobody (the legacy window read "of a SAFETY")
        ("Team run for a loss of 7 yards for a SAFETY fumbled, recovered by TEM Forrest Brock for a SAFETY", None),
        # replays: the overturned call after the ruling is not read (401754570301)
        (
            "(02:51) Shotgun #7 M.Moss pass incomplete short left to #0 C.Bell thrown to LOU50 broken up by #20 "
            'M.Upton. The previous play is under automatic review - "Incomplete pass". CALL OVERTURNED. (Original '
            "Play: (02:51) Shotgun #7 M.Moss pass complete short middle to #0 C.Bell caught at LOU50, for 8 yards to "
            "the BC.48 fumbled by #0 C.Bell at BC.48 forced by #20 M.Upton recovered by BC. #2 K.Price at BC.48, End "
            "Of Play)",
            None,
        ),
        # ESPN's lead-in keeps the overturned fumble; the ruling has none (401520442102854601)
        (
            "Joe Milton III pass complete to for 7 yds fumbled, recovered by VAN Shotgun Milton III,Joe pass "
            "complete short right to White,Squirrel for 7 yards to the VANDY38, End Of Play. The previous play is "
            "under review. The ruling on the field has been overturned. The runner was down prior to losing the "
            "ball (OVERTURNED PLAY: Shotgun #7 J.Milton III pass complete short right to #10 S.White for 8 yards to "
            "the VANDY37 fumbled by #10 S.White at VANDY37 forced by #23 J.Mahoney recovered by VANDY #1 C.Taylor "
            "at VANDY37, End Of Play).",
            None,
        ),
        # a review that creates the fumble keeps the ruling's fumbler (401520365104888301)
        (
            "Braedyn Locke pass complete to Cade Yacamelli for 11 yds to the WIS 44 pass complete to for 9 yds Cade "
            "Yacamelli fumbled, forced by Aaron Casey, recovered by IU Josh Sanguinetti No Huddle-Shotgun "
            "Locke,Braedyn pass complete short middle to Yacamelli,Cade for 11 yards to the WIS44 fumbled by "
            "Yacamelli,Cade at WIS42 forced by Casey,Aaron recovered by IND Sanguinetti,Josh at WIS44, End Of Play. "
            "The previous play is under review. The ruling on the field has been overturned. The runner was not "
            "down prior to losing the ball (OVERTURNED PLAY: (11:42) No Huddle-Shotgun #18 B.Locke pass complete "
            "short middle to #25 C.Yacamelli for 9 yards to the WIS42 (#7 J.Mangum-Farrar; #44 A.Casey)).",
            "Cade Yacamelli",
        ),
        # stats.ncaa: the pass rusher before "fumbled by" is not the fumbler
        (
            "Shotgun #10 T.Ti'a pass complete short right to #7 T.Walker caught at OSU37, for 11 yards to the "
            "OSU40 QB hurried by #10 B.Peleti fumbled by #7 T.Walker at OSU38 forced by #9 C.Francl",
            "T.Walker",
        ),
        # stats.ncaa's team placeholder names no player
        (
            "(06:48) No Huddle Navy rush middle for 6 yards loss to the Navy34 fumbled by Navy at Navy39 "
            "recovered by FAU #11 G.Marcelin at Navy34, End Of Play",
            None,
        ),
        (
            "(01:41) Shotgun Miami (FL) rush middle for 17 yards loss to the MIA02 fumbled by Miami (FL) at "
            "MIA02 recovered by MIA #11 C.Beck at MIA02, End Of Play",
            None,
        ),
    ],
)
def test_published_fumble_rows(text, expected):
    assert _fumbler(text) == expected
