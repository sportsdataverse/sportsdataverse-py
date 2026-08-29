"""Penalty flag + enforcement classification (cfbfastR-cfb-data#32).

Every case here is a real play text pattern taken from the release, not an
invented one -- the bugs these lock in were all found by measuring real seasons
(`sdv-py/dev/penalty-analysis/`), and a synthetic fixture would not have shown
any of them.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

# name-mangled private
_SETUP = CFBPlayProcess._CFBPlayProcess__setup_penalty_data


def _run(rows: list[tuple[str, str]]) -> pl.DataFrame:
    """rows = [(type.text, text)] -> the penalty columns."""
    df = pl.DataFrame({"type.text": [r[0] for r in rows], "text": [r[1] for r in rows]})
    return _SETUP(CFBPlayProcess.__new__(CFBPlayProcess), df)


def test_offsetting_matches_both_spellings() -> None:
    """ESPN writes `offsetting` 44x vs `off-setting` 1x in 2025; the old
    hyphen-only pattern flagged 0 of the 44."""
    out = _run(
        [
            ("Penalty", "PENALTY WKU Holding offsetting MSU Holding offsetting. NO PLAY."),
            ("Penalty", "PENALTY off-setting penalties. NO PLAY."),
        ]
    )
    assert out["penalty_offset"].to_list() == [True, True]


def test_declined_is_flagged_off_a_normally_typed_play() -> None:
    """The old gate required `type.text == 'Penalty'`, missing 576 of 894
    declined texts in 2025."""
    out = _run(
        [
            ("Pass Incompletion", "Smith pass incomplete Penalty, Holding declined"),
            ("Penalty", "PENALTY Holding declined"),
        ]
    )
    assert out["penalty_declined"].to_list() == [True, True]


def test_nullified_by_penalty_is_a_no_play() -> None:
    """ESPN's explicit verdict. 179 plays in 2025; the old rule caught none of
    the ones that never say 'no play'."""
    out = _run(
        [
            ("Rushing Touchdown", "Lawrence rush left for 8 yards TOUCHDOWN nullified by penalty PENALTY MOST Holding"),
        ]
    )
    assert out["penalty_no_play"][0] is True
    assert out["penalty_enforcement"][0] == "no_play"
    assert out["penalty_negated_play"][0] is True


def test_multi_penalty_counts_and_all_declined() -> None:
    """A play can carry two fouls where only one is declined, so `declined` in
    the text does NOT mean the play stood."""
    out = _run(
        [
            ("Rush", "run for 4 yds Penalty, Holding declined PENALTY Face Mask"),  # 1 of 2 declined
            ("Rush", "run for 4 yds Penalty, Holding declined"),  # all declined
        ]
    )
    assert out["penalty_count"].to_list() == [2, 1]
    assert out["penalty_declined_count"].to_list() == [1, 1]
    assert out["penalty_all_declined"].to_list() == [False, True]
    # only the all-declined play is classified as having stood
    assert out["penalty_enforcement"][1] == "declined"
    assert out["penalty_negated_play"][1] is False


def test_negating_and_standing_fouls_are_classified() -> None:
    out = _run(
        [
            ("Rush", "rush for 3 yards Penalty, Offensive Holding (-10 Yards)"),
            ("Sack", "sacked for -7 yards Penalty, Intentional Grounding"),
        ]
    )
    assert out["penalty_enforcement"].to_list() == ["negating_foul", "play_stands"]
    assert out["penalty_negated_play"].to_list() == [True, False]


def test_auto_first_down_fouls_stay_unknown() -> None:
    """These CANNOT be classified from the available signals -- a negated play
    carrying an automatic first down resets the down to 1 instead of repeating
    it, so the replay signal cannot separate it from a dead-ball foul. Guessing
    here is how cfbfastR-cfb-data#30 shipped."""
    out = _run(
        [
            ("Pass Incompletion", "pass incomplete for a 1ST down Penalty, Defensive pass interference"),
            ("Pass Reception", "pass complete for 12 yards Penalty, Personal Foul (15 Yards)"),
            ("Pass Reception", "pass complete Penalty, Roughing Passer"),
        ]
    )
    assert out["penalty_enforcement"].to_list() == ["unknown"] * 3
    # null, NOT false -- a consumer must not read "unknown" as "the play counted"
    assert out["penalty_negated_play"].to_list() == [None, None, None]


def test_no_penalty_leaves_enforcement_null() -> None:
    out = _run([("Rush", "Jones run for 5 yards to the OSU 30")])
    assert out["penalty_flag"][0] is False
    assert out["penalty_enforcement"][0] is None
    assert out["penalty_negated_play"][0] is False


@pytest.mark.parametrize(
    "text,expected",
    [
        ("TOUCHDOWN nullified by penalty PENALTY Holding", False),
        ("rush for 8 yards TOUCHDOWN", True),
        ("pass complete for a TD", True),
        ("TOUCHDOWN PENALTY Holding. NO PLAY.", False),
    ],
)
def test_td_play_excludes_negated_plays(text: str, expected: bool) -> None:
    """30 nullified touchdowns across 2015/2021/2025 were still counted."""
    from sportsdataverse.cfb.cfb_pbp import _PENALTY_NEGATED_TEXT

    df = pl.DataFrame({"text": [text]}).with_columns(
        td_play=pl.col("text").str.contains("(?i)touchdown|(?i)for a TD")
        & ~pl.col("text").str.contains(_PENALTY_NEGATED_TEXT)
    )
    assert df["td_play"][0] is expected


def test_negated_touchdown_produces_no_td_flags() -> None:
    """`td_play` alone was not enough (cfbfastR-cfb-data#32 review).

    ESPN keeps the `Passing Touchdown` / `Rushing Touchdown` label on a play it
    also says was nullified, and `pass_td` / `rush_td` fire off that label
    directly -- so gating only `td_play` left them True. Measured on 2025: 15
    `pass_td` and 15 `rush_td` on negated plays, every one reaching a player
    leaderboard, where `summarize_passer` sums `pass_td` into `passing_td`.
    """
    from sportsdataverse.cfb.cfb_pbp import _PENALTY_NEGATED_TEXT

    rows = [
        ("Rushing Touchdown", "rush left for 8 yards TOUCHDOWN nullified by penalty PENALTY Holding", True),
        ("Passing Touchdown", "pass complete for 20 yards TOUCHDOWN PENALTY Holding. NO PLAY.", True),
        ("Rushing Touchdown", "rush left for 8 yards TOUCHDOWN", False),
        ("Passing Touchdown", "pass complete for 20 yards TOUCHDOWN", False),
    ]
    df = pl.DataFrame(
        {
            "type.text": [r[0] for r in rows],
            "text": [r[1] for r in rows],
            "negated": [r[2] for r in rows],
        }
    )
    neg = pl.col("text").str.contains(_PENALTY_NEGATED_TEXT)
    out = df.with_columns(
        td_play=pl.col("text").str.contains("(?i)touchdown|(?i)for a TD") & ~neg,
        touchdown=pl.col("type.text").str.contains("(?i)touchdown") & ~neg,
        td_check=pl.col("text").str.contains("(?i)touchdown") & ~neg,
        pass_td=pl.when(neg)
        .then(False)
        .when(pl.col("type.text").is_in(["Passing Touchdown"]))
        .then(True)
        .otherwise(False),
        rush_td=pl.when(neg)
        .then(False)
        .when(pl.col("type.text").is_in(["Rushing Touchdown"]))
        .then(True)
        .otherwise(False),
    )
    negated = out.filter(pl.col("negated"))
    for col in ("td_play", "touchdown", "td_check", "pass_td", "rush_td"):
        assert not any(negated[col].to_list()), f"{col} still True on a negated touchdown"
    # and a real touchdown is untouched
    real = out.filter(~pl.col("negated"))
    assert all(real["td_play"].to_list())
    assert real["pass_td"].to_list() == [False, True]
    assert real["rush_td"].to_list() == [True, False]


# --- penalty_detail labeler: gaps measured by the 2025-season taxonomy ---


def _details(rows):
    return _run(rows)["penalty_detail"].to_list()


def test_detail_roughing_the_passer_with_the() -> None:
    """'Roughing The Passer' (with 'the') was 242 of the 844 'Missing' rows in
    2025; the old pattern required the literal 'roughing passer'."""
    assert _details(
        [
            ("Penalty", "PENALTY OSU Roughing The Passer (Jack Sawyer) 15 yards to the OSU 40"),
            ("Penalty", "PENALTY MICH roughing passer 15 yards"),
        ]
    ) == ["Roughing the Passer", "Roughing the Passer"]


def test_detail_declined_keeps_foul_name() -> None:
    """A declined penalty keeps its foul name; the disposition lives in
    penalty_declined. The old chain labeled 318 rows just 'Declined'."""
    out = _run([("Pass Incompletion", "Smith pass incomplete Penalty, Holding declined")])
    assert out["penalty_detail"][0] == "Holding"
    assert out["penalty_declined"][0] is True


def test_detail_disposition_fires_only_without_foul_name() -> None:
    out = _run([("Penalty", "PENALTY declined")])
    assert out["penalty_detail"][0] == "Declined"


def test_detail_illegal_substitution_maps_to_substitution_infraction() -> None:
    assert _details([("Penalty", "PENALTY UGA Illegal Substitution 5 yards to the UGA 30")]) == [
        "Substitution Infraction",
    ]


def test_detail_hyphenated_offside() -> None:
    """44 'off-side' spellings in 2025 fell to Missing."""
    assert _details([("Penalty", "PENALTY ARK off-side 5 yards to the ARK 35")]) == ["Offside"]


def test_detail_vendor_typos() -> None:
    """ESPN ships literal 'Inteference' and 'inelgible' typos."""
    assert _details(
        [
            ("Penalty", "USC Penalty, Sideline Inteference (15 Yards) to the MOST 38"),
            ("Pass Reception", "Anderson pass complete Wyoming Penalty, inelgible downfield on pass (-5 Yards)"),
        ]
    ) == ["Sideline Interference", "Ineligible Downfield"]


def test_detail_block_below_waist_and_chop_block() -> None:
    assert _details(
        [
            ("Rush", "Old Dominion Penalty, Block Below Waist (TJ Johnson) to the ODU 5 for a 1ST down"),
            ("Rush", "Montgomery rush middle for 1 yard PENALTY UCF Chop Block (King) 15 yards"),
        ]
    ) == ["Block Below the Waist", "Chop Block"]


def test_detail_running_into_the_kicker_and_touch_pass() -> None:
    assert _details(
        [
            ("Punt", "Jones punt for 40 yds PENALTY ISU Running Into The Kicker 5 yards"),
            ("Pass Incompletion", "PENALTY USD Illegal Touch-Pass (5 yards) to the USD 25"),
        ]
    ) == ["Running Into Kicker", "Illegal Touching"]


def test_detail_disconcerting_signals() -> None:
    assert _details(
        [("Penalty", "PENALTY PUR Disconcerting Signals (Powell,Mani) 5 yards from BSU25 to BSU30. NO PLAY.")]
    ) == ["Disconcerting Signals"]


def test_parenthesised_yardage_is_captured_as_a_number() -> None:
    """The `(15 Yards)` form must reach yds_penalty as a bare number.

    A regression guard rather than a demonstration: the primary extractor already
    handled these three inputs. What it locks in is that the column stays free of
    the punctuation that reached the release on 8,745 of 29,892 penalty rows
    (`'(10'`, `' (5'`, `'(-5'`) via the parenthesised-form fallback -- which was
    both the fragment-capture pattern the primary extractor had abandoned for
    producing garbage, and mis-grouped, three of its four branches always
    returning null.
    """
    out = _run(
        [
            ("Penalty", "Nebraska Penalty, Face Mask (15 Yards) (Ty Robinson) to the NEB 40"),
            ("Penalty", "Rutgers Penalty, Delay of Game (-5 Yards) to the RUT 49"),
            ("Rush", "Doug Brumfield run for 4 yds Penalty, Holding (10 yards) to the NMex 30"),
        ]
    )
    vals = out["yds_penalty"].to_list()
    for v in vals:
        assert v is None or "(" not in str(v), f"punctuation survived into yds_penalty: {v!r}"
    assert [None if v is None else int(v) for v in vals] == [15, -5, 10]


def test_implausible_penalty_yardage_is_rejected() -> None:
    """No penalty enforcement produces 35 yards. 401424416 p143 carries ESPN's own
    corrupt text ("1035 yards"), from which the parser recovered `'035'` -> 35.

    Past 25 yards, every one of the 53 such rows in 2022-24 is either the parser
    seizing a nearby number that is not the penalty (a rush gain, a field-goal
    distance, a kick distance on an offsetting flag) or ESPN stating an impossible
    one. Publishing 0 says "no reliable penalty yardage"; publishing 35 asserts a
    35-yard penalty. The bound stays loose -- legitimate values reach ~25, since
    ESPN reports net spot change on pass interference -- so only the unarguable
    cases are caught.
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    summary = json.loads((Path(__file__).parent / "fixtures" / "summary_401424416.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    import pytest as _pytest

    monkeypatch = _pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=401424416)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    row = plays.filter(pl.col("game_play_number") == 143).row(0, named=True)
    assert "1035 yards" in row["text"], "fixture drifted; this play should carry ESPN's corrupt yardage"
    assert row["penalty_yards_signed"] == 0, f"implausible yardage survived: {row['penalty_yards_signed']}"

    # and the game's other penalties keep theirs
    pens = plays.filter(pl.col("penalty_flag") == True)  # noqa: E712
    kept = pens.filter(pl.col("penalty_yards_signed") != 0)
    assert kept.height >= 10, "the bound should not be stripping ordinary penalties"
    assert kept.select(pl.col("penalty_yards_signed").abs().max()).item() <= 25


# --- enforcement-spot resolution ------------------------------------------------

_SPOT_ROW = {
    "homeTeamAbbrev": "LOU",
    "homeTeamName": "Louisville",
    "homeTeamNameAlt": "Louisville",
    "homeTeamMascot": "Cardinals",
    "awayTeamAbbrev": "FSU",
    "awayTeamName": "Florida State",
    "awayTeamNameAlt": "Florida St",
    "awayTeamMascot": "Seminoles",
}


def _spot(penalty_text: str, **over):
    from sportsdataverse.cfb.cfb_pbp import _parse_penalty_spot

    return _parse_penalty_spot({**_SPOT_ROW, "penalty_text": penalty_text, **over})


def test_spot_team_class_does_not_swallow_the_yardline() -> None:
    """The single character that mattered most in this parser.

    A team class of `[A-Za-z][A-Za-z0-9...]*` is greedy and matches digits, so
    "to the FLORIDAST13" captures team "FLORIDAST1" at the 3-yard line. Measured
    against end.yardsToEndzone on no-play penalties, that one character was the
    difference between 12.4% and 97.3% agreement.
    """
    assert _spot("FLORIDAST pass interference 14 yards to the FLORIDAST13, NO PLAY.") == "away:13"
    assert _spot("LOUISVILLE offside 5 yards to the LOUISVILLE25, NO PLAY.") == "home:25"


def test_spot_resolves_the_side_not_just_the_number() -> None:
    """A yardline is meaningless without knowing whose half it is in -- reading it
    as a distance is what published a flag against a team as a gain for it."""
    assert _spot(", False Start (Trey Benson) to the FlaSt 23") == "away:23"
    assert _spot(", Offensive Holding (10 Yards) to the Lvile 11") == "home:11"


def test_spot_takes_the_last_clause_as_the_enforcement_result() -> None:
    """The richer format states both spots; the from-spot is where the ball WAS."""
    assert _spot("FSU Holding (Smith,J) 10 yards from LOU46 to FSU44. NO PLAY.") == "away:44"


def test_spot_midfield_needs_no_side() -> None:
    """50 is its own complement, so the team token is absent and not needed."""
    assert _spot("Louisville Penalty, Personal Foul (TEAM) to the 50 yard line") == "mid:50"


def test_spot_is_none_rather_than_guessed_when_unresolvable() -> None:
    """A wrong side mirrors the field position and costs about 6 points of EP, so
    an unresolved token must stay absent."""
    assert _spot(", Illegal Shift (Amari Terry) declined") is None
    assert _spot(", Personal Foul (14 Yards)") is None
    assert _spot(None) is None
    # a token matching neither side must not be forced onto one
    assert _spot(", False Start to the ZZZQQ 20") is None


def test_spot_column_agrees_with_the_end_state_on_no_play_penalties() -> None:
    """End-to-end: on a NO PLAY penalty the ball ends AT the enforcement spot, so
    penalty_spot_yardsToEndzone and end.yardsToEndzone must agree. That identity is
    what validates the whole resolver -- measured at 97.6% across the fixture games
    with zero wrong-side errors, the residual being end-state defects rather than
    spot ones.
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 401636889
    summary = json.loads((Path(__file__).parent / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    v = plays.filter(
        (pl.col("penalty_no_play") == True)  # noqa: E712
        & pl.col("penalty_spot_yardsToEndzone").is_not_null()
        & pl.col("end.yardsToEndzone").is_not_null()
        & ~pl.col("type.text").str.contains("(?i)kickoff")
    )
    assert v.height >= 5, "fixture should carry several no-play penalties"
    mismatched = v.filter(pl.col("penalty_spot_yardsToEndzone") != pl.col("end.yardsToEndzone"))
    assert mismatched.height == 0, mismatched.select(
        "penalty_text", "penalty_spot_yardsToEndzone", "end.yardsToEndzone"
    ).rows()

    # and the kickoff penalty keeps the two concepts apart: the spot is the kicking
    # team's own 20, while the end state is the touchback (B3 part d.i)
    ko = plays.filter(
        pl.col("text").str.contains("(?i)kickoff") & pl.col("text").str.contains("(?i)Baylor Penalty, Unsportsmanlike")
    )
    assert ko.height == 1
    assert ko.row(0, named=True)["penalty_spot_yardline"] == 20
    assert ko.row(0, named=True)["end.yardsToEndzone"] == 75


def test_pre_2014_net_penalty_yardage_survives_the_bound() -> None:
    """The bound is era-aware because the text template changed what this field holds.

    Pre-2014 ESPN states a NET figure on a kickoff out of bounds -- "kickoff for 62
    yards out-of-bounds, Ucla penalty 32 yard illegal procedure accepted" -- which
    routinely exceeds the modern ~25 ceiling. Applying 25 to that era discarded its
    convention as corrupt: 164 rows in 2007 alone, ~278 across 2005-2013. Half the
    field is the invariant that still holds there.

    Game 252740026 p99 is the worked example (2005, Ucla, 32 yards).
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 252740026
    fixture = Path(__file__).parent / "fixtures" / f"summary_{gid}.json"
    if not fixture.exists():
        pytest.skip(f"fixture summary_{gid}.json not captured")
    summary = json.loads(fixture.read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    row = plays.filter(pl.col("text").str.contains("(?i)Ucla penalty 32 yard illegal procedure"))
    assert row.height == 1, "fixture drifted; expected the 32-yard out-of-bounds enforcement"
    r = row.row(0, named=True)
    assert r["penalty_yards_signed"] == 32, f"era-legitimate net yardage was discarded: {r['penalty_yards_signed']}"


def test_pre_2014_kickoff_penalty_gate_does_not_fire_on_a_described_kick() -> None:
    """ESPN wrote "for N yards" through 2013 and "for N yds" from 2014, a total
    changeover with no overlap season. The part (d) gate -- "this kickoff row
    describes no kick outcome" -- matched only "yds", so it classified 108 pre-2014
    rows as describing no kick when they plainly do.

    That misclassification was LATENT, not published: diffing six 2005 games across
    the fix, no row's end.yardsToEndzone or EPA moved, because those rows never met
    part (d)'s other conditions (a re-kick following, or possession continuing into
    the next play). This test therefore guards the classification, not a value that
    was once wrong -- the sub-condition would have fired the moment one of those
    other conditions held.

    Game 252740026 p99: "Justin Medlock kickoff for 62 yards out-of-bounds, Ucla
    penalty 32 yard illegal procedure accepted." A kick of 62 yards is described;
    the end state must be left alone.
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 252740026
    fixture = Path(__file__).parent / "fixtures" / f"summary_{gid}.json"
    if not fixture.exists():
        pytest.skip(f"fixture summary_{gid}.json not captured")
    summary = json.loads(fixture.read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    row = plays.filter(pl.col("text").str.contains("(?i)kickoff for 62 yards out-of-bounds"))
    assert row.height == 1
    r = row.row(0, named=True)
    # the touchback yardline is what part (d.i) would have written
    assert r["end.yardsToEndzone"] not in (75, 80), "part (d) fired on a kick that IS described"


# --- presentational-token stripping (v0.1.3 regression) -------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # the cases the formation stripping exists for
        ("No Huddle-Shotgun #1 C.Parker", "C.Parker"),
        ("dle-Shotgun #5 R.Marshall", "R.Marshall"),  # capture window truncates the tag
        ("le-Shotgun #20 N.Laughlin", "N.Laughlin"),
        ("[No Huddle, Shotgun], Brin, Davis", "Brin, Davis"),  # v0.1.3 left a stray "]"
        ("[Shotgun], Smith,Joe", "Smith,Joe"),
        # real surnames that CONTAIN a formation keyword. v0.1.3's trailing [\s\-]*
        # matches the empty string, so "Huddleston" was truncated to "ston" -- the
        # module comment asserted no surname contains "huddle", and 2014 text
        # ("Rakeem Cato sacked by Parrish Huddleston") disproves it.
        ("Parrish Huddleston", "Parrish Huddleston"),
        ("Chris Huddleston", "Chris Huddleston"),
        ("Sam Huddle", "Sam Huddle"),
        # both at once: the tag is stripped, the surname survives
        ("No Huddle-Shotgun Huddleston,Chris", "Huddleston,Chris"),
        ("J.Smith", "J.Smith"),
        # 2025's vendor template opens every play with the game clock, which a
        # capture anchored at the start of the text takes along. Published 2025
        # carries 3,682 kickoff_player_name values of this shape, plus rusher,
        # passer and interception names -- and 51,400 athlete_name values with a
        # TRUNCATED formation prefix, which is the same capture-window artifact.
        ("(15:00) #36 T.Morrison", "T.Morrison"),
        ("(05:04) #99 J.Firebaugh", "J.Firebaugh"),  # jersey needs 3 digits, not 2
        ("o Huddle-Shotgun #7 C.Williams", "C.Williams"),
        ("uddle-Shotgun #22 J.Smith", "J.Smith"),
        ("-Shotgun #2 R.Hammond Jr.", "R.Hammond Jr."),
        ("No Huddle-Shotgun #8 K.Ah Yat", "K.Ah Yat"),
        # a team-credited value must survive untouched
        ("TEAM", "TEAM"),
    ],
)
def test_formation_prefix_requires_a_word_end(raw: str, expected: str) -> None:
    from sportsdataverse.cfb.cfb_pbp import _strip_presentational_tokens

    out = pl.DataFrame({"n": [raw]}).with_columns(_strip_presentational_tokens(pl.col("n")).alias("o"))
    assert out["o"][0] == expected


# --- blocked-FG blocker name (capture-window truncation) ------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # ESPN appends the NEXT play's text after the name. The legacy (.{0,25})
        # window stops mid-debris and its cleanup trims at the first comma, which
        # arrives too late -- the clock comes first. 9 rows in published 2024, 1 in
        # 2023, every one exactly 25 characters, the window's ceiling.
        (
            "Colton Boomer 27 yd FG BLOCKED blocked by LaMareon James (00:07) Boomer,Colton "
            "field goal attempt from 27 yards NO GOOD",
            "LaMareon James",
        ),
        (
            "Grady Gross 32 yd FG BLOCKED blocked by Connor O'Toole (01:46) Gross,Grady "
            "field goal attempt from 32 yards NO GOOD",
            "Connor O'Toole",
        ),
        # ESPN credits some blocks to the team rather than a player, and writes the
        # RETURN clause straight after with a double space. TEAM is the right answer;
        # an \s+ between the optional team token and the name swallowed it and ran on
        # into the returner. Single spaces keep the name-shaped branch off this.
        ("Kyle Bullard 39 yd FG BLOCKED blocked by TEAM  Teu Kautai return for 12 yards", "TEAM"),
        # ordinary forms must be unaffected
        ("X 30 yd FG BLOCKED blocked by SAC Noah St-Juste", "Noah St-Juste"),
        ("X 30 yd FG BLOCKED blocked by #55 John Smith", "John Smith"),
    ],
)
def test_blocked_fg_blocker_name_is_not_capture_window_debris(text: str, expected: str) -> None:
    RX = (
        r"(?i)blocked by (?:(?-i:[A-Z]{2,6}) )?(?:#\d+ )?"
        r"((?-i:[A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]*){1,2}))"
    )
    LEGACY = r"(?i)blocked by (.{0,25})"
    out = pl.DataFrame({"t": [text]}).with_columns(
        pl.coalesce(
            pl.col("t").str.extract(RX, 1),
            pl.col("t")
            .str.extract(LEGACY)
            .str.replace(r",(.+)", "")
            .str.replace(r"blocked by ", "")
            .str.replace(r"  (.)+", ""),
        )
        .str.strip_chars()
        .alias("name")
    )
    assert out["name"][0] == expected


@pytest.mark.parametrize(
    ("text", "describes_a_kick"),
    [
        # 2004-2013
        ("Justin Medlock kickoff for 62 yards out-of-bounds, Ucla penalty 32 yard illegal procedure", True),
        # 2014+
        ("Enock Gota kickoff for 65 yds , Solomon Beebe return for 90 yds to the UAB -25", True),
        ("Nate Reed kickoff for 65 yds for a touchback", True),
        # 2025 vendor feed -- no "for" at all. Only 57% of 2025 kickoff rows carry
        # the 2014 form, so this is not a rare variant.
        ("(15:00) #36 T.Morrison kickoff 65 yards to the SAC00, Touchback", True),
        ("#99 J.Firebaugh onside kickoff 2 yards to the SAC37 PENALTY SAC Illegal Touch Of Kick", True),
        ("Kinaga,Yoann kickoff 0 yards to the ISU35 PENALTY ISU Delay Of Game", True),
        # penalty-only rows: no kick outcome stated. The bare "N Yards" here is the
        # PENALTY yardage, which is why the kick distance must be anchored on the
        # word kickoff rather than matched loosely -- matching loosely excludes
        # exactly the rows part (d) exists to repair.
        ("kickoff UTEP Penalty, Targeting on HAGOPIAN, Joe enforced (-15 Yards) to the UTEP 20", False),
        ("kickoff MURRAY ST Penalty, Delay Of Game (-5 Yards) to the MurrS 30", False),
        ("Mateen Bhaghani kickoff UCLA Penalty, unsportsmanlike conduct (Julian Armella) to the UCLA 20", False),
        ("Luke Akers kickoff Northwestern Penalty, unsportsmanlike conduct (-15 Yards) to the 50 yard line", False),
    ],
)
def test_part_d_kick_outcome_gate_across_all_three_vocabularies(text: str, describes_a_kick: bool) -> None:
    """Part (d) repairs a kickoff row only when its text describes NO kick outcome.

    Three vocabularies have to agree on that judgement: "for N yards" (2004-2013),
    "for N yds" (2014+), and the 2025 vendor feed's "kickoff N yards". Getting it
    wrong in one direction rewrites the end state of a kick that plainly happened;
    getting it wrong in the other leaves the enforcement-spot defect unrepaired.
    """
    gate = (
        r"(?i)kick(?:off)?\s+(?:for\s+)?-?\d+\s*(?:yds|yards)"
        r"|for \d+ (?:yds|yards)|touchback|return|out.of.bounds|recovered|downed|fair catch"
    )
    matched = pl.DataFrame({"t": [text]}).with_columns(pl.col("t").str.contains(gate).alias("m"))["m"][0]
    assert matched == describes_a_kick


# --- kick-return end state (B3 part (e)) ----------------------------------------


def _ret(text: str, **over):
    from sportsdataverse.cfb.cfb_pbp import _parse_return_spot

    row = {**_SPOT_ROW, "text": text, **over}
    return _parse_return_spot(row)


def test_return_spot_reads_all_three_era_forms() -> None:
    """The three vocabularies end the same way, so one spot regex serves them all."""
    # 2004-2013
    assert _ret("Preston Jones kickoff for 80 yards returned by N. Cruz for 24 yards to the FSU 24.") == "away:24"
    # 2014+
    assert _ret("Massimo Biscardi kickoff for 60 yds , Trey Sanders return for 15 yds to the LOU 20") == "home:20"
    # 2025 vendor feed: no space before the yardline, no "for"
    assert _ret("(02:31) #31 H.Smith kickoff 65 yards to the LOU00 #27 J.Norman return 22 yards to LOU08") == "home:8"


def test_return_spot_takes_the_last_spot_not_the_first() -> None:
    """A return can be followed by an enforcement that moves the ball again; the
    final mention is where it came to rest. 401762869 p122 is the worked case --
    the text names LOU00 at the catch and LOU08 after everything."""
    assert _ret("kickoff 65 yards to the LOU00 J.Norman return 22 yards to the FSU22 (tackle) to LOU08") == "home:8"


def test_return_spot_declines_touchdowns_and_nullified_returns() -> None:
    """Neither reports a spot the EP model should read: the scoring convention and
    the nullification both put the end state somewhere the text does not describe."""
    assert _ret("kickoff 65 yards , R.Hammond return 99 yards to the LOU00 TOUCHDOWN") is None
    assert _ret("kickoff 65 yards , T.Smith return 100 yards to the FSU00 TOUCHDOWN nullified by penalty") is None
    assert _ret("Nate Reed kickoff for 65 yds for a touchback") is None  # no return at all
    assert _ret(None) is None


def test_return_spot_is_none_rather_than_guessed() -> None:
    """A wrong side mirrors the field position, so an unresolvable team stays absent."""
    assert _ret("kickoff 60 yds , A.Player return for 15 yds to the ZZZQQ 20") is None


def test_return_spot_class_scope_is_kickoffs_only() -> None:
    """Part (e) touches kickoff returns and nothing else, and that scope is load
    bearing rather than incidental.

    Punt and interception returns look like candidates. Adjudicated against the
    next real play on 2025, the text beats the stored field 93-26 on punts and
    57-8 on interceptions. Widening to them was nonetheless BACKTESTED AND
    REJECTED: across the 26 rows it moved, the EPA distribution got worse rather
    than better -- rows above |EPA| 4 went 1 to 7 and the maximum 4.22 to 5.02 --
    and the new values disagree with the text as well as the field. "punt 58 yards
    to the LOU28 #5 C.Lacy return 21 yards to the LOU49" was rewritten to 80 when
    the return plainly ends around 51.

    The lesson is that the next-play agreement the gate tests can be satisfied by a
    value that is still wrong on these classes: beating the field is not the same
    as being right, since both can be wrong together. They stay out until that is
    understood.

    Fumble returns are out on their own evidence -- there the stored field beats
    the text 27 to 9.
    """
    assert _ret("(06:18) #37 D.Bale kickoff 67 yards to the LOU23 #6 D.Booth return 17 yards to LOU40") == "home:40"
    assert _ret("Ryan Rehkow punt for 45 yds , J.Smith return for 8 yds to the LOU 30") is None
    assert _ret("A.Jones pass intercepted by B.Lee return for 12 yds to the FSU 44") is None
    assert _ret("David Blough fumbled (aborted) at FSU 45, recovered by David Blough, returned to the FSU 45") is None


def test_spot_midfield_loses_to_a_later_team_qualified_spot() -> None:
    """CodeRabbit finding: the midfield clause was checked FIRST and returned
    immediately, so any text mentioning the 50 resolved there even when a later
    clause named the real enforcement spot. The contract is the LAST spot wins, so
    the two patterns have to compete on position.

    The comparison is >=, not >, because they match the same text at the same
    offset: _PENALTY_SPOT_RE's "the" is optional, so it also reads "to the 50" as
    team "the" at yardline 50. On a tie midfield is right -- the alternative
    resolves to no team at all, which is how "to the 50 yard line" started
    returning None while this was being fixed.
    """
    assert _spot("Penalty, Holding to the 50 yard line then enforced to the FlaSt 35") == "away:35"
    assert _spot("Penalty, Holding enforced to the FlaSt 35 and then to the 50 yard line") == "mid:50"
    assert _spot("Penalty, Personal Foul (TEAM) to the 50 yard line") == "mid:50"


def test_ep_between_is_not_folded_across_a_score() -> None:
    """B8: EP_between measures the UNEXPLAINED discontinuity since the last play.
    After a score there is none -- the points explain it -- and lag_EP_end is a
    realized 7.00 rather than a field-position expectation. The possession-change
    branch ADDS it, handing the next play a fictitious ~8-point swing.

    26 scrimmage penalties across 2005-2025 fall where the fold applies after a
    score, and 14 of them published |EPA| above 4. Zeroing the term takes that to
    zero and drops the maximum from 8.39 to 3.25.

    401628374 p64 is the worked case: a 6-yard completion with an unnecessary
    roughness flag, following a rushing touchdown, published at +8.08.
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 401628374
    fixture = Path(__file__).parent / "fixtures" / f"summary_{gid}.json"
    if not fixture.exists():
        pytest.skip(f"fixture summary_{gid}.json not captured")
    summary = json.loads(fixture.read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    row = plays.filter(pl.col("game_play_number") == 64)
    assert row.height == 1
    r = row.row(0, named=True)
    assert r["lag_scoringPlay"] is True, "fixture drifted; p63 should be the scoring play"
    assert r["EP_between"] == 0.0, "the fold must not cross a score"
    assert abs(r["EPA"]) < 4, f"a 6-yard completion with a flag is not an 8-point play (got {r['EPA']})"


def test_declined_and_offsetting_penalties_have_zero_penalty_epa() -> None:
    """B9: EPA_penalty isolates the PENALTY's effect, so a flag with no effect must
    read zero. A declined penalty leaves the play exactly as it was; an offsetting
    pair replays the down. Both were inheriting the PLAY's EP swing instead --
    "Buffalo Penalty, Offensive Holding (Yards) declined" published +2.81 -- on
    roughly 97% of such rows (286 of 296 in 2022, 158 of 161 in 2024, 314 of 319
    in 2025).
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 401628344
    fixture = Path(__file__).parent / "fixtures" / f"summary_{gid}.json"
    if not fixture.exists():
        pytest.skip(f"fixture summary_{gid}.json not captured")
    summary = json.loads(fixture.read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    declined = plays.filter(
        (pl.col("penalty_flag") == True)  # noqa: E712
        & ((pl.col("penalty_declined") == True) | (pl.col("penalty_offset") == True))  # noqa: E712
    )
    assert declined.height >= 1, "fixture drifted; expected at least one declined penalty"
    nonzero = declined.filter(pl.col("EPA_penalty").abs() > 0.001)
    assert nonzero.height == 0, nonzero.select("text", "EPA_penalty").rows()

    # accepted penalties keep a value
    accepted = plays.filter(
        (pl.col("penalty_flag") == True)  # noqa: E712
        & (pl.col("penalty_declined") != True)  # noqa: E712
        & (pl.col("penalty_offset") != True)  # noqa: E712
    )
    assert accepted.filter(pl.col("EPA_penalty").is_not_null()).height == accepted.height


def test_penalty_side_requires_two_independent_signals() -> None:
    """B10: penalty_yards_signed has a reliable magnitude but not a reliable sign
    (45% raw agreement with the enforcement direction), and any single substitute
    is not much better -- penalized_team alone runs 91.2%, and a 9% sign error
    MIRRORS the yardage. penalty_side therefore resolves only when two independent
    signals agree, and penalty_yards_net carries the offense-perspective signed
    yardage on those rows: 59.5% coverage at 99.62% accuracy over 19,608 labelled
    no-play rows, null on the rest.

    L1 (an automatic first down implies a defensive flag) is excluded on declined
    and offsetting rows, where the conversion came from the play standing --
    "Offensive Holding ... declined for a 1ST down" carries the flag on the
    OFFENSE.
    """
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 401628344
    fixture = Path(__file__).parent / "fixtures" / f"summary_{gid}.json"
    if not fixture.exists():
        pytest.skip(f"fixture summary_{gid}.json not captured")
    summary = json.loads(fixture.read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()

    plays = pl.from_dicts(out["plays"], infer_schema_length=None)
    pen = plays.filter(pl.col("penalty_flag") == True)  # noqa: E712

    # ground truth on no-play rows: the enforcement IS the observed movement
    lab = pen.filter(
        (pl.col("penalty_no_play") == True)  # noqa: E712
        & pl.col("start.yardsToEndzone").is_not_null()
        & pl.col("end.yardsToEndzone").is_not_null()
        & ((pl.col("start.yardsToEndzone") - pl.col("end.yardsToEndzone")) != 0)
        & pl.col("penalty_side").is_not_null()
    ).with_columns((pl.col("start.yardsToEndzone") - pl.col("end.yardsToEndzone")).alias("delta"))
    assert lab.height >= 2, "fixture drifted; expected resolvable no-play penalties"
    wrong_side = lab.filter(
        ((pl.col("penalty_side") == "def") & (pl.col("delta") < 0))
        | ((pl.col("penalty_side") == "off") & (pl.col("delta") > 0))
    )
    assert wrong_side.height == 0, wrong_side.select("penalty_detail", "penalty_side", "delta").rows()
    # and the net yardage reproduces the observed movement exactly on those rows
    mismatch = lab.filter(pl.col("penalty_yards_net") != pl.col("delta"))
    assert mismatch.height == 0, mismatch.select("penalty_detail", "penalty_yards_net", "delta").rows()

    # the declined first-down holding must NOT resolve
    declined_fd = pen.filter(
        (pl.col("penalty_declined") == True) & (pl.col("penalty_1st_conv") == True)  # noqa: E712
    )
    assert declined_fd.height >= 1
    assert declined_fd.filter(pl.col("penalty_side").is_not_null()).height == 0


def _run_fixture(gid: int):
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    summary = json.loads((Path(__file__).parent / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(mod, "download", lambda *a, **k: _Resp())
        proc = CFBPlayProcess(gameId=gid)
        proc.join_participants = False
        proc.espn_cfb_pbp()
        out = proc.run_processing_pipeline()
    finally:
        monkeypatch.undo()
    return pl.from_dicts(out["plays"], infer_schema_length=None)


def test_penalty_epa_direct_isolates_the_flag_from_the_play() -> None:
    """B11: EPA_penalty_direct is EP(actual end) - EP(counterfactual end), where the
    counterfactual is where the play would have ended with no flag: start minus
    the PLAY's own yardage. That is deliberately not end + penalty_yards_net,
    which is right for a tack-on and wrong for a spot-of-foul enforcement -- a
    22-yard run with offensive holding ends at start+3, and the flag's cost is
    the 22 yards plus 10, not 10.

    Two fixtures carry the two shapes. 401628344: an offensive holding on a
    6-yard run -- play-level EPA -0.62, EPA_penalty -0.44, but the flag alone
    -1.00, because the run was wiped as well as the 10. 401636889: a defensive
    holding tacked onto a 4-yard run -- EPA_penalty +1.75 credits the whole play,
    the flag alone is +0.63.
    """
    a = _run_fixture(401628344)
    off = a.filter(pl.col("EP_penalty_cf").is_not_null())
    assert off.height == 1, f"fixture drifted; expected 1 counterfactual row, got {off.height}"
    off = off.row(0, named=True)
    assert off["penalty_detail"] == "Offensive Holding"
    # counterfactual = start - play yards (6-yard run from the 46 -> 40); the actual
    # end is BEHIND the start because enforcement was from the spot of the foul
    assert off["penalty_cf_yardsToEndzone"] == 40
    assert off["end.yardsToEndzone"] > off["start.yardsToEndzone"]
    assert off["EPA_penalty_direct"] < off["EPA_penalty"] < 0, (off["EPA_penalty_direct"], off["EPA_penalty"])

    b = _run_fixture(401636889)
    de = b.filter(pl.col("EP_penalty_cf").is_not_null())
    assert de.height == 1, f"fixture drifted; expected 1 counterfactual row, got {de.height}"
    de = de.row(0, named=True)
    assert de["penalty_detail"] == "Defensive Holding"
    assert de["penalty_cf_yardsToEndzone"] == 22
    assert 0 < de["EPA_penalty_direct"] < de["EPA_penalty"], (de["EPA_penalty_direct"], de["EPA_penalty"])

    # sign agrees with the independently-resolved side on both
    for r in (off, de):
        assert (r["penalty_side"] == "def") == (r["EPA_penalty_direct"] > 0)

    # everything outside scope is null, never guessed; declined stays zero
    for plays in (a, b):
        assert plays.filter(pl.col("EP_penalty_cf").is_not_null() & (pl.col("penalty_no_play") == True)).height == 0  # noqa: E712
        declined = plays.filter(pl.col("penalty_declined") == True)  # noqa: E712
        assert declined.filter(pl.col("EPA_penalty_direct").abs() > 0.001).height == 0


def test_yds_sacked_takes_the_stated_loss_over_the_yardline() -> None:
    """2004-2007 text puts the yardline before the loss -- "sacked by Pierre Bell at
    the ECaro 48 for a loss of 8 yards" -- and the positional grab stored -48 for
    an 8-yard sack. 2,227 of 2,283 sacks in 2005, 2,395 of 2,424 in 2006 and 2,806
    of 2,843 in 2007 disagreed with their own "loss of N" clause. Found because the
    counterfactual built on yds_sacked produced a sign disagreement.

    A sack that states no loss at all ("sacked by Wendell Chavis, fumbled at the
    SMU 30, recovered by ...") is null rather than the yardline.
    """
    plays = _run_fixture(262940151)  # 2005, SMU @ East Carolina
    sacks = plays.filter(pl.col("sack") == True).with_columns(  # noqa: E712
        pl.col("text").str.extract(r"(?i)loss of (\d+)", 1).cast(pl.Int32, strict=False).alias("loss")
    )
    assert sacks.height >= 4, "fixture drifted; expected several sacks"
    stated = sacks.filter(pl.col("loss").is_not_null())
    assert stated.filter(pl.col("yds_sacked") != -pl.col("loss")).height == 0, stated.select(
        "yds_sacked", "loss", "text"
    ).rows()
    unstated = sacks.filter(pl.col("loss").is_null())
    assert unstated.filter(pl.col("yds_sacked").is_not_null()).height == 0, unstated.select("yds_sacked", "text").rows()
    assert sacks.filter(pl.col("yds_sacked").abs() > 25).height == 0


def test_penalty_side_first_down_signal_only_on_no_play_rows() -> None:
    """L1 (an automatic first down implies a defensive flag) holds only when the
    flag is the SOLE source of the first down, which is a no-play row. On a play
    that stands the conversion came from the play: "pass complete for 36 yards ...
    for a 1ST down, AIR FORCE penalty 10 yard Illegal Block" is an offensive foul,
    and L1 read it as defensive. The 362-game validation surfaced it as a sign
    disagreement with the counterfactual EP.
    """
    plays = _run_fixture(401628344)
    stands_fd = plays.filter(
        (pl.col("penalty_flag") == True)  # noqa: E712
        & (pl.col("penalty_no_play") == False)  # noqa: E712
        & (pl.col("penalty_1st_conv") == True)  # noqa: E712
        & (pl.col("type.text") != "Penalty")
        & (pl.col("penalty_detail").is_in(["Offensive Holding", "Illegal Block", "Holding"]))
    )
    # an offensive foul on a converting play must never resolve 'def'
    assert stands_fd.filter(pl.col("penalty_side") == "def").height == 0, stands_fd.select(
        "penalty_detail", "penalty_side", "text"
    ).rows()


def test_mirror_repair_reaches_scrimmage_penalty_rows() -> None:
    """Part (c) once excluded rows carrying a penalty, on the reasoning that a flag
    legitimately moves the ball between snaps. That guards a DISAGREEMENT between
    end and next start; it does not touch the exact-complement test, which with
    possession unchanged is the mirror signature whether or not a flag was thrown.

    2025 has 229 of 2,045 scrimmage-penalty rows (11.2%) stored as the exact
    complement of the next play's start; 2024 has one. 401760419 p104: "Personal
    Foul 11 yard from NEV21 to NEV1" was stored as 90 -- ninety yards from the
    goal for a team on the 1 -- and the counterfactual EP surfaced it as a sign
    disagreement.
    """
    plays = _run_fixture(401760419)
    row = plays.filter(pl.col("text").str.contains("(?i)Personal Foul 11 yard from NEV21 to NEV1"))
    assert row.height == 1, "fixture drifted; expected the NEV21->NEV1 personal foul"
    r = row.row(0, named=True)
    assert r["end.yardsToEndzone"] < 15, f"enforcement to the 1 must not read as {r['end.yardsToEndzone']} to go"
    # a defensive flag that hands the offense the 1-yard line is a gain, not a -4.8.
    # penalty_side is correctly NULL here -- Personal Foul is a mixed foul type and
    # L1 needs a no-play row -- so the sign is checked against the text, not the side.
    assert r["EP_penalty_cf"] is not None
    assert r["EPA_penalty_direct"] > 0, r["EPA_penalty_direct"]


def test_receiver_name_survives_the_2025_vendor_template() -> None:
    """The receiver capture is "to (.+)" plus a cleanup chain that never learned
    the 2025 vendor phrasing -- "to #3 A.Evans III caught at ARK40", "to J.Hayes
    thrown TCU45", "thrown to UM20 broken up by #1 G.Smith" -- so the regex
    fallback ran on into those clauses. The participant join hides it on the
    site, which is exactly why it went unnoticed: the fallback is what produced
    the phantom-player box-score rows in the first place and has to be right on
    its own. A bare yardline and a lone initial are nulled rather than published.
    """
    plays = _run_fixture(401752746)  # 2025, vendor template
    names = plays.filter(pl.col("receiver_player_name").is_not_null())["receiver_player_name"]
    assert names.len() >= 20, "fixture drifted; expected a normal number of targets"
    debris = names.filter(
        names.str.contains(r"(?i)caught at|thrown|broken up|intended for|defended by|^[A-Z]{2,6}\d{1,2},?$|^[A-Z]\.?$")
    )
    assert debris.len() == 0, debris.to_list()
    assert all(" " not in n or n.split()[-1] in ("Jr.", "III", "II", "IV") for n in names.to_list()), (
        names.unique().to_list()
    )


def test_lag_ep_end_skips_timeout_rows() -> None:
    """B12: lag_EP_end must carry the last REAL play's EP_end across a Timeout,
    not the Timeout row's own model score. The 2025 vendor template leaves stale
    state on Timeout rows ("Timeout TCU" at 4th & 9 carrying 2nd & 5 from two
    plays earlier), so the model's EP on that row is a phantom, and the lag was
    taken before the Timeout override corrected it. The play after the Timeout
    then saw a discontinuity that did not exist and, if it was a penalty play,
    folded it into EPA -- a safety published at -3.09 against -0.64 real.

    Across published 2025, penalty plays after a Timeout carried |EP_between|
    mean 1.88 against 0.18 elsewhere, with 25 above |EPA| 3.
    """
    plays = _run_fixture(401752753)
    p = {
        r["game_play_number"]: r
        for r in plays.filter(pl.col("game_play_number").is_between(123, 125)).iter_rows(named=True)
    }
    assert p[124]["type.text"] == "Timeout", "fixture drifted; p124 should be the Timeout"
    # the Timeout row itself is flat
    assert abs(p[124]["EP_end"] - p[124]["EP_start"]) < 1e-6
    assert p[124]["EPA"] == 0
    # and the play after it lags the last REAL play, not the Timeout's model value
    assert abs(p[125]["lag_EP_end"] - p[123]["EP_end"]) < 1e-6, (p[125]["lag_EP_end"], p[123]["EP_end"])


def test_all_zero_end_state_is_missing_and_backfilled_past_a_timeout() -> None:
    """B13: the 2025 vendor template ships an ALL-ZERO end state -- yardLine 0,
    yardsToEndzone 0, down 0, distance 0, team present -- on ordinary scrimmage
    plays. Nothing flagged it as missing (end_state_missing keyed on a null
    team), the <= 0 clamp read the 0 as the goal line and stored 99, and
    "R.Sharpe rush left for 41 yards gain to the MSU20" scored -2.52 EPA.
    Published rows ending at 99 with down 0 on a non-scoring, non-turnover
    scrimmage play: 88 in 2022, 6 in 2024, 103 in 2025.

    The backfill must also read the next REAL play: the row after this one is a
    Timeout carrying stale state (61), and the ensuing snap starts at 20.
    """
    plays = _run_fixture(401752753)
    r = plays.filter(pl.col("game_play_number") == 123).row(0, named=True)
    assert "R.Sharpe rush left for 41 yards" in r["text"], "fixture drifted"
    assert r["end.yardsToEndzone"] == 20, r["end.yardsToEndzone"]
    assert r["EPA"] > 0, f"a 41-yard run to the 20 is not a {r['EPA']:+.2f} play"
    # and nothing else in the game is left at the phantom 99 / down 0
    left = plays.filter(
        (pl.col("end.down") == 0)
        & (pl.col("end.yardsToEndzone") == 99)
        & (pl.col("scoring_play") != True)  # noqa: E712
        & (pl.col("is_turnover") != True)  # noqa: E712
        & ~pl.col("type.text").str.contains("(?i)kickoff|punt|field goal|timeout|end period")
    )
    assert left.height == 0, left.select("game_play_number", "type.text", "text").rows()


def test_last_row_of_a_live_game_has_a_win_probability() -> None:
    """B14: the most recent play of a game in progress. Every possession-change
    branch of wp_after borrows the NEXT play's wp_before, and the last row of a
    live game has no next play yet, so wp_after / wpa / home_wp_after came out
    null and rendered as 0.0% on the site -- on the row people are looking at.

    Simulated by truncating a completed game at a possession-changing play and
    marking it in progress. The fill must exist, and must land near the value
    the full game derives from the actual next play (the model's raw end-state
    prediction, by contrast, misses by 0.15-0.79 on such rows).
    """
    import copy
    import json
    from pathlib import Path

    import sportsdataverse.cfb.cfb_pbp as mod

    gid = 401644749
    full = json.loads((Path(__file__).parent / "fixtures" / f"summary_{gid}.json").read_text(encoding="utf-8"))

    def run(summary):
        class _Resp:
            def json(self):
                return summary

        mp = pytest.MonkeyPatch()
        try:
            mp.setattr(mod, "download", lambda *a, **k: _Resp())
            proc = CFBPlayProcess(gameId=gid)
            proc.join_participants = False
            proc.espn_cfb_pbp()
            return pl.from_dicts(proc.run_processing_pipeline()["plays"], infer_schema_length=None)
        finally:
            mp.undo()

    ref = run(full).sort("game_play_number")
    # pick a mid-game possession change with a real next play as the cut point
    cands = ref.filter(
        (pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
        & (pl.col("scoringPlay") == False)  # noqa: E712
        & (pl.col("period") == 2)
        & pl.col("wp_after").is_not_null()
        & pl.col("type.text").is_in(["Interception Return", "Fumble Recovery (Opponent)", "Punt"])
    )
    assert cands.height >= 1, "fixture drifted; no mid-game possession change to cut at"
    cut = cands.row(0, named=True)
    truth = cut["wp_after"]

    # truncate the payload after that play and mark the game in progress
    trunc = copy.deepcopy(full)
    drives = trunc["drives"]["previous"]
    kept, done = [], False
    for d in drives:
        plays = []
        for p in d["plays"]:
            plays.append(p)
            if str(p.get("id")) == str(cut["id"]):
                done = True
                break
        d = dict(d, plays=plays)
        kept.append(d)
        if done:
            break
    trunc["drives"]["previous"] = kept
    trunc["header"]["competitions"][0]["status"]["type"]["completed"] = False
    live = run(trunc).sort("game_play_number")
    last = live.row(-1, named=True)
    assert str(last["id"]) == str(cut["id"]), "truncation did not end on the chosen play"
    assert last["status_type_completed"] is False
    assert last["wp_after"] is not None and last["wpa"] is not None and last["home_wp_after"] is not None
    assert abs(last["wp_after"] - truth) < 0.10, (last["wp_after"], truth)
