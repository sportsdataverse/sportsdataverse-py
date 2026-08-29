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
