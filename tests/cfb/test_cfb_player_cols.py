"""Regression tests for play-text player-name extraction in ``__add_player_cols``.

These guard the multi-alternative ``str.extract`` group-index fix: polars
``str.extract`` defaults to capture group 1, but ESPN play text matches
non-first alternatives of the extraction patterns (e.g. "rush" is the 3rd
alternative of the rusher pattern, so its name lands in group 3). The default
group-1 extract therefore returned ``None`` for every ESPN "rush for" play --
masked for 2014+ games (``__join_participants`` overwrites with structured
participant names) but null for pre-2014 games, which have no participant array.

The fix coalesces across every capture group so the matched alternative's name
is returned regardless of its position. See ``_extract_player_name`` in
``cfb_pbp.py``.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess


def _run_player_cols(rows: list[dict]) -> pl.DataFrame:
    """Drive the (name-mangled) private ``__add_player_cols`` on synthetic rows."""
    proc = CFBPlayProcess(gameId=1)
    df = pl.DataFrame(rows)
    return proc._CFBPlayProcess__add_player_cols(df)


def _row(
    text: str, *, rush=False, pass_=False, sack_vec=False, sack=False, fumble_vec=False, type_text="Rush"
) -> dict[str, str | bool]:
    return {
        "text": text,
        "rush": rush,
        "pass": pass_,
        "sack_vec": sack_vec,
        "sack": sack,
        "fumble_vec": fumble_vec,
        "type.text": type_text,
    }


def test_rusher_extracted_from_espn_rush_text():
    """ESPN uses '{Name} rush for {N} yards' (3rd pattern alternative). The
    group-index bug returned None here; the fix must extract the rusher."""
    out = _run_player_cols(
        [
            _row("Bryant Moniz rush for 22 yards to the Hawa 39 for a 1ST down.", rush=True, type_text="Rush"),
            _row("Joey Iosefa rush for 1 yard to the Hawa 17.", rush=True, type_text="Rush"),
            _row("John Smith run for 5 yards", rush=True, type_text="Rush"),  # CFBD-style 'run' (group 1)
        ]
    )
    assert out["rusher_player_name"].to_list() == ["Bryant Moniz", "Joey Iosefa", "John Smith"]


def test_passer_and_receiver_extracted():
    out = _run_player_cols(
        [
            _row(
                "Tom Brady pass complete to Randy Moss for 15 yards to the NE 40.",
                pass_=True,
                type_text="Pass Reception",
            )
        ]
    )
    assert out["passer_player_name"][0] == "Tom Brady"
    assert out["receiver_player_name"][0] == "Randy Moss"


def test_punter_extracted_from_both_phrasings():
    """'{Name} punt' (group 1) AND 'Punt by {Name}' (group 2) must both resolve."""
    out = _run_player_cols(
        [
            _row("Scott Harding punt for 41 yards, John Doe returns for 5 yards.", type_text="Punt"),
            _row("Punt by Scott Harding for 41 yards", type_text="Punt"),
        ]
    )
    assert out["punter_player_name"].to_list() == ["Scott Harding", "Scott Harding"]


def test_kickoff_specialist_extracted():
    out = _run_player_cols(
        [
            _row("Joe Kicker kickoff for 65 yards", type_text="Kickoff"),
            _row("Joe Kicker on-side kick to the 45", type_text="Kickoff"),  # 'on-side' = group 2
        ]
    )
    assert out["kickoff_player_name"].to_list() == ["Joe Kicker", "Joe Kicker"]


def test_punt_returner_extracted_from_returns_and_returned_by():
    """'..., {Name} returns' (group 1) AND '..., returned by {Name}' (group 3)."""
    out = _run_player_cols(
        [
            _row("Scott Harding punt for 41 yards, John Doe returns for 5 yards to the 30.", type_text="Punt"),
            _row("Scott Harding punt for 41 yards, returned by John Doe for 5 yards.", type_text="Punt"),
        ]
    )
    assert out["punt_return_player_name"].to_list() == ["John Doe", "John Doe"]


def test_fg_kicker_extracted_after_digit_escape_fix():
    """The fg-kicker pattern used ``\\\\d`` (literal backslash) instead of ``\\d``,
    so it matched nothing. Fixing the escape lets the kicker name extract."""
    out = _run_player_cols([_row("Brett Baer 47 Yd Field Goal Good", type_text="Field Goal Good")])
    assert out["fg_kicker_player_name"][0] == "Brett Baer"


def test_kickoff_returner_extracted_without_yardage_tail():
    """'returned by {Name} for N yards' must not leak the yardage tail into the name."""
    out = _run_player_cols(
        [
            _row(
                "Tyler Hadden kickoff for 64 yards returned by Arthur Jaffee for 4 yards to the 30.",
                type_text="Kickoff Return",
            ),
            _row("Joe Kicker kickoff, John Doe return for 22 yards.", type_text="Kickoff Return"),
        ]
    )
    assert out["kickoff_return_player_name"].to_list() == ["Arthur Jaffee", "John Doe"]


def test_fumble_player_not_blanked_by_yardage_digits():
    """Regression: after the ``\\d`` escape fix, the greedy ``(.+)(\\d{1,2})`` cleanup
    erased the whole fumbler capture whenever a yardage digit remained. It was
    removed -- the name must survive."""
    out = _run_player_cols(
        [_row("John Doe fumbled at the 25, recovered by Smith.", rush=True, fumble_vec=True, type_text="Rush")]
    )
    fp = out["fumble_player_name"][0]
    assert fp not in (None, ""), f"fumble_player blanked: {fp!r}"


def test_fumble_forced_player_extracted():
    out = _run_player_cols(
        [
            _row(
                "Joe Back rush for 5 yards, fumbled, forced by Bill Smith, recovered by Tom Jones at the 40.",
                rush=True,
                fumble_vec=True,
                type_text="Rush",
            )
        ]
    )
    assert out["fumble_forced_player_name"][0] == "Bill Smith"


def test_roster_player_id_attach_team_aware():
    """__attach_player_ids resolves {type}_player_id from the game roster, keyed by
    the player's team so an identical name on opposing rosters doesn't collide."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 100, "full_name": "Joe Back", "team_id": 10},
        {"athlete_id": 200, "full_name": "Sam Sack", "team_id": 20},
        {"athlete_id": 300, "full_name": "Joe Back", "team_id": 20},  # same name, other team
    ]
    df = pl.DataFrame(
        [{"rusher_player_name": "Joe Back", "sack_player_name": "Sam Sack", "pos_team": 10, "def_pos_team": 20}]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    # team-aware: pos_team=10 -> Joe Back@10 (not the @20 namesake); def_pos_team=20 -> Sam Sack@20
    assert out["rusher_player_id"][0] == 100
    assert out["sack_player_id"][0] == 200


def test_garbage_player_name_nulled():
    """Obvious play-text artifacts ('... loss ...') are nulled before the id-join."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    df = pl.DataFrame([{"rusher_player_name": "bea loss of", "pos_team": 10}])
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["rusher_player_name"][0] is None


def test_pre2014_team_abbrev_suffix_stripped_and_id_resolved():
    """2004-era text renders "Player Name (TEAM)"; the passer capture keeps the
    parenthetical. It must be shed so the name column is clean AND the roster
    id-join lands (previously "matt leinart usc" never matched "matt leinart").

    Real strings from cfb-raw game 242410259 (2004 VT/USC).
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 100, "full_name": "Matt Leinart", "team_id": 30},
        {"athlete_id": 200, "full_name": "Bryan Randall", "team_id": 40},
        {"athlete_id": 300, "full_name": "Reggie Bush", "team_id": 30},
    ]
    df = pl.DataFrame(
        [
            {"passer_player_name": "Matt Leinart (USC)", "pos_team": 30, "return_team": 30},
            {"passer_player_name": "Bryan Randall (VT)", "pos_team": 40, "return_team": 30},
            {"passer_player_name": "Reggie Bush (USC)", "pos_team": 30, "return_team": 30},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_name"].to_list() == ["Matt Leinart", "Bryan Randall", "Reggie Bush"]
    assert out["passer_player_id"].to_list() == [100, 200, 300]


def test_narrative_tail_stripped_from_captured_name():
    """Pass-direction words and ESPN's missing-space concatenations bleed into the
    capture on 2005-2013 text. Real strings sampled from cfb-raw 2010-2013.
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 11, "full_name": "Russell Wilson", "team_id": 50},
        {"athlete_id": 22, "full_name": "Dominique Davis", "team_id": 50},
        {"athlete_id": 33, "full_name": "Raynard Horne", "team_id": 50},
    ]
    df = pl.DataFrame(
        [
            {"passer_player_name": "Russell Wilson sideline", "pos_team": 50},
            {"passer_player_name": "Russell Wilson deep out", "pos_team": 50},
            {"passer_player_name": "Dominique Davis screen", "pos_team": 50},
            {"passer_player_name": "Dominique Davis crossing", "pos_team": 50},
            {"passer_player_name": "Raynard Hornetackled by", "pos_team": 50},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_name"].to_list() == [
        "Russell Wilson",
        "Russell Wilson",
        "Dominique Davis",
        "Dominique Davis",
        "Raynard Horne",
    ]
    assert out["passer_player_id"].to_list() == [11, 11, 22, 22, 33]


def test_fallback_join_tiers_are_team_scoped_and_uniqueness_gated():
    """Below exact match: first-initial+last, then bare surname -- both scoped to
    the fielding team and only when unique within it. An ambiguous surname must
    stay null rather than guess.
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 1, "full_name": "Anthony Smith", "team_id": 60},
        {"athlete_id": 2, "full_name": "Cornell Brockington", "team_id": 60},
        {"athlete_id": 3, "full_name": "Devon Jones", "team_id": 60},
        {"athlete_id": 4, "full_name": "Marcus Jones", "team_id": 60},  # surname collision
    ]
    df = pl.DataFrame(
        [
            {"rusher_player_name": "A. Smith", "pos_team": 60},  # initial tier
            {"rusher_player_name": "Brockington", "pos_team": 60},  # unique-surname tier
            {"rusher_player_name": "Jones", "pos_team": 60},  # ambiguous -> null
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["rusher_player_id"].to_list() == [1, 2, None]


def _box(team_id, athletes):
    """Minimal ESPN summary boxscore shaped for _parse_espn_player_box."""
    return {
        "boxscore": {
            "players": [
                {
                    "team": {"id": team_id, "abbreviation": "AAA"},
                    "statistics": [
                        {
                            "name": "passing",
                            "keys": ["completions"],
                            "athletes": [
                                {"athlete": {"id": aid, "displayName": nm}, "stats": ["1"]} for aid, nm in athletes
                            ],
                        }
                    ],
                }
            ]
        }
    }


def test_boxscore_resolves_ids_when_roster_is_empty():
    """ESPN 404s the roster resource on a large minority of games (376 of 898 in
    2018). The per-player box score carries the same athlete-id namespace and is
    already in the summary, so it must still resolve ids with no roster at all.
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = None  # the 2018 failure mode
    proc.json = _box(60, [("777", "Sam Hartman"), ("888", "Gardner Minshew II")])
    df = pl.DataFrame(
        [
            {"passer_player_name": "Sam Hartman", "pos_team": 60},
            {"passer_player_name": "Gardner Minshew II", "pos_team": 60},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_id"].to_list() == [777, 888]


def test_boxscore_supplements_a_partial_roster_without_displacing_it():
    """The box score is additive: roster-resolved ids stay, box-only players get
    resolved, and the two sources agreeing on a name keeps it unambiguous."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [{"athlete_id": 111, "full_name": "Rostered Passer", "team_id": 60}]
    proc.json = _box(
        60,
        [
            ("111", "Rostered Passer"),  # same id in both sources -> still unique
            ("222", "Box Only Passer"),  # absent from the roster
        ],
    )
    df = pl.DataFrame(
        [
            {"passer_player_name": "Rostered Passer", "pos_team": 60},
            {"passer_player_name": "Box Only Passer", "pos_team": 60},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_id"].to_list() == [111, 222]


def test_conflicting_ids_across_sources_stay_null():
    """If roster and box disagree on a name's id, the name is ambiguous and must
    resolve to null rather than silently picking one."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [{"athlete_id": 111, "full_name": "Disputed Name", "team_id": 60}]
    proc.json = _box(60, [("999", "Disputed Name")])
    df = pl.DataFrame([{"passer_player_name": "Disputed Name", "pos_team": 60}])
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_id"][0] is None


def test_legitimate_surname_colliding_with_a_stopword_is_preserved():
    """A real surname that IS one of the tail stopwords must survive.

    Blanket tail-stripping would turn "John Middle" into "John", and the surname
    fallback could then resolve it to a different athlete entirely. The strip is
    therefore only applied when the roster confirms the trimmed form.
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 501, "full_name": "John Middle", "team_id": 80},
        {"athlete_id": 502, "full_name": "Alex Screen", "team_id": 80},
        {"athlete_id": 503, "full_name": "John Passer", "team_id": 80},
    ]
    df = pl.DataFrame(
        [
            {"rusher_player_name": "John Middle", "pos_team": 80},
            {"rusher_player_name": "Alex Screen", "pos_team": 80},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["rusher_player_name"].to_list() == ["John Middle", "Alex Screen"]
    assert out["rusher_player_id"].to_list() == [501, 502]


def test_tail_word_does_not_resolve_via_the_surname_fallback():
    """A narrative tail word is itself a plausible surname, so the fuzzy tiers
    must not run until the tail is stripped.

    With "Russell Wilson" and "Alex Screen" both rostered, the captured text
    "Russell Wilson screen" hit the surname tier on the UNTRIMMED name and
    resolved to Alex Screen -- silent misattribution to the wrong player.
    """
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [
        {"athlete_id": 701, "full_name": "Russell Wilson", "team_id": 70},
        {"athlete_id": 702, "full_name": "Alex Screen", "team_id": 70},
    ]
    df = pl.DataFrame(
        [
            {"passer_player_name": "Russell Wilson screen", "pos_team": 70},
            {"passer_player_name": "Alex Screen", "pos_team": 70},  # exact still wins
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_name"].to_list() == ["Russell Wilson", "Alex Screen"]
    assert out["passer_player_id"].to_list() == [701, 702]


def test_name_that_is_entirely_a_tail_word_resolves_to_null():
    """If stripping the tail leaves nothing, there is no name to match -- the
    surname tier must not rescue it into a real athlete."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [{"athlete_id": 801, "full_name": "Alex Screen", "team_id": 70}]
    df = pl.DataFrame([{"passer_player_name": "screen", "pos_team": 70}])
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["passer_player_id"][0] is None


def test_unmatched_name_keeps_its_tail_rather_than_guessing():
    """With no roster evidence that a tail is narrative bleed, trimming would be a
    guess -- so the captured name is left as-is and the id stays null."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    proc.game_roster = [{"athlete_id": 601, "full_name": "Someone Else", "team_id": 90}]
    df = pl.DataFrame([{"rusher_player_name": "Unknown Guy sideline", "pos_team": 90}])
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["rusher_player_name"][0] == "Unknown Guy sideline"
    assert out["rusher_player_id"][0] is None


def test_real_surname_not_damaged_by_tail_stripper():
    """The tail stripper is end-anchored on a closed stopword list; ordinary names
    (including a legitimate parenthetical-free name) must pass through intact."""
    proc = CFBPlayProcess(gameId=1)
    proc.join_participants = False
    df = pl.DataFrame(
        [
            {"rusher_player_name": "Middleton Banks", "pos_team": 70},
            {"rusher_player_name": "Deep Patel", "pos_team": 70},
            {"rusher_player_name": "LenDale White", "pos_team": 70},
        ]
    )
    out = proc._CFBPlayProcess__attach_player_ids(df)
    assert out["rusher_player_name"].to_list() == ["Middleton Banks", "Deep Patel", "LenDale White"]
