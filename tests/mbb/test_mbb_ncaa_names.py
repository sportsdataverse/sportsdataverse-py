"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_names` (Task 5b.3).

Every ``test_convert_from_*`` / ``test_box_aware_compare_*`` /
``test_fuzzy_box_match_*`` case is a 1:1 transliteration of an inline
``utest`` literal from ``LineupErrorAnalysisUtilsTests.scala`` (read-only
cbb-explorer clone):

* ``convert_from_initials`` block (``:22-43``).
* ``convert_from_digits`` block (``:44-53``).
* ``Fixer.box_aware_compare`` block (``:124-183``) -- the 10 "not trivial"
  survivors of the oracle's own filter over its 50-pair ``legacy_misspellings``
  fixture (everything else resolves ``StrongSurnameMatch`` and is filtered
  out by the Scala test itself). See
  :mod:`sportsdataverse.mbb.mbb_ncaa_names`'s FUZZY-MATCH PARITY docstring
  section for why 2 of the 10 ``WeakSurnameMatch`` scores are off by one
  point from the Java oracle (both stay under the 70 threshold, so the
  classification and winning ``box_name`` are unaffected -- the pass bar per
  the Task 5b.3 protocol).
* ``Fixer.fuzzy_box_match`` block (``:185-269``) -- all 9 direct scenarios.

The ``tidy_player`` block (``:17-21``) is empty upstream (a ``//TODO`` noting
it's exercised end-to-end by ``ExtractorUtils.build_partial_lineup_list``'s
oracle, Task 5b.5) and ``validate_lineup`` / the clump-fixing blocks
(``:55-120``, ``:122-... NameFixer`` sibling) are the stint-VALIDATION half
of ``LineupErrorAnalysisUtils`` -- **out of scope for this module** (Task
5d owns it). Both are skipped here; ``tidy_player`` /
``build_tidy_player_context`` instead get hand-written smoke tests below
(no Scala oracle exists yet), each traced by hand against the ported
algorithm and noted with the expected intermediate values.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerId,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_names import (
    FuzzyMatchError,
    NoSurnameMatch,
    StrongSurnameMatch,
    WeakSurnameMatch,
    box_aware_compare,
    build_tidy_player_context,
    convert_from_digits,
    convert_from_initials,
    fuzzy_box_match,
    tidy_player,
)
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code


def _lineup_event(
    players: list[PlayerCodeId],
    players_out: list[PlayerCodeId] | None = None,
    team: TeamId | None = None,
) -> LineupEvent:
    """Minimal box-score :class:`LineupEvent` fixture -- only the fields
    ``build_tidy_player_context`` / ``tidy_player`` actually read
    (``players``, ``players_out``, ``team.team``) are meaningful; the rest
    are filler."""
    return LineupEvent(
        date=datetime(2020, 1, 1),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=0.0,
        duration_mins=0.0,
        score_info=ScoreInfo.empty(),
        team=TeamSeasonId(team or TeamId("Test Team"), Year(2020)),
        opponent=TeamSeasonId(TeamId("Other Team"), Year(2020)),
        lineup_id=LineupId.unknown,
        players=players,
        players_in=[],
        players_out=players_out or [],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )


# --- convert_from_initials (utest :22-43) -----------------------------------

_INITIAL_SET = {"AoBo": "name1", "RoBo": "name2", "RaBa": "name3"}


@pytest.mark.parametrize("name", ["", "A", "A ", "A C Jr", "ABC", "A D"])
def test_convert_from_initials_failures(name: str) -> None:
    """utest ``:23-27``."""
    assert convert_from_initials(name, _INITIAL_SET) is None


def test_convert_from_initials_a_b() -> None:
    """utest ``:28-30``."""
    assert convert_from_initials("A B", _INITIAL_SET) == "name1"


def test_convert_from_initials_reversed() -> None:
    """utest ``:31-33`` -- ``"B, A"`` is the surname-first shorthand for the same pair."""
    assert convert_from_initials("B, A", _INITIAL_SET) == "name1"


def test_convert_from_initials_wrong_order() -> None:
    """utest ``:34-36``."""
    assert convert_from_initials("A, B", _INITIAL_SET) is None


def test_convert_from_initials_multi_match_space() -> None:
    """utest ``:37-39``."""
    assert convert_from_initials("R B", _INITIAL_SET) is None


def test_convert_from_initials_multi_match_comma() -> None:
    """utest ``:40-42``."""
    assert convert_from_initials("B, R", _INITIAL_SET) is None


# --- convert_from_digits (utest :44-53) -------------------------------------


def test_convert_from_digits() -> None:
    """utest ``:44-53``."""
    codes = [
        PlayerCodeId(code="1", id=PlayerId("bad_name")),
        PlayerCodeId(code="1000", id=PlayerId("name1")),
    ]
    assert [convert_from_digits(name, codes) for name in ["1x", "100", "1000"]] == [None, None, "name1"]


# --- box_aware_compare (utest "Fixer.box_aware_compare" :124-183) -----------


def test_box_aware_compare_no_surname_match_guity_amaya() -> None:
    """utest ``:143-146``."""
    result = box_aware_compare("FINKLEA,AMAYA", "GUITY,AMAYA")
    assert isinstance(result, NoSurnameMatch)
    assert result.box_name == "GUITY,AMAYA"
    assert result.exact_first_name == "amaya"
    assert result.near_first_name is None


def test_box_aware_compare_weak_tuitele_peanut() -> None:
    """utest ``:147-150`` -- rapidfuzz overall score 68 vs Java's 67 (parity
    note); both are < 70 so the classification is unaffected."""
    result = box_aware_compare("sirena tuitele", "Tuitele, Peanut")
    assert isinstance(result, WeakSurnameMatch)
    assert result.box_name == "Tuitele, Peanut"
    assert result.score < 70


def test_box_aware_compare_weak_osborne_john_from_osbrone_malik() -> None:
    """utest ``:151-154`` -- exact Java parity (score 59)."""
    result = box_aware_compare("Osbrone, Malik", "Osborne, John")
    assert isinstance(result, WeakSurnameMatch)
    assert result.box_name == "Osborne, John"
    assert result.score == 59


def test_box_aware_compare_no_surname_osborne_john_from_stranger() -> None:
    """utest ``:155-158``."""
    result = box_aware_compare("Stranger, John", "Osborne, John")
    assert isinstance(result, NoSurnameMatch)
    assert result.box_name == "Osborne, John"
    assert result.exact_first_name == "john"
    assert result.near_first_name is None


def test_box_aware_compare_no_surname_khalil_ali() -> None:
    """utest ``:159-162`` -- the "Khalil, Ali, Jr." case the oracle's own
    filter deliberately keeps even though it's a match, not a mismatch."""
    result = box_aware_compare("Ali", "Khalil, Ali, Jr.")
    assert isinstance(result, NoSurnameMatch)
    assert result.box_name == "Khalil, Ali, Jr."
    assert result.exact_first_name == "ali"
    assert result.near_first_name is None


def test_box_aware_compare_weak_james_onome() -> None:
    """utest ``:163-166`` -- rapidfuzz overall score 63 vs Java's 64 (parity
    note); both are < 70."""
    result = box_aware_compare("Akinbode-James, O.", "James, Onome")
    assert isinstance(result, WeakSurnameMatch)
    assert result.box_name == "James, Onome"
    assert result.score < 70


def test_box_aware_compare_weak_pryor_dearica() -> None:
    """utest ``:167-170`` -- exact Java parity (score 69)."""
    result = box_aware_compare("Dee Dee Pryor", "Pryor, DeArica")
    assert isinstance(result, WeakSurnameMatch)
    assert result.box_name == "Pryor, DeArica"
    assert result.score == 69


def test_box_aware_compare_weak_fanord_donalson() -> None:
    """utest ``:171-174`` -- exact Java parity (score 38)."""
    result = box_aware_compare("Jonathan Fanard", "Fanord, Donalson")
    assert isinstance(result, WeakSurnameMatch)
    assert result.box_name == "Fanord, Donalson"
    assert result.score == 38


def test_box_aware_compare_no_surname_parchman_omar() -> None:
    """utest ``:175-178``."""
    result = box_aware_compare("PATTERSON,OMAR", "PARCHMAN,OMAR")
    assert isinstance(result, NoSurnameMatch)
    assert result.box_name == "PARCHMAN,OMAR"
    assert result.exact_first_name == "omar"
    assert result.near_first_name is None


def test_box_aware_compare_no_surname_wilson_kobe() -> None:
    """utest ``:179-182``."""
    result = box_aware_compare("10", "WILSON,KOBE")
    assert isinstance(result, NoSurnameMatch)
    assert result.box_name == "WILSON,KOBE"
    assert result.exact_first_name is None
    assert result.near_first_name is None


@pytest.mark.parametrize(
    ("candidate", "box_name"),
    [
        ("SARION,MCGEE", "MCGEE,SARION"),
        ("Korneila Wright", "Wright, Kay Kay"),
        ("SADARIUS,BOWSER", "BOWSER,SADARIUS"),
        ("B.J. Greenlee", "Greenlee, Bryan"),
        ("JR., RIDEAU", "RIDEAU, FLOYD"),
    ],
)
def test_box_aware_compare_trivial_strong_matches_stay_strong(candidate: str, box_name: str) -> None:
    """Supplement, drawn from the same ``legacy_misspellings`` fixture the
    Scala test filters out as "trivial" (i.e. NOT in the 10-case oracle
    above). These are comma-joined, multi-token pairs the parity probe used
    to catch a real threshold-crossing miss: without
    ``processor=rapidfuzz.utils.default_process`` (see the module's
    FUZZY-MATCH PARITY docstring) these landed as ``WeakSurnameMatch``
    instead of the correct ``StrongSurnameMatch``.
    """
    result = box_aware_compare(candidate, box_name)
    assert isinstance(result, StrongSurnameMatch)
    assert result.score >= 70


# --- fuzzy_box_match (utest "Fixer.fuzzy_box_match" :185-269) --------------


def test_fuzzy_box_match_single_strong_match() -> None:
    """utest ``:187-194``."""
    result = fuzzy_box_match(
        "sirena tuitele",
        ["Suitele, Sirena", "Tuitele, Peanut", "Guity, Amaya", "Pryor, DeArica", "Guity, Robison"],
        "test1c",
    )
    assert result == "Suitele, Sirena"


def test_fuzzy_box_match_multiple_strong_clear_winner() -> None:
    """utest ``:196-203``."""
    result = fuzzy_box_match(
        "Jones, Mike",
        ["Jones, Bates", "Kristensen, David", "Jones, Michael", "Collins, Carter", "Brajkovic, Luka"],
        "test1b",
    )
    assert result == "Jones, Michael"


def test_fuzzy_box_match_multiple_strong_error() -> None:
    """utest ``:205-212``."""
    result = fuzzy_box_match(
        "sirena tuitele",
        ["Suitele, Sirena", "Tuitele, Irena", "Guity, Amaya", "Pryor, DeArica", "Guity, Robison"],
        "test1a",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.1A" in result.message


def test_fuzzy_box_match_single_weak_match() -> None:
    """utest ``:214-221``."""
    result = fuzzy_box_match(
        "sirena tuitele",
        ["Tuitele, Peanut", "Guity, Amaya", "Pryor, DeArica", "Guity, Sirena"],
        "test2b",
    )
    assert result == "Tuitele, Peanut"


def test_fuzzy_box_match_multiple_weak_error() -> None:
    """utest ``:223-230``."""
    result = fuzzy_box_match(
        "sirena tuitele",
        ["Tuitele, Peanut", "Tuitele, Rabbit", "Guity, Amaya", "Pryor, DeArica", "Guity, Robison"],
        "test1b",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.2A" in result.message


def test_fuzzy_box_match_unique_first_name_resolves() -> None:
    """utest ``:232-241``, INVERTED -- a DELIBERATE divergence from the Scala
    oracle, which retired this branch ("3C") and errored on a unique
    first-name-only match.

    The upstream fixture argues our side: `FINKLEA,AMAYA` against a box
    carrying `Guity, Amaya` is Amaya **Finklea-Guity** -- one compound
    surname, split across the two pages. The oracle rejected a correct match.

    Rejecting is not the conservative choice. It is how name-changed players
    are deleted: the NCAA retro-updates the box to a player's CURRENT
    surname while the play-by-play keeps the surname as of the game, so the
    surname gate scores zero and the first name is the only signal left.
    An unresolved sub leaves the on-court set at 4 or 6, every stint is
    flagged, and the team yields zero good stints for that game.

    Measured over 800 real games before flipping: 13 unique bindings, all
    correct (9 surname changes, 3 misspellings, 1 scorer typo), 0 false
    positives. See the comment in `fuzzy_box_match` for the full table.
    """
    result = fuzzy_box_match(
        "FINKLEA,AMAYA",
        ["Guity, Amaya", "Pryor, DeArica", "Guity, Robison"],
        "test3c",
    )
    assert result == "Guity, Amaya", result


def test_fuzzy_box_match_ambiguous_first_name_still_errors() -> None:
    """The guards that make the divergence above safe are still guards.

    Two players sharing the first name is genuinely ambiguous, so it must
    keep erroring -- enabling 3C must not weaken 3A.
    """
    result = fuzzy_box_match(
        "EATON,LEXI",
        ["Rydalch, Lexi", "Bailey, Lexi"],
        "test3a",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.3A" in result.message


def test_fuzzy_box_match_surname_change_resolves_by_first_name() -> None:
    """The real BYU 2014-15 case: three name changes on one roster.

    `EATON` / `BROADHEAD` / `FULLER` are the play-by-play surnames;
    `Rydalch` / `Devashrayee` / `Nielson` are what the box calls the same
    three women. Before this change BYU produced lineups for 9 of 33 games.
    """
    box = ["Rydalch, Lexi", "Devashrayee, Cassie", "Nielson, Kristine", "Bailey, Morgan"]
    assert fuzzy_box_match("EATON,LEXI", box, "byu") == "Rydalch, Lexi"
    assert fuzzy_box_match("BROADHEAD,CASSIE", box, "byu") == "Devashrayee, Cassie"
    assert fuzzy_box_match("FULLER,KRISTINE", box, "byu") == "Nielson, Kristine"


def test_fuzzy_box_match_multiple_first_name_error() -> None:
    """utest ``:243-250``."""
    result = fuzzy_box_match(
        "FINKLEA,AMAYA",
        ["Guity, Amaya", "Pryor, DeArica", "Robinson, Amaya"],
        "test3a",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.3A" in result.message


def test_fuzzy_box_match_near_first_name_error() -> None:
    """utest ``:252-259``."""
    result = fuzzy_box_match(
        "FINKLEA,AMAYA",
        ["Guity, Amaya", "Pryor, DeArica", "Robinson, Anaya"],
        "test3b",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.3B" in result.message


def test_fuzzy_box_match_no_good_matches() -> None:
    """utest ``:261-268``."""
    result = fuzzy_box_match(
        "FINKLEA,ALISON",
        ["Guity, Amaya", "Pryor, DeArica", "Robinson, Anaya"],
        "test4a",
    )
    assert isinstance(result, FuzzyMatchError)
    assert "ERROR.4A" in result.message


# --- build_tidy_player_context / tidy_player (no Scala oracle -- utest
# ``:17-21`` is an empty "//TODO: adequately test ... build_partial_lineup_list"
# stub; end-to-end coverage lands in Task 5b.5) -------------------------------


def test_build_tidy_player_context_maps() -> None:
    """``all_players_map`` is a straight code -> name index."""
    p1 = build_player_code("Mitchell, Makhi", None)
    p2 = build_player_code("Smith, John", None)
    ctx = build_tidy_player_context(_lineup_event([p1, p2]))
    assert ctx.all_players_map == {p1.code: p1.id.name, p2.code: p2.id.name}


def test_build_tidy_player_context_alt_map_singleton() -> None:
    """A unique truncated code ("MMitchell" from "MiMitchell" + unrelated
    "Smith, John") resolves to a singleton name list."""
    p1 = build_player_code("Mitchell, Makhi", None)  # code "MiMitchell"
    p2 = build_player_code("Smith, John", None)
    ctx = build_tidy_player_context(_lineup_event([p1, p2]))
    assert ctx.alt_all_players_map["MMitchell"] == ["Mitchell, Makhi"]


def test_build_tidy_player_context_alt_map_collision_is_ambiguous() -> None:
    """The canonical duplicate-name risk case: two Mitchells' distinct
    codes ("MiMitchell" / "MlMitchell") truncate to the SAME "MMitchell" key
    -- this must stay a 2-element list so :func:`tidy_player`'s alt-code
    fallback correctly refuses to guess between them."""
    p1 = build_player_code("Mitchell, Makhi", None)
    p2 = build_player_code("Mitchell, Makhel", None)
    ctx = build_tidy_player_context(_lineup_event([p1, p2]))
    assert sorted(ctx.alt_all_players_map["MMitchell"]) == sorted([p1.id.name, p2.id.name])


def test_tidy_player_exact_code_match() -> None:
    p1 = build_player_code("Mitchell, Makhi", None)
    ctx = build_tidy_player_context(_lineup_event([p1]))
    resolved, ctx = tidy_player(p1.id.name, ctx)
    assert resolved == p1.id.name


def test_tidy_player_alt_code_match_short_initial_format() -> None:
    """The canonical fuzzy-match-risk-note scenario: a single box "Anderson,
    Al" (code "AlAnderson") resolves from the PbP's truncated "Anderson, A"
    -- build_player_code("Anderson, A", ...).code == "AAnderson" ==
    truncate_code_1("AlAnderson"), a unique alt match."""
    box_player = build_player_code("Anderson, Al", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player("Anderson, A", ctx)
    assert resolved == "Anderson, Al"


def test_tidy_player_double_barrel_strip() -> None:
    """PbP records the double-barrel surname in full ("Bigby-Williams,
    Kavell") but the box score only has the second half ("Williams,
    Kavell") -- the double-barrel-strip retry recovers it."""
    box_player = build_player_code("Williams, Kavell", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player("Bigby-Williams, Kavell", ctx)
    assert resolved == "Williams, Kavell"


def test_tidy_player_initials_fallback() -> None:
    """ "A A" is shorthand for a box player coded "AlAnderson" (code[0]=='A',
    code[2]=='A') -- routes through :func:`convert_from_initials`."""
    box_player = build_player_code("Anderson, Al", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player("A A", ctx)
    assert resolved == "Anderson, Al"


def test_tidy_player_digit_fallback() -> None:
    """A jersey-number-only PbP mention resolves against ``players_out``."""
    player_out = PlayerCodeId(code="1000", id=PlayerId("Wilson, Kobe"))
    ctx = build_tidy_player_context(_lineup_event([], players_out=[player_out]))
    resolved, ctx = tidy_player("1000", ctx)
    assert resolved == "Wilson, Kobe"


@pytest.mark.parametrize("team_token", ["Team", "TEAM", "TEAM DEF", "TEAM FULL"])
def test_tidy_player_team_normalization(team_token: str) -> None:
    """The four team-stat-row sentinels normalize to the single string
    ``"Team"`` (and skip the fuzzy matcher entirely)."""
    box_player = build_player_code("Anderson, Al", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player(team_token, ctx)
    assert resolved == "Team"


def test_tidy_player_fuzzy_fallback() -> None:
    """A mis-spelled PbP name with exactly one plausible box-score candidate
    resolves via the terminal :func:`fuzzy_box_match` fallback (this is the
    same "Osbrone, Malik" / "Osborne, John" pair box_aware_compare is
    oracle-verified against above -- a single ``WeakSurnameMatch`` wins when
    it's the only unassigned candidate)."""
    box_player = build_player_code("Osborne, John", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player("Osbrone, Malik", ctx)
    assert resolved == "Osborne, John"


def test_tidy_player_identity_fallthrough() -> None:
    """An unresolvable name (no box-score candidate is even a weak fuzzy
    match) is returned unchanged -- rejected later by the out-of-scope
    validation pass, not here."""
    box_player = build_player_code("Anderson, Al", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved, ctx = tidy_player("Nonexistent Xyzzy", ctx)
    assert resolved == "Nonexistent Xyzzy"


def test_tidy_player_cache_hit_short_circuits() -> None:
    box_player = build_player_code("Anderson, Al", None)
    ctx = build_tidy_player_context(_lineup_event([box_player]))
    resolved_1, ctx = tidy_player("Anderson, Al", ctx)
    resolved_2, ctx_2 = tidy_player("Anderson, Al", ctx)
    assert resolved_1 == resolved_2 == "Anderson, Al"
    assert ctx_2 is ctx  # cache hit returns the SAME context, no new entry


def test_tidy_player_cache_is_asymmetric_by_design() -> None:
    """Documents the ported-verbatim Scala quirk (the module's "Behavioral
    quirk" docstring note): the cache is written under the misspelling-
    corrected name ("Lawal, Levi"), not the raw ``p_in`` that was actually
    looked up ("Lewal, Levi" -- an NJIT misspelling-table entry), so a
    repeat lookup of the SAME misspelled ``p_in`` never hits the cache."""
    box_player = build_player_code("Lawal, Levi", TeamId("NJIT"))
    ctx = build_tidy_player_context(_lineup_event([box_player], team=TeamId("NJIT")))

    resolved, ctx = tidy_player("Lewal, Levi", ctx)
    assert resolved == "Lawal, Levi"
    assert ctx.resolution_cache == {"Lawal, Levi": "Lawal, Levi"}
    assert "Lewal, Levi" not in ctx.resolution_cache
