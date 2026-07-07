"""Tests for ``sportsdataverse.mbb.mbb_ncaa_events`` (cbb-explorer port).

Transliterated 1:1 from ``EventUtilsTests.scala`` (utest,
``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``) -- the ``all_test_cases``
list is the exact same strings, in the exact same declared bucket order
(jumpball, timeout, shot_made, shot_missed, rebound, free_throw_made,
free_throw_missed, turnover, blocked, stolen, assist, foul), and each
extractor's expected output list is the literal Scala list transliterated
verbatim. ``utest``'s ``==>`` is deep-equality (no tolerance) -- ported as
plain ``==`` assertions.
"""

from __future__ import annotations

from collections import Counter

from sportsdataverse.mbb import mbb_ncaa_events as ev

# ---------------------------------------------------------------------------
# The `all_test_cases` fixture (EventUtilsTests.scala:11-136)
# ---------------------------------------------------------------------------

jumpball_test_cases = [
    "19:58:00,0-0,Kavell Bigby-Williams, jumpball lost",
    "19:58:00,0-0,Bruno Fernando, jumpball won",
]

timeout_test_cases = [
    "04:04:00,26-33,Ignored, timeout short",
    "00:21,59-62,IGNORED 30 Second Timeout",
    "10:00,51-60,TEAM 30 Second Timeout",
    "10:00,51-60,TEAM Team Timeout",
    "10:00,51-60,TEAM Media Timeout",
]

shot_made_test_cases = [
    "08:44:00,20-23,Bruno Fernando1, 2pt dunk 2ndchance;pointsinthepaint made",
    "08:44:00,20-23,Bruno Fernando2, 2pt alleyoop pointsinthepaint made",
    "08:44:00,20-23,WATKINS,MIKE made Dunk",
    "08:44:00,20-23,Jalen Smith, 2pt layup 2ndchance;pointsinthepaint made",
    "08:44:00,20-23,Landers Nolley II, 2pt drivinglayup made",
    "08:44:00,20-23,BOLTON,RASIR made Layup",
    "08:44:00,20-23,STEVENS,LAMAR made Tip In",
    "08:44:00,20-23,Eric Ayala, 3pt jumpshot made",
    "08:44:00,20-23,SMITH,JALEN made Three Point Jumper",
    "15:27,13-8,TRIMBLE JR,BRYAN made Three Point Jumper",
    "08:44:00,20-23,Francesca Pan2, 2pt hookshot 2ndchance;pointsinthepaint made",
    "08:44:00,20-23,Francesca Pan3, 2pt hookshot pointsinthepaint;fastbreak made",
    "08:44:00,20-23,Francesca Pan4, 2pt hookshot pointsinthepaint made",
    "08:44:00,20-23,Francesca Pan, 2pt hookshot 2ndchance made",
    "08:44:00,20-23,Anthony Cowan, 2pt jumpshot fromturnover;fastbreak made",
    "08:44:00,20-23,STEVENS,LAMAR2 made Two Point Jumper",
]

shot_missed_test_cases = [
    "08:44:00,20-23,Bruno Fernando3, 2pt dunk missed",
    "08:44:00,20-23,Joshua Tomaic, 2pt alleyoop missed",
    "08:44:00,20-23,WATKINS,MIKE1 missed Dunk",
    "08:44:00,20-23,Eric Carter, 2pt layup missed",
    "08:44:00,20-23,Landers Nolley II2, 2pt drivinglayup;pointsinthepaint missed",
    "08:44:00,20-23,TOMAIC,JOSHUA missed Layup",
    "08:44:00,20-23,HUERTER,KEVIN missed Tip In",
    "08:44:00,20-23,Eric Ayala2, 3pt jumpshot 2ndchance missed",
    "08:44:00,20-23,DREAD,MYLES missed Three Point Jumper",
    "08:44:00,20-23,Christina Morra2, 2pt hookshot 2ndchance;pointsinthepaint missed",
    "08:44:00,20-23,Christina Morra3, 2pt hookshot pointsinthepaint;fastbreak missed",
    "08:44:00,20-23,Christina Morra4, 2pt hookshot pointsinthepaint missed",
    "08:44:00,20-23,Christina Morra, 2pt hookshot 2ndchance missed",
    "08:44:00,20-23,Ricky Lindo Jr., 2pt jumpshot missed",
    "08:44:00,20-23,SMITH,JALEN1 missed Two Point Jumper",
]

rebound_test_cases = [
    "08:44:00,20-23,Darryl Morsell, rebound defensive",
    "08:44:00,20-23,Jalen Smith1, rebound offensive",
    "08:44:00,20-23,Team, rebound offensive team",
    "08:44:00,20-23,SMITH,JALEN2 Offensive Rebound",
    "08:44:00,20-23,HARRAR,JOHN Defensive Rebound",
    "04:33,46-45,TEAM Deadball Rebound",
    "04:28:0,52-59,Team, rebound offensivedeadball",
    "04:28:0,52-59,Team, rebound defensivedeadball",
]

free_throw_made_test_cases = [
    "08:44:00,20-23,Kevin Anderson0M, freethrow 1of1 made",
    "08:44:00,20-23,Kevin Anderson1M, freethrow 1of2 made",
    "08:44:00,20-23,Kevin Anderson, freethrow 2of2 made",
    "08:44:00,20-23,Kevin Anderson3M, freethrow 1of3 made",
    "08:44:00,20-23,DREAD,MYLES1 made Free Throw",
]

free_throw_missed_test_cases = [
    "08:44:00,20-23,Kevin Anderson0m, freethrow 1of1 missed",
    "08:44:00,20-23,Kevin Anderson1, freethrow 1of2 missed",
    "08:44:00,20-23,Kevin Anderson2, freethrow 2of2 missed",
    "08:44:00,20-23,Kevin Anderson3, freethrow 1of3 missed",
    "08:44:00,20-23,Kevin Anderson4, freethrow 2of3 missed",
    "08:44:00,20-23,Kevin Anderson5, freethrow 3of3 missed",
    "08:44:00,20-23,DREAD,MYLES2 missed Free Throw",
]

turnover_test_cases = [
    "14:11:00,7-9,Bruno Fernando4, turnover badpass",
    "14:11:00,7-9,Joshua Tomaic, turnover lostball",
    "14:11:00,7-9,Jalen Smith2, turnover offensive",
    "14:11:00,7-9,Kevin Anderson6, turnover travel",
    "14:11:00,7-9,MORSELL,DARRYL Turnover",
]

blocked_test_cases = [
    "14:11:00,7-9,Emmitt Williams, block",
    "04:53,55-69,LAYMAN,JAKE Blocked Shot",
]

stolen_test_cases = [
    "08:44:00,20-23,Jacob Cushing, steal",
    "05:10,55-68,MASON III,FRANK Steal",
]

assist_test_cases = [
    "18:28:00,0-0,Kyle Guy, assist",
    "19:49,0-2,EDWARDS,CARSEN Assist",
]

foul_test_cases = [
    "10:00,51-60,TEAM Commits Foul",
    "13:36:00,7-9,Jalen Smith3, foul personal shooting;2freethrow",
    "10:00,51-60,MYKHAILIUK,SVI Commits Foul",
    "06:43:00,55-79,Bruno Fernando5, foul technical classa;2freethrow",
    "02:28:00,27-38,Jalen Smith4, foulon",
    "03:42:00,10-10,Eric Carter1, foul personal flagrant1;2freethrow",
    "02:28:00,27-38,Eric Ayala3, foul offensive",
]

all_test_cases = (
    jumpball_test_cases
    + timeout_test_cases
    + shot_made_test_cases
    + shot_missed_test_cases
    + rebound_test_cases
    + free_throw_made_test_cases
    + free_throw_missed_test_cases
    + turnover_test_cases
    + blocked_test_cases
    + stolen_test_cases
    + assist_test_cases
    + foul_test_cases
)


def _collect(parser):
    """``all_test_cases.collect { case Parser(name) => name }`` transliteration."""
    return [name for s in all_test_cases if (name := parser(s)) is not None]


# ---------------------------------------------------------------------------
# ParseAnyPlay (frequency map)
# ---------------------------------------------------------------------------


def test_parse_any_play_frequency_map():
    some_names = {
        "DREAD,MYLES": 1,
        "HARRAR,JOHN": 1,
        "Kavell Bigby-Williams": 1,
        "WATKINS,MIKE": 1,
        "Bruno Fernando": 1,
        "Emmitt Williams": 1,
        "Team": 3,
        "Jalen Smith": 1,
        "Darryl Morsell": 1,
        "Joshua Tomaic": 2,
        "SMITH,JALEN": 1,
        "TEAM": 5,
        "BOLTON,RASIR": 1,
        "MASON III,FRANK": 1,
        "TRIMBLE JR,BRYAN": 1,
    }
    counts = Counter(_collect(ev.parse_any_play))
    filtered = {name: n for name, n in counts.items() if name in some_names}
    assert filtered == some_names


# ---------------------------------------------------------------------------
# Jump ball
# ---------------------------------------------------------------------------


def test_parse_jumpball_won_or_lost():
    assert _collect(ev.parse_jumpball_won_or_lost) == [
        "Kavell Bigby-Williams",
        "Bruno Fernando",
    ]


def test_parse_jumpball_won():
    assert _collect(ev.parse_jumpball_won) == ["Bruno Fernando"]


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_parse_timeout():
    assert _collect(ev.parse_timeout) == ["Team", "TEAM", "TEAM", "TEAM", "TEAM"]


# ---------------------------------------------------------------------------
# Shots made
# ---------------------------------------------------------------------------

shots_made = [
    "Bruno Fernando1",
    "Bruno Fernando2",
    "WATKINS,MIKE",
    "Jalen Smith",
    "Landers Nolley II",
    "BOLTON,RASIR",
    "STEVENS,LAMAR",
    "Eric Ayala",
    "SMITH,JALEN",
    "TRIMBLE JR,BRYAN",
    "Francesca Pan2",
    "Francesca Pan3",
    "Francesca Pan4",
    "Francesca Pan",
    "Anthony Cowan",
    "STEVENS,LAMAR2",
]


def test_parse_shot_made():
    assert _collect(ev.parse_shot_made) == shots_made


rim_shots_made = [
    "Bruno Fernando1",
    "Bruno Fernando2",
    "WATKINS,MIKE",
    "Jalen Smith",
    "Landers Nolley II",
    "BOLTON,RASIR",
    "STEVENS,LAMAR",
    "Francesca Pan2",
    "Francesca Pan3",
    "Francesca Pan4",
]


def test_parse_rim_made():
    assert _collect(ev.parse_rim_made) == rim_shots_made


ft_2p_made = rim_shots_made + ["Francesca Pan", "Anthony Cowan", "STEVENS,LAMAR2"]


def test_parse_two_pointer_made():
    assert _collect(ev.parse_two_pointer_made) == ft_2p_made


def test_parse_three_pointer_made():
    assert _collect(ev.parse_three_pointer_made) == [
        "Eric Ayala",
        "SMITH,JALEN",
        "TRIMBLE JR,BRYAN",
    ]


# ---------------------------------------------------------------------------
# Shots missed
# ---------------------------------------------------------------------------

shots_missed = [
    "Bruno Fernando3",
    "Joshua Tomaic",
    "WATKINS,MIKE1",
    "Eric Carter",
    "Landers Nolley II2",
    "TOMAIC,JOSHUA",
    "HUERTER,KEVIN",
    "Eric Ayala2",
    "DREAD,MYLES",
    "Christina Morra2",
    "Christina Morra3",
    "Christina Morra4",
    "Christina Morra",
    "Ricky Lindo Jr.",
    "SMITH,JALEN1",
]


def test_parse_shot_missed():
    assert _collect(ev.parse_shot_missed) == shots_missed


rim_missed = [
    "Bruno Fernando3",
    "Joshua Tomaic",
    "WATKINS,MIKE1",
    "Eric Carter",
    "Landers Nolley II2",
    "TOMAIC,JOSHUA",
    "HUERTER,KEVIN",
    "Christina Morra2",
    "Christina Morra3",
    "Christina Morra4",
]


def test_parse_rim_missed():
    assert _collect(ev.parse_rim_missed) == rim_missed


fg_2p_missed = rim_missed + ["Christina Morra", "Ricky Lindo Jr.", "SMITH,JALEN1"]


def test_parse_two_pointer_missed():
    assert _collect(ev.parse_two_pointer_missed) == fg_2p_missed


def test_parse_three_pointer_missed():
    assert _collect(ev.parse_three_pointer_missed) == ["Eric Ayala2", "DREAD,MYLES"]


# ---------------------------------------------------------------------------
# Rebounds
# ---------------------------------------------------------------------------


def test_parse_rebound():
    assert _collect(ev.parse_rebound) == [
        "Darryl Morsell",
        "Jalen Smith1",
        "Team",
        "SMITH,JALEN2",
        "HARRAR,JOHN",
        "TEAM",
        "Team",
        "Team",
    ]


def test_parse_offensive_rebound():
    assert _collect(ev.parse_offensive_rebound) == [
        "Jalen Smith1",
        "Team",
        "SMITH,JALEN2",
        "Team",
    ]


def test_parse_live_offensive_rebound():
    assert _collect(ev.parse_live_offensive_rebound) == [
        "Jalen Smith1",
        "Team",
        "SMITH,JALEN2",
    ]


drbs = ["Darryl Morsell", "HARRAR,JOHN", "Team"]


def test_parse_defensive_rebound():
    assert _collect(ev.parse_defensive_rebound) == drbs


def test_parse_deadball_rebound():
    assert _collect(ev.parse_deadball_rebound) == ["TEAM", "Team", "Team"]


def test_parse_offensive_deadball_rebound():
    assert _collect(ev.parse_offensive_deadball_rebound) == ["Team"]


# ---------------------------------------------------------------------------
# Free throws
# ---------------------------------------------------------------------------

fts_made = ["Kevin Anderson0M", "Kevin Anderson1M", "Kevin Anderson", "Kevin Anderson3M", "DREAD,MYLES1"]


def test_parse_free_throw_made():
    assert _collect(ev.parse_free_throw_made) == fts_made


fts_missed = [
    "Kevin Anderson0m",
    "Kevin Anderson1",
    "Kevin Anderson2",
    "Kevin Anderson3",
    "Kevin Anderson4",
    "Kevin Anderson5",
    "DREAD,MYLES2",
]


def test_parse_free_throw_missed():
    assert _collect(ev.parse_free_throw_missed) == fts_missed


def test_parse_free_throw_event():
    assert _collect(ev.parse_free_throw_event) == [
        "Kevin Anderson0M",
        "Kevin Anderson1M",
        "Kevin Anderson3M",
        "DREAD,MYLES1",
        "Kevin Anderson0m",
        "Kevin Anderson1",
        "Kevin Anderson3",
        "DREAD,MYLES2",
    ]


fts_attempt = fts_made + fts_missed


def test_parse_free_throw_attempt():
    assert _collect(ev.parse_free_throw_attempt) == fts_attempt


fts_made_gen2 = [
    ("Kevin Anderson0M", 1, 1),
    ("Kevin Anderson1M", 1, 2),
    ("Kevin Anderson", 2, 2),
    ("Kevin Anderson3M", 1, 3),
]

fts_missed_gen2 = [
    ("Kevin Anderson0m", 1, 1),
    ("Kevin Anderson1", 1, 2),
    ("Kevin Anderson2", 2, 2),
    ("Kevin Anderson3", 1, 3),
    ("Kevin Anderson4", 2, 3),
    ("Kevin Anderson5", 3, 3),
]

fts_attempt_gen2 = fts_made_gen2 + fts_missed_gen2


def test_parse_free_throw_event_attempt_gen2():
    assert _collect(ev.parse_free_throw_event_attempt_gen2) == fts_attempt_gen2


# ---------------------------------------------------------------------------
# Turnovers / blocks / steals / assists
# ---------------------------------------------------------------------------

turnovers = ["Bruno Fernando4", "Joshua Tomaic", "Jalen Smith2", "Kevin Anderson6", "MORSELL,DARRYL"]


def test_parse_turnover():
    assert _collect(ev.parse_turnover) == turnovers


blockers = ["Emmitt Williams", "LAYMAN,JAKE"]


def test_parse_shot_blocked():
    assert _collect(ev.parse_shot_blocked) == blockers


stealers = ["Jacob Cushing", "MASON III,FRANK"]


def test_parse_stolen():
    assert _collect(ev.parse_stolen) == stealers


def test_parse_assist():
    assert _collect(ev.parse_assist) == ["Kyle Guy", "EDWARDS,CARSEN"]


# ---------------------------------------------------------------------------
# Fouls
# ---------------------------------------------------------------------------


def test_parse_personal_foul():
    assert _collect(ev.parse_personal_foul) == ["Jalen Smith3", "MYKHAILIUK,SVI", "Eric Carter1"]


def test_parse_flagrant_foul():
    assert _collect(ev.parse_flagrant_foul) == ["Eric Carter1"]


def test_parse_technical_foul():
    assert _collect(ev.parse_technical_foul) == ["TEAM", "Bruno Fernando5"]


def test_parse_offensive_foul():
    assert _collect(ev.parse_offensive_foul) == ["Eric Ayala3"]


def test_parse_foul_info():
    assert _collect(ev.parse_foul_info) == ["Jalen Smith4"]


# ---------------------------------------------------------------------------
# Combinators
# ---------------------------------------------------------------------------

offensive_actions = shots_made + shots_missed + fts_attempt + turnovers


def test_parse_offensive_event():
    assert _collect(ev.parse_offensive_event) == offensive_actions


defensive_actions = blockers + stealers


def test_parse_defensive_info_event():
    assert _collect(ev.parse_defensive_info_event) == defensive_actions


def test_parse_defensive_action_event():
    assert _collect(ev.parse_defensive_action_event) == drbs


defensive_events = drbs + defensive_actions


def test_parse_defensive_event():
    assert _collect(ev.parse_defensive_event) == defensive_events


# ---------------------------------------------------------------------------
# Extras: is_gen2, parse_game_time, sub in/out (not exercised by the Scala
# EventUtilsTests fixture, but part of the ported public surface -- covered
# by ExtractorUtils.scala:107-113-style full-match discipline checks).
# ---------------------------------------------------------------------------


def test_is_gen2():
    from sportsdataverse.mbb.mbb_ncaa_models import RawGameEvent

    gen2 = RawGameEvent.for_team("19:58:00,0-0,Bruno Fernando, jumpball won", 19.0)
    legacy = RawGameEvent.for_team("08:44,20-23,WATKINS,MIKE made Dunk", 8.0)
    assert ev.is_gen2(gen2) is True
    assert ev.is_gen2(legacy) is False


def test_parse_game_time_old_and_new_format():
    assert ev.parse_game_time("05:10") == 5.0 + 10 / 60.0
    assert ev.parse_game_time("19:58:00") == 19.0 + 58 / 60.0 + 0 / 6000.0
    assert ev.parse_game_time("garbage") is None


def test_parse_game_time_requires_full_match():
    # Scala `.r` unapply is a full match, not find -- a trailing suffix
    # after the time pattern must reject the match.
    assert ev.parse_game_time("05:10 extra") is None


def test_parse_team_sub_in_old_and_new_format():
    assert ev.parse_team_sub_in("Bruno Fernando Enters Game") == "Bruno Fernando"
    assert ev.parse_team_sub_in("Bruno Fernando, substitution in") == "Bruno Fernando"
    assert ev.parse_team_sub_in("garbage") is None


def test_parse_team_sub_out_old_and_new_format():
    assert ev.parse_team_sub_out("Bruno Fernando Leaves Game") == "Bruno Fernando"
    assert ev.parse_team_sub_out("Bruno Fernando, substitution out") == "Bruno Fernando"
    assert ev.parse_team_sub_out("garbage") is None
