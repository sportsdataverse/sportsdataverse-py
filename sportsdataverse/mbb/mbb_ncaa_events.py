"""NCAA PBP-line event extractors (cbb-explorer port).

Faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``EventUtils.scala`` -- the second of four Phase-5a modules
(``mbb_ncaa_models.py`` / ``mbb_ncaa_events.py`` / ``mbb_ncaa_possessions.py``
/ wbb shims). **Task 5a.2 ports the play-by-play-line extractors**: one
``parse_x(s) -> str | None`` function per Scala ``ParseX`` object (the
``unapply`` return value), consumed by the possession calculator (5a.3).

Every NCAA raw event string is a comma-joined ``"time,score,event"`` line in
one of two on-the-wire formats:

* **Old / legacy** -- ``HH:MM,score,SURNAME,FIRST verb Noun`` (all-caps
  player names, e.g. ``"08:44,20-23,WATKINS,MIKE made Dunk"``).
* **New / "gen2"** -- ``HH:MM:SS,score,First Last, lower-verb`` (mixed-case
  names followed by a comma-space then a lowercase verb phrase, e.g.
  ``"08:44:00,20-23,Bruno Fernando, 2pt dunk made"``). :func:`is_gen2`
  detects this format (``ev.info`` contains ``", "``).

Most extractors are ``old_regex.unapply(x).orElse(new_regex.unapply(x))`` in
the Scala source -- ported as an explicit first-match-wins chain, in the
**same declared order as the Scala** (a few extractors try new-format first;
see each function's docstring for the citation).

**Full-match discipline.** Scala's ``regex.r`` case-pattern ``unapply``
(``Regex.unapplySeq``) calls ``Matcher.matches()``, which requires the
*entire* string to match -- ported here as :func:`re.fullmatch`, never
:func:`re.match`/:func:`re.search`. A partial match would silently accept
garbage suffixes/prefixes the Scala original rejects.

**Negative lookahead is fine here.** Several old-format regexes use
``(?!...)`` (e.g. ``made (?!Free Throw)`` to exclude free throws from the
generic "shot made" union, ``(?!TEAM)`` to exclude the literal team-foul
placeholder from personal fouls). This module uses Python's stdlib :mod:`re`
directly (not polars/Rust regex), which supports lookaround -- the
project-wide "no lookaround" rule is specific to polars expression regex and
does not apply here.

**Port traps (documented per-function below):**

* :func:`parse_timeout` **discards its captured group** and returns a fixed
  literal (``"TEAM"`` for an old-format match, ``"Team"`` for new) --
  Scala's ``case Some(timeout_regex(_)) => Some("TEAM")`` never uses the
  underscore-bound capture.
* :func:`parse_shot_missed`'s new-format regex matches **either**
  ``missed`` or ``blocked`` (``(?:missed|blocked)``) -- a blocked shot is
  also counted as a missed shot for possession purposes.
* :func:`parse_personal_foul`'s old-format regex excludes the literal name
  ``TEAM`` via ``(?!TEAM)`` (that's a technical-foul placeholder, not a
  player); :func:`parse_technical_foul`'s old-format regex captures that
  literal ``TEAM`` on purpose.
* :func:`parse_offensive_rebound`'s new-format regex (``rebound
  offensive.*``) matches **both** live and dead-ball offensive rebounds --
  :func:`parse_offensive_deadball_rebound` is the new-only deadball-specific
  subset, and :func:`parse_live_offensive_rebound` is offensive rebounds
  *excluding* the deadball ones.
* :func:`parse_free_throw_event`: the new-format branch only matches
  ``freethrow 1of[123]`` (the *first* free throw of a set, i.e. the set's
  start) -- a ``2of2``/``2of3``/``3of3`` continuation does NOT count as a
  fresh "FT event" in new format (old format has no such marker, so any old
  make/miss counts).

**Tuple overload (Task 5e.3 addition).** ``ParseTeamSubIn``/``ParseTeamSubOut``
also carry a ``(Option[String], Option[String])``-tuple overload in the
Scala source (``EventUtils.scala:39-42,53-56``), matched directly against a
raw ``(team, opponent)`` pair during play-by-play *construction*. Task 5a.2's
original docstring guessed this was consumed by
``ExtractorUtils.build_partial_lineup_list`` and deferred it to "Phase 5b" --
that guess was wrong: ``build_partial_lineup_list`` (ported in Task 5b, see
``mbb_ncaa_stints.py``) already receives fully-constructed ``SubInEvent``/
``SubOutEvent`` objects, never raw ``(team, opponent)`` string pairs. The
actual (and only) consumer is ``PlayByPlayParser.parse_game_event``
(``PlayByPlayParser.scala:414-456``), ported in Task 5e.3 --
:func:`parse_team_sub_in_pair`/:func:`parse_team_sub_out_pair` below.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of ``EventUtils.scala`` from
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(package ``org.piggottfamily.cbb_explorer``), which is licensed under the
Apache License, Version 2.0 (the upstream repo's ``LICENSE`` file; full text
at `<http://www.apache.org/licenses/LICENSE-2.0>`_). Per Apache-2.0 Section
4's redistribution-of-derivative-works obligations, sportsdataverse-py
(itself MIT-licensed) retains the upstream copyright notice for this
derivative::

    Copyright (c) Alex-At-Home (https://github.com/Alex-At-Home) and
    contributors. Licensed under the Apache License, Version 2.0.

See ``THIRD_PARTY_NOTICES.md`` at the repository root for the full
third-party attribution entry -- Task 5a.4 adds cbb-explorer's entry there.

**Landmine index (reachable scalar division).** None. Every computation in
this module is regex matching, string comparison, or (:func:`parse_game_time`)
addition/division by fixed non-zero literals (``60.0``, ``6000.0``) -- no
division by a runtime-derived value exists.

Example::

    from sportsdataverse.mbb.mbb_ncaa_events import parse_rim_made, parse_shot_missed

    parse_rim_made("08:44:00,20-23,Bruno Fernando, 2pt dunk 2ndchance;pointsinthepaint made")
    # 'Bruno Fernando'
    parse_shot_missed("14:11:00,7-9,Emmitt Williams, 2pt jumpshot blocked")
    # 'Emmitt Williams'  (new-format "blocked" counts as a missed shot)

See Also:
    * `cbb-on-off-analyzer <https://github.com/Alex-At-Home/cbb-on-off-analyzer>`_ -- the TypeScript sibling this Scala core feeds
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R men's basketball companion package
"""

from __future__ import annotations

import re
from typing import Optional

from sportsdataverse.mbb.mbb_ncaa_models import RawGameEvent

__all__ = [
    "is_gen2",
    "parse_game_time",
    "parse_team_sub_in",
    "parse_team_sub_out",
    "parse_team_sub_in_pair",
    "parse_team_sub_out_pair",
    "parse_any_play",
    "parse_jumpball_won_or_lost",
    "parse_jumpball_won",
    "parse_timeout",
    "parse_rim_made",
    "parse_rim_missed",
    "parse_two_pointer_made",
    "parse_two_pointer_missed",
    "parse_three_pointer_made",
    "parse_three_pointer_missed",
    "parse_shot_made",
    "parse_shot_missed",
    "parse_shot_blocked",
    "parse_rebound",
    "parse_offensive_rebound",
    "parse_defensive_rebound",
    "parse_deadball_rebound",
    "parse_offensive_deadball_rebound",
    "parse_live_offensive_rebound",
    "parse_free_throw_made",
    "parse_free_throw_missed",
    "parse_free_throw_attempt",
    "parse_free_throw_event",
    "parse_free_throw_event_attempt_gen2",
    "parse_turnover",
    "parse_stolen",
    "parse_assist",
    "parse_personal_foul",
    "parse_technical_foul",
    "parse_flagrant_foul",
    "parse_offensive_foul",
    "parse_foul_info",
    "parse_offensive_event",
    "parse_defensive_action_event",
    "parse_defensive_info_event",
    "parse_defensive_event",
]


def is_gen2(ev: RawGameEvent) -> bool:
    """Detect the new/"gen2" NCAA event format (``EventUtils.is_gen2``,
    ``EventUtils.scala:12-14``).

    Args:
        ev: The raw game event to inspect.

    Returns:
        ``True`` if ``ev.info`` contains a comma-space (``", "``), the
        gen2 format's field separator; ``False`` for the old/legacy format.
    """
    return ", " in ev.info


# ---------------------------------------------------------------------------
# Date-time parser
# ---------------------------------------------------------------------------

_TIME_REGEX = re.compile(r"([0-9]+):([0-9]+)(?:[:]([0-9]+))?")


def parse_game_time(x: str) -> Optional[float]:
    """Parse the game-clock time from a raw event's leading field
    (``ParseGameTime``, ``EventUtils.scala:19-28``).

    Args:
        x: The raw ``"HH:MM"`` or ``"HH:MM:SS"`` time field.

    Returns:
        ``min + secs/60.0 + optional_csecs/6000.0`` as a float, or ``None``
        if ``x`` doesn't fully match ``([0-9]+):([0-9]+)(?:[:]([0-9]+))?``.
    """
    m = _TIME_REGEX.fullmatch(x)
    if m is None:
        return None
    minutes, secs, csecs = m.group(1), m.group(2), m.group(3)
    return int(minutes) * 1.0 + int(secs) / 60.0 + (int(csecs) if csecs is not None else 0) / 6000.0


# ---------------------------------------------------------------------------
# Team substitution events (string overload only -- see module docstring)
# ---------------------------------------------------------------------------

_SUB_REGEX_IN = re.compile(r"(.+) +Enters Game")
_SUB_REGEX_IN_NEW_FORMAT = re.compile(r"(.+), +substitution in")


def parse_team_sub_in(x: str) -> Optional[str]:
    """Team (not opponent) substitution in (``ParseTeamSubIn``,
    ``EventUtils.scala:36-48``, string overload).

    Args:
        x: The raw event string.

    Returns:
        The substituting player's name, or ``None`` if ``x`` doesn't match
        either the old (``"... Enters Game"``) or new (``"..., substitution
        in"``) format.
    """
    m = _SUB_REGEX_IN.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _SUB_REGEX_IN_NEW_FORMAT.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_SUB_REGEX_OUT = re.compile(r"(.+) +Leaves Game")
_SUB_REGEX_OUT_NEW_FORMAT = re.compile(r"(.+), +substitution out")


def parse_team_sub_out(x: str) -> Optional[str]:
    """Team (not opponent) substitution out (``ParseTeamSubOut``,
    ``EventUtils.scala:50-62``, string overload).

    Args:
        x: The raw event string.

    Returns:
        The substituted-out player's name, or ``None`` if ``x`` doesn't
        match either the old (``"... Leaves Game"``) or new (``"...,
        substitution out"``) format.
    """
    m = _SUB_REGEX_OUT.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _SUB_REGEX_OUT_NEW_FORMAT.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


def parse_team_sub_in_pair(team: Optional[str], opponent: Optional[str]) -> Optional[str]:
    """Team substitution in, tuple overload (``ParseTeamSubIn.unapply(x:
    (Option[String], Option[String]))``, ``EventUtils.scala:39-42``).

    Matched directly against a raw ``(team_text, opponent_text)`` pair during
    play-by-play event construction (``PlayByPlayParser.parse_game_event``,
    Task 5e.3) -- see the module docstring's "Tuple overload" note.

    Args:
        team: The team-side event text, or ``None``.
        opponent: The opponent-side event text, or ``None``.

    Returns:
        :func:`parse_team_sub_in`'s result on ``team``, but only if
        ``opponent`` is ``None`` (the Scala match arm is
        ``case (Some(player), None) => ...``; any other shape, including
        both present or both absent, returns ``None`` here).
    """
    if team is not None and opponent is None:
        return parse_team_sub_in(team)
    return None


def parse_team_sub_out_pair(team: Optional[str], opponent: Optional[str]) -> Optional[str]:
    """Team substitution out, tuple overload (``ParseTeamSubOut.unapply(x:
    (Option[String], Option[String]))``, ``EventUtils.scala:53-56``).

    See :func:`parse_team_sub_in_pair` for the shared shape/rationale.

    Args:
        team: The team-side event text, or ``None``.
        opponent: The opponent-side event text, or ``None``.

    Returns:
        :func:`parse_team_sub_out`'s result on ``team``, but only if
        ``opponent`` is ``None``.
    """
    if team is not None and opponent is None:
        return parse_team_sub_out(team)
    return None


# ---------------------------------------------------------------------------
# In-game events (based on the combined time,score,event line)
# ---------------------------------------------------------------------------

_ANY_PLAY_REGEX_NEW = re.compile(r"[^,]+,[^,]+,([^,]+), .*")
_ANY_PLAY_REGEX_OLD = re.compile(r"[^,]+,[^,]+,([ A-Z.,-]+) .*")


def parse_any_play(x: str) -> Optional[str]:
    """Pull the player/team name out of any play line (``ParseAnyPlay``,
    ``EventUtils.scala:69-79``).

    Order matters -- new-format is tried first (the old-format regex could
    spuriously match a new-format line; per the Scala's own comment "can't
    match on new test for new first").

    Args:
        x: The raw event string.

    Returns:
        The extracted name, or ``None`` if neither the new (``",
        ([^,]+), ..."``) nor old (all-caps run before a space) pattern
        fully matches.
    """
    m = _ANY_PLAY_REGEX_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _ANY_PLAY_REGEX_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


# Jump ball

_JUMPBALL_WON_OR_LOST_REGEX = re.compile(r"[^,]+,[^,]+,(.+), +jumpball (?:won|lost)")


def parse_jumpball_won_or_lost(x: str) -> Optional[str]:
    """Jump ball, won or lost -- doesn't matter which (``ParseJumpballWonOrLost``,
    ``EventUtils.scala:84-95``). New format only (no legacy examples).

    Args:
        x: The raw event string.

    Returns:
        The player's name, or ``None`` if no match.
    """
    m = _JUMPBALL_WON_OR_LOST_REGEX.fullmatch(x)
    return m.group(1) if m is not None else None


_JUMPBALL_WON_REGEX = re.compile(r"[^,]+,[^,]+,(.+), +jumpball won")


def parse_jumpball_won(x: str) -> Optional[str]:
    """Jump ball won specifically (``ParseJumpballWon``,
    ``EventUtils.scala:97-107``). New format only.

    Args:
        x: The raw event string.

    Returns:
        The player's name, or ``None`` if no match.
    """
    m = _JUMPBALL_WON_REGEX.fullmatch(x)
    return m.group(1) if m is not None else None


_TIMEOUT_REGEX = re.compile(r"[^,]+,[^,]+,(.+) +Timeout")
_TIMEOUT_REGEX_NEW = re.compile(r"[^,]+,[^,]+,(.+), +timeout.*")


def parse_timeout(x: str) -> Optional[str]:
    """Timeout (``ParseTimeout``, ``EventUtils.scala:110-122``).

    **Port trap:** the captured group is discarded -- the Scala match arm
    is ``case Some(timeout_regex(_)) => Some("TEAM")`` (old format) /
    ``case Some(timeout_regex_new(_)) => Some("Team")`` (new format); both
    branches return a fixed literal, never the underscore-bound capture.

    Args:
        x: The raw event string.

    Returns:
        The literal ``"TEAM"`` for an old-format match, ``"Team"`` for
        new-format, or ``None`` if neither matches.
    """
    if _TIMEOUT_REGEX.fullmatch(x) is not None:
        return "TEAM"
    if _TIMEOUT_REGEX_NEW.fullmatch(x) is not None:
        return "Team"
    return None


# ---------------------------------------------------------------------------
# Shots
# ---------------------------------------------------------------------------

_RIM_MADE_OLD = re.compile(r"[^,]+,[^,]+,(.+) made +(?:Dunk|Layup|Tip In)")
_RIM_MADE_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:dunk|drivinglayup|layup|alleyoop)(?:.* +)?made")
_RIM_HOOK_MADE_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:hookshot.*pointsinthepaint)(?:.* +)?made")


def parse_rim_made(x: str) -> Optional[str]:
    """Dunk / alley-oop / layup make, including rim-forced hookshots
    (``ParseRimMade``, ``EventUtils.scala:127-151``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no rim-shot-made pattern matches.
    """
    for rx in (_RIM_MADE_OLD, _RIM_MADE_NEW, _RIM_HOOK_MADE_NEW):
        m = rx.fullmatch(x)
        if m is not None:
            return m.group(1)
    return None


_RIM_MISSED_OLD = re.compile(r"[^,]+,[^,]+,(.+) missed +(?:Dunk|Layup|Tip In)")
_RIM_MISSED_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:dunk|drivinglayup|layup|alleyoop)(?:.* +)?missed")
_RIM_HOOK_MISSED_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:hookshot.*pointsinthepaint)(?:.* +)?missed")


def parse_rim_missed(x: str) -> Optional[str]:
    """Dunk / alley-oop / layup miss, including rim-forced hookshots
    (``ParseRimMissed``, ``EventUtils.scala:154-173``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no rim-shot-missed pattern matches.
    """
    for rx in (_RIM_MISSED_OLD, _RIM_MISSED_NEW, _RIM_HOOK_MISSED_NEW):
        m = rx.fullmatch(x)
        if m is not None:
            return m.group(1)
    return None


_TWO_PT_MADE_OLD = re.compile(r"[^,]+,[^,]+,(.+) made (?!Three|Free Throw).*")
_TWO_PT_MADE_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:.* +)?made")


def parse_two_pointer_made(x: str) -> Optional[str]:
    """Any 2pt make (rim shots plus jumpers/etc.) (``ParseTwoPointerMade``,
    ``EventUtils.scala:175-189``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _TWO_PT_MADE_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _TWO_PT_MADE_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_TWO_PT_MISSED_OLD = re.compile(r"[^,]+,[^,]+,(.+) missed (?!Three|Free Throw).*")
_TWO_PT_MISSED_NEW = re.compile(r"[^,]+,[^,]+,(.+), +2pt +(?:.* +)?missed")


def parse_two_pointer_missed(x: str) -> Optional[str]:
    """Any 2pt miss (``ParseTwoPointerMissed``, ``EventUtils.scala:191-206``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _TWO_PT_MISSED_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _TWO_PT_MISSED_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_THREE_PT_MADE_OLD = re.compile(r"[^,]+,[^,]+,(.+) made Three Point.*")
_THREE_PT_MADE_NEW = re.compile(r"[^,]+,[^,]+,(.+), +3pt +(?:.* +)?made")


def parse_three_pointer_made(x: str) -> Optional[str]:
    """Any 3pt make (``ParseThreePointerMade``, ``EventUtils.scala:208-221``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _THREE_PT_MADE_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _THREE_PT_MADE_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_THREE_PT_MISSED_OLD = re.compile(r"[^,]+,[^,]+,(.+) missed Three Point.*")
_THREE_PT_MISSED_NEW = re.compile(r"[^,]+,[^,]+,(.+), +3pt +(?:.* +)?missed")


def parse_three_pointer_missed(x: str) -> Optional[str]:
    """Any 3pt miss (``ParseThreePointerMissed``, ``EventUtils.scala:222-235``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _THREE_PT_MISSED_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _THREE_PT_MISSED_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_SHOT_MADE_UNION_OLD = re.compile(r"[^,]+,[^,]+,(.+) made +(?!Free Throw).*")
_SHOT_MADE_UNION_NEW = re.compile(r"[^,]+,[^,]+,(.+), +[23]pt +(?:.* +)?made")


def parse_shot_made(x: str) -> Optional[str]:
    """Umbrella for all made (non-free-throw) shots (``ParseShotMade``,
    ``EventUtils.scala:238-251``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _SHOT_MADE_UNION_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _SHOT_MADE_UNION_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_SHOT_MISSED_UNION_OLD = re.compile(r"[^,]+,[^,]+,(.+) missed +(?!Free Throw).*")
_SHOT_MISSED_UNION_NEW = re.compile(r"[^,]+,[^,]+,(.+), +[23]pt +(?:.* +)?(?:missed|blocked)")


def parse_shot_missed(x: str) -> Optional[str]:
    """Umbrella for all missed (non-free-throw) shots (``ParseShotMissed``,
    ``EventUtils.scala:254-267``).

    **Port trap:** the new-format regex matches ``missed`` **or**
    ``blocked`` -- a blocked shot counts as a missed shot here.

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _SHOT_MISSED_UNION_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _SHOT_MISSED_UNION_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_BLOCKED_SHOT_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Blocked Shot")
_BLOCKED_SHOT_NEW = re.compile(r"[^,]+,[^,]+,(.+), +block")


def parse_shot_blocked(x: str) -> Optional[str]:
    """Blocked shot -- captures the *blocker* (``ParseShotBlocked``,
    ``EventUtils.scala:270-282``).

    Args:
        x: The raw event string.

    Returns:
        The blocker's name, or ``None`` if no match.
    """
    m = _BLOCKED_SHOT_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _BLOCKED_SHOT_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


# ---------------------------------------------------------------------------
# Rebounds
# ---------------------------------------------------------------------------

_REBOUND_OLD = re.compile(r"[^,]+,[^,]+,(.+) +(?:Offensive|Defensive|Deadball) +Rebound")
_REBOUND_NEW = re.compile(r"[^,]+,[^,]+,(.+), +rebound +.*")


def parse_rebound(x: str) -> Optional[str]:
    """Any rebound, offensive/defensive/deadball/team, uncategorized
    (``ParseRebound``, ``EventUtils.scala:287-304``).

    Args:
        x: The raw event string.

    Returns:
        The rebounder's name, or ``None`` if no match.
    """
    m = _REBOUND_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _REBOUND_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_OFF_REBOUND_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Offensive +Rebound")
_OFF_REBOUND_NEW = re.compile(r"[^,]+,[^,]+,(.+), +rebound +offensive.*")


def parse_offensive_rebound(x: str) -> Optional[str]:
    """Offensive rebound (``ParseOffensiveRebound``, ``EventUtils.scala:307-322``).

    **Port trap:** the new-format regex (``rebound offensive.*``) matches
    **both** live and dead-ball offensive rebounds -- the legacy format has
    no offensive-deadball distinction. Use :func:`parse_live_offensive_rebound`
    to exclude the dead-ball subset.

    Args:
        x: The raw event string.

    Returns:
        The rebounder's name, or ``None`` if no match.
    """
    m = _OFF_REBOUND_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _OFF_REBOUND_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_DEF_REBOUND_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Defensive +Rebound")
_DEF_REBOUND_NEW = re.compile(r"[^,]+,[^,]+,(.+), +rebound +defensive.*")


def parse_defensive_rebound(x: str) -> Optional[str]:
    """Defensive rebound (``ParseDefensiveRebound``, ``EventUtils.scala:325-340``).

    Args:
        x: The raw event string.

    Returns:
        The rebounder's name, or ``None`` if no match.
    """
    m = _DEF_REBOUND_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _DEF_REBOUND_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_OFF_DEADBALL_REBOUND_NEW = re.compile(r"[^,]+,[^,]+,(.+), +rebound offensivedeadball")


def parse_offensive_deadball_rebound(x: str) -> Optional[str]:
    """Offensive dead-ball rebound -- **new format only**, no legacy
    equivalent (``ParseOffensiveDeadballRebound``, ``EventUtils.scala:369-380``).

    Args:
        x: The raw event string.

    Returns:
        The rebounder's name, or ``None`` if no match.
    """
    m = _OFF_DEADBALL_REBOUND_NEW.fullmatch(x)
    return m.group(1) if m is not None else None


_DEADBALL_REBOUND_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Deadball +Rebound")
_DEADBALL_REBOUND_DEF_NEW = re.compile(r"[^,]+,[^,]+,(.+), +rebound defensivedeadball")


def parse_deadball_rebound(x: str) -> Optional[str]:
    """Any dead-ball rebound, offensive or defensive (``ParseDeadballRebound``,
    ``EventUtils.scala:346-363``).

    Tries :func:`parse_offensive_deadball_rebound` first (matching the
    Scala's ``ParseOffensiveDeadballRebound.unapply(x).orElse { ... }``),
    then falls back to the old-format ``"... Deadball Rebound"`` literal or
    the new-format ``"..., rebound defensivedeadball"``.

    Args:
        x: The raw event string.

    Returns:
        The rebounder's name, or ``None`` if no match.
    """
    result = parse_offensive_deadball_rebound(x)
    if result is not None:
        return result
    m = _DEADBALL_REBOUND_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _DEADBALL_REBOUND_DEF_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


def parse_live_offensive_rebound(x: str) -> Optional[str]:
    """Live-ball (non-deadball) offensive rebound -- new format only gives
    you this distinction; in old format identical to
    :func:`parse_offensive_rebound` (``ParseLiveOffensiveRebound``,
    ``EventUtils.scala:383-388``).

    Args:
        x: The raw event string.

    Returns:
        :func:`parse_offensive_rebound`'s result, unless
        :func:`parse_offensive_deadball_rebound` also matches (then
        ``None``).
    """
    result = parse_offensive_rebound(x)
    if result is None:
        return None
    if parse_offensive_deadball_rebound(x) is not None:
        return None
    return result


# ---------------------------------------------------------------------------
# Free throws
# ---------------------------------------------------------------------------

_FT_MADE_OLD = re.compile(r"[^,]+,[^,]+,(.+) made +Free Throw")
_FT_MADE_NEW = re.compile(r"[^,]+,[^,]+,(.+), +freethrow [0-9]of[0-9] +(?:.* +)?made")


def parse_free_throw_made(x: str) -> Optional[str]:
    """Any made free throw (``ParseFreeThrowMade``, ``EventUtils.scala:393-405``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _FT_MADE_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _FT_MADE_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_FT_MISSED_OLD = re.compile(r"[^,]+,[^,]+,(.+) missed +Free Throw")
_FT_MISSED_NEW = re.compile(r"[^,]+,[^,]+,(.+), +freethrow [0-9]of[0-9] +(?:.* +)?missed")


def parse_free_throw_missed(x: str) -> Optional[str]:
    """Any missed free throw (``ParseFreeThrowMissed``, ``EventUtils.scala:408-420``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _FT_MISSED_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _FT_MISSED_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


def parse_free_throw_attempt(x: str) -> Optional[str]:
    """Any free throw attempt, missed or made (``ParseFreeThrowAttempt``,
    ``EventUtils.scala:423-427``). Tries missed first (matching the Scala's
    ``ParseFreeThrowMissed.unapply(x).orElse { ParseFreeThrowMade.unapply(x) }``).

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if neither matches.
    """
    result = parse_free_throw_missed(x)
    if result is not None:
        return result
    return parse_free_throw_made(x)


_FT_START_NEW = re.compile(r"[^,]+,[^,]+,(.+), +freethrow 1of[123] .*")


def parse_free_throw_event(x: str) -> Optional[str]:
    """Presence of 1+ free throws in a possession -- old format only (will
    double-count if a set is split across clumps) (``ParseFreeThrowEvent``,
    ``EventUtils.scala:430-441``).

    **Port trap:** the new-format branch only matches ``freethrow
    1of[123]`` -- the *first* free throw of a set. A ``2of2``/``2of3``/
    ``3of3`` continuation does not re-trigger a "new FT event"; old format
    has no such marker, so any old-format make/miss counts.

    Args:
        x: The raw event string.

    Returns:
        The shooter's name, or ``None`` if no match.
    """
    m = _FT_START_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _FT_MISSED_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _FT_MADE_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_FT_GEN2_NEW = re.compile(r"[^,]+,[^,]+,(.+), +freethrow ([123])of([123]) .*")


def parse_free_throw_event_attempt_gen2(x: str) -> Optional[tuple[str, int, int]]:
    """New-format ("gen2") free throw, telling you whether it's the final
    FT of the set (``ParseFreeThrowEventAttemptGen2``, ``EventUtils.scala:444-450``).

    Args:
        x: The raw event string.

    Returns:
        ``(shooter, attempt_number, attempts_in_set)``, e.g.
        ``("Kevin Anderson", 1, 2)`` for a ``"freethrow 1of2"`` line, or
        ``None`` if ``x`` isn't a new-format free throw line.
    """
    m = _FT_GEN2_NEW.fullmatch(x)
    if m is None:
        return None
    return (m.group(1), int(m.group(2)), int(m.group(3)))


# ---------------------------------------------------------------------------
# Turnovers / steals / assists
# ---------------------------------------------------------------------------

_TURNOVER_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Turnover")
_TURNOVER_NEW = re.compile(r"[^,]+,[^,]+,(.+), +turnover +.*")


def parse_turnover(x: str) -> Optional[str]:
    """Turnover (``ParseTurnover``, ``EventUtils.scala:454-470``).

    Args:
        x: The raw event string.

    Returns:
        The player's name who turned it over, or ``None`` if no match.
    """
    m = _TURNOVER_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _TURNOVER_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_STOLEN_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Steal")
_STOLEN_NEW = re.compile(r"[^,]+,[^,]+,(.+), +steal")


def parse_stolen(x: str) -> Optional[str]:
    """Steal (``ParseStolen``, ``EventUtils.scala:473-485``).

    Args:
        x: The raw event string.

    Returns:
        The stealer's name, or ``None`` if no match.
    """
    m = _STOLEN_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _STOLEN_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_ASSIST_OLD = re.compile(r"[^,]+,[^,]+,(.+) +Assist")
_ASSIST_NEW = re.compile(r"[^,]+,[^,]+,(.+), +assist")


def parse_assist(x: str) -> Optional[str]:
    """Assist (``ParseAssist``, ``EventUtils.scala:488-500``).

    Args:
        x: The raw event string.

    Returns:
        The assisting player's name, or ``None`` if no match.
    """
    m = _ASSIST_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _ASSIST_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


# ---------------------------------------------------------------------------
# Fouls
# ---------------------------------------------------------------------------

_PERSONAL_FOUL_OLD = re.compile(r"[^,]+,[^,]+,(?!TEAM)(.+) +Commits Foul")
_PERSONAL_FOUL_NEW = re.compile(r"[^,]+,[^,]+,(.+), +foul personal.*")


def parse_personal_foul(x: str) -> Optional[str]:
    """Personal foul, offensive or defensive (``ParsePersonalFoul``,
    ``EventUtils.scala:505-517``).

    **Port trap:** the old-format regex excludes the literal name
    ``TEAM`` via ``(?!TEAM)`` -- that string is the technical-foul
    placeholder (see :func:`parse_technical_foul`), not a player. New-format
    personal fouls that also happen to be flagrant (``"foul personal
    flagrant..."``) still match here, since the prefix ``"foul personal"``
    is shared.

    Args:
        x: The raw event string.

    Returns:
        The fouling player's name, or ``None`` if no match.
    """
    m = _PERSONAL_FOUL_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _PERSONAL_FOUL_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_TECHNICAL_FOUL_OLD = re.compile(r"[^,]+,[^,]+,(TEAM) +Commits Foul")
_TECHNICAL_FOUL_NEW = re.compile(r"[^,]+,[^,]+,(.+), +foul technical.*")


def parse_technical_foul(x: str) -> Optional[str]:
    """Technical foul (``ParseTechnicalFoul``, ``EventUtils.scala:520-532``).

    **Port trap:** the old-format regex captures the literal ``TEAM`` on
    purpose -- old format only records coach technicals this way, with no
    player name available.

    Args:
        x: The raw event string.

    Returns:
        The literal ``"TEAM"`` for an old-format coach technical, the
        fouling player's name for new format, or ``None`` if no match.
    """
    m = _TECHNICAL_FOUL_OLD.fullmatch(x)
    if m is not None:
        return m.group(1)
    m = _TECHNICAL_FOUL_NEW.fullmatch(x)
    if m is not None:
        return m.group(1)
    return None


_FLAGRANT_FOUL_NEW = re.compile(r"[^,]+,[^,]+,(.+), +foul personal flagrant.*")


def parse_flagrant_foul(x: str) -> Optional[str]:
    """Flagrant foul -- new format only, no legacy examples found
    (``ParseFlagrantFoul``, ``EventUtils.scala:535-545``).

    Args:
        x: The raw event string.

    Returns:
        The fouling player's name, or ``None`` if no match.
    """
    m = _FLAGRANT_FOUL_NEW.fullmatch(x)
    return m.group(1) if m is not None else None


_OFF_FOUL_NEW = re.compile(r"[^,]+,[^,]+,(.+), +foul offensive.*")


def parse_offensive_foul(x: str) -> Optional[str]:
    """Offensive foul -- new format only, no legacy examples found; used
    just for counting stats, not distinguishing flagrant/normal
    (``ParseOffensiveFoul``, ``EventUtils.scala:548-558``).

    Args:
        x: The raw event string.

    Returns:
        The fouling player's name, or ``None`` if no match.
    """
    m = _OFF_FOUL_NEW.fullmatch(x)
    return m.group(1) if m is not None else None


_FOUL_INFO_NEW = re.compile(r"[^,]+,[^,]+,(.+), +foulon")


def parse_foul_info(x: str) -> Optional[str]:
    """Who was fouled -- new format only, no legacy examples found
    (``ParseFoulInfo``, ``EventUtils.scala:561-571``).

    Args:
        x: The raw event string.

    Returns:
        The fouled player's name, or ``None`` if no match.
    """
    m = _FOUL_INFO_NEW.fullmatch(x)
    return m.group(1) if m is not None else None


# ---------------------------------------------------------------------------
# Combinators
# ---------------------------------------------------------------------------


def parse_offensive_event(x: str) -> Optional[str]:
    """A primary offensive event (not assists) that directly tells us which
    side is in possession -- free throws, shots, turnovers
    (``ParseOffensiveEvent``, ``EventUtils.scala:576-587``). Offensive
    rebounds are deliberately excluded (they're implied by one of these).

    Tried in Scala-declared order: FT made, FT missed, shot made, shot
    missed, turnover.

    Args:
        x: The raw event string.

    Returns:
        The acting player's name, or ``None`` if none of the offensive
        sub-parsers match.
    """
    for parser in (
        parse_free_throw_made,
        parse_free_throw_missed,
        parse_shot_made,
        parse_shot_missed,
        parse_turnover,
    ):
        result = parser(x)
        if result is not None:
            return result
    return None


def parse_defensive_action_event(x: str) -> Optional[str]:
    """A defensive event that tells us which side is in possession -- just
    defensive rebounds (``ParseDefensiveActionEvent``, ``EventUtils.scala:590-596``).

    Args:
        x: The raw event string.

    Returns:
        :func:`parse_defensive_rebound`'s result.
    """
    return parse_defensive_rebound(x)


def parse_defensive_info_event(x: str) -> Optional[str]:
    """A defensive event that provides context to an offensive action (e.g.
    a turnover caused by a steal) -- blocks, steals
    (``ParseDefensiveInfoEvent``, ``EventUtils.scala:599-605``).

    Args:
        x: The raw event string.

    Returns:
        The defending player's name, or ``None`` if neither a block nor a
        steal matches.
    """
    result = parse_shot_blocked(x)
    if result is not None:
        return result
    return parse_stolen(x)


def parse_defensive_event(x: str) -> Optional[str]:
    """Any defensive event -- union of :func:`parse_defensive_action_event`
    and :func:`parse_defensive_info_event` (``ParseDefensiveEvent``,
    ``EventUtils.scala:608-611``).

    Args:
        x: The raw event string.

    Returns:
        The defending player's name, or ``None`` if neither matches.
    """
    result = parse_defensive_action_event(x)
    if result is not None:
        return result
    return parse_defensive_info_event(x)
