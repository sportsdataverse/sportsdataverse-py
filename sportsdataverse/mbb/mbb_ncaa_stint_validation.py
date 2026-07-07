"""NCAA lineup validation (men's basketball) -- ``ValidationError`` + ``validate_lineup``.

Faithful Python port of the **stint-VALIDATION half** of
``LineupErrorAnalysisUtils.scala`` in Alex-At-Home/cbb-explorer (the Scala
NCAA play-by-play ingestion pipeline behind hoop-explorer.com): the checks
that flag a built lineup stint as internally inconsistent. The
**name-resolution half** of the same Scala object (``tidy_player`` /
``build_tidy_player_context`` / ``NameFixer``) was ported in Phase 5b as
:mod:`sportsdataverse.mbb.mbb_ncaa_names`; this module consumes that surface
directly (:func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`,
:func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player`) rather than
re-deriving it. Task 5d.1 ports only ``ValidationError`` + ``allowedErrors``
+ ``validate_lineup`` (``LineupErrorAnalysisUtils.scala:18-26,181-218``) --
the ONE piece of this Scala object with an upstream oracle
(``LineupErrorAnalysisUtilsTests.scala:55-120``). The clumping/self-healing
functions (``BadLineupClump`` / ``clump_bad_lineups`` /
``handle_common_sub_bug`` / ``find_missing_subs`` / ``add_missing_players`` /
``analyze_and_fix_clumps`` / ``categorize_bad_lineups``) have NO upstream
oracle and are Tasks 5d.2/5d.3, extending this same module.

Ported members (Scala anchors in each docstring):

* :class:`ValidationError` -- the 3 ways a lineup can be declared invalid
  (``:18-20``).
* :data:`ALLOWED_ERRORS` -- the terminating filter set (``:21-26``).
* :func:`validate_lineup` -- the 3 independent checks (``:181-218``).

**Return shape: a Python ``list``, not a ``set``.** The Scala signature
returns ``Set[ValidationError.Value]``, but ``ValidationError`` is a Scala
``Enumeration``, and the oracle asserts against ``.toList`` --
``scala.collection.immutable.Set`` built from an ``Enumeration``'s
``ValueSet`` always iterates/``.toList``s in declaration (ordinal) order,
regardless of insertion order (see the multi-error oracle case:
``List(WrongNumberOfPlayers, UnknownPlayers)``, never the reverse). A plain
Python ``set`` of :class:`ValidationError` members has no such ordering
guarantee, so :func:`validate_lineup` returns a ``list`` built by appending
each failing check **in declaration order** (:data:`ValidationError`'s
member order matches the Scala's exactly) -- this reproduces the oracle's
``.toList`` order directly, with no extra sort step needed at either the
library or the test call site. Downstream 5d.3 fixers that only need the
Scala's ``.isEmpty`` idiom can use ``not validate_lineup(...)`` against this
list identically to a set.

**``tidy_ctx`` is rebuilt per call, and NOT threaded across the loop
iterations within one call.** ``validate_lineup`` calls
:func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context` fresh
every time (cheap; matches the Scala exactly, ``:195``) -- no caching
across calls. Within one call's inactive-players loop, every
:func:`~sportsdataverse.mbb.mbb_ncaa_names.tidy_player` call reads the SAME
``tidy_ctx`` object; the Scala's ``.collect { case ParseAnyPlay(player) if
... => ...tidy_player(player, tidy_ctx)._1... }`` never threads
``tidy_player``'s returned (cache-updated) context back into the next
iteration -- it always resolves against the one context built at ``:195``.
Ported verbatim: this module's loop never reassigns ``tidy_ctx``.

**License / provenance (Apache License, Version 2.0).** This module is a
derivative work of the ``ValidationError``/``allowedErrors``/
``validate_lineup`` portion of ``LineupErrorAnalysisUtils.scala`` from
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
third-party attribution entry.

Landmine index (reachable error sites, numbered across the module):
    1. None. :func:`validate_lineup` is a pure sequence of comprehensions,
       an ``all()``, and list appends -- no arithmetic, no indexing without
       a length guard, no division.

Example::

    from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
        ValidationError,
        validate_lineup,
    )

    errors = validate_lineup(lineup_event, box_lineup, valid_player_codes)
    if not errors:
        ...  # lineup is clean
    elif ValidationError.WRONG_NUMBER_OF_PLAYERS in errors:
        ...

See Also:
    * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college
      basketball data in R.
"""

from __future__ import annotations

from enum import Enum

from sportsdataverse.mbb.mbb_ncaa_events import parse_any_play
from sportsdataverse.mbb.mbb_ncaa_models import LineupEvent
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context, tidy_player
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code

__all__ = [
    "ValidationError",
    "ALLOWED_ERRORS",
    "validate_lineup",
]


class ValidationError(Enum):
    """The 3 ways a lineup can be declared invalid, in Scala declaration
    (ordinal) order (``LineupErrorAnalysisUtils.ValidationError``, ``:18-20``).
    Member order is load-bearing -- see the module docstring's "Return
    shape" note.
    """

    #: ``len(lineup.players) != 5`` (``:22``).
    WRONG_NUMBER_OF_PLAYERS = "WrongNumberOfPlayers"

    #: A player on the floor isn't in the box score (``:23``).
    UNKNOWN_PLAYERS = "UnknownPlayers"

    #: A player mentioned in a game event isn't on the floor (``:24``).
    INACTIVE_PLAYERS = "InactivePlayers"


#: The terminating filter set (``LineupErrorAnalysisUtils.allowedErrors``,
#: ``:21-26``) -- every :class:`ValidationError` is currently allowed; kept
#: as an explicit set (rather than an implicit "all of them") to mirror the
#: Scala's explicit listing and to give a single place to narrow the filter
#: later without touching :func:`validate_lineup`.
ALLOWED_ERRORS: frozenset[ValidationError] = frozenset(
    (
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
        ValidationError.UNKNOWN_PLAYERS,
        ValidationError.INACTIVE_PLAYERS,
    )
)


def validate_lineup(
    lineup_event: LineupEvent,
    box_lineup: LineupEvent,
    valid_player_codes: set[str],
) -> list[ValidationError]:
    """Flags a lineup stint as internally inconsistent, via 3 independent
    checks (``LineupErrorAnalysisUtils.validate_lineup``, ``:181-218``).

    Args:
        lineup_event: The lineup stint to validate.
        box_lineup: The team's box-score lineup event (``players`` is the
            full roster) -- used both to build the name-resolution context
            (see :func:`~sportsdataverse.mbb.mbb_ncaa_names.build_tidy_player_context`)
            and, indirectly, as the source of ``players_out`` for
            jersey-number resolution inside :func:`~sportsdataverse.mbb
            .mbb_ncaa_names.tidy_player`.
        valid_player_codes: Every player code that's actually on the box
            score / roster for this team-season.

    Returns:
        The failing :class:`ValidationError`\\ s, in declaration order (see
        the module docstring's "Return shape" note) -- empty if
        ``lineup_event`` is clean.

        * :attr:`ValidationError.WRONG_NUMBER_OF_PLAYERS` -- ``lineup_event``
          doesn't have exactly 5 players on the floor.
        * :attr:`ValidationError.UNKNOWN_PLAYERS` -- some player on the
          floor isn't in ``valid_player_codes``.
        * :attr:`ValidationError.INACTIVE_PLAYERS` -- some player mentioned
          in ``lineup_event``'s own (team-side) raw game events resolves to
          a code not in ``valid_player_codes`` (i.e. isn't on the floor,
          per the lineup being validated).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_ncaa_stint_validation import validate_lineup
            errors = validate_lineup(lineup_event, box_lineup, {"MiMitchell", "BbBob"})
            assert not errors  # a clean lineup returns []
    """
    right_number_of_players = len(lineup_event.players) == 5
    all_players_known = all(p.code in valid_player_codes for p in lineup_event.players)

    # Players mentioned in game events who aren't in the lineup (``:195-208``).
    # `tidy_ctx` is built once and never reassigned below -- see the module
    # docstring's "tidy_ctx is rebuilt per call" note.
    tidy_ctx = build_tidy_player_context(box_lineup)
    inactive_players_mentioned: list[str] = []
    for raw in lineup_event.raw_game_events:
        if raw.team is None:
            continue
        player = parse_any_play(raw.team)
        if player is None or player.lower() == "team":
            continue
        resolved_name = tidy_player(player, tidy_ctx)[0]
        code = build_player_code(resolved_name, lineup_event.team.team).code
        if code not in valid_player_codes:
            inactive_players_mentioned.append(code)

    errors: list[ValidationError] = []
    if not right_number_of_players:
        errors.append(ValidationError.WRONG_NUMBER_OF_PLAYERS)
    if not all_players_known:
        errors.append(ValidationError.UNKNOWN_PLAYERS)
    if inactive_players_mentioned:
        errors.append(ValidationError.INACTIVE_PLAYERS)

    return [e for e in errors if e in ALLOWED_ERRORS]
