"""Women's college basketball NCAA stint-builder core: player codes + team-name parsing.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_stints` -- the faithful
port of the player-code generator, team-name parser, the play-by-play event
ADT, event reordering, and the substitution-tracking stint builder itself
from ``ExtractorUtils.scala`` in
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(the Scala NCAA play-by-play ingestion pipeline behind hoop-explorer.com).
The stint-building logic is entirely league-agnostic -- ``ExtractorUtils``
already branches on gender internally (e.g. quarters vs. halves in
:func:`start_time_from_period`), so the same code serves both the men's and
women's college basketball index. This module re-exports the mbb core types
and functions **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``ExtractorUtils.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_stints`` module docstring and
in ``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_stints import build_player_code

        print(build_player_code("Watkins, Mike", []))

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_stints import (
    PLAYER_CODE_MAX_FRAGMENT_LENGTH,
    PLAYER_CODE_MAX_LENGTH,
    SUB_SAFETY_DELTA_MINS,
    GameBreakEvent,
    GameEndEvent,
    LineupBuildingState,
    MiscGameBreak,
    MiscGameEvent,
    OtherOpponentEvent,
    OtherTeamEvent,
    PlayByPlayEvent,
    SubEvent,
    SubInEvent,
    SubOutEvent,
    build_lineup_id,
    build_new_player_list,
    build_partial_lineup_list,
    build_player_code,
    duration_from_period,
    parse_team_name,
    remove_diacritics,
    reorder_and_reverse,
    start_time_from_period,
)

__all__ = [
    "PLAYER_CODE_MAX_LENGTH",
    "PLAYER_CODE_MAX_FRAGMENT_LENGTH",
    "remove_diacritics",
    "build_player_code",
    "parse_team_name",
    "SUB_SAFETY_DELTA_MINS",
    "SubInEvent",
    "SubOutEvent",
    "OtherTeamEvent",
    "OtherOpponentEvent",
    "GameBreakEvent",
    "GameEndEvent",
    "SubEvent",
    "MiscGameEvent",
    "MiscGameBreak",
    "PlayByPlayEvent",
    "LineupBuildingState",
    "reorder_and_reverse",
    "build_partial_lineup_list",
    "build_new_player_list",
    "build_lineup_id",
    "start_time_from_period",
    "duration_from_period",
]
