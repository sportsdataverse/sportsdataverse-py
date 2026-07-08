"""Women's college basketball NCAA possession-core data models.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_models` -- the faithful port
of hoop-explorer's ``cbb-explorer`` (Scala 2.12, package
``org.piggottfamily.cbb_explorer``) data-model layer
(`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_).
The identity/value types, the raw-event record, and the possession-fragment
types are entirely league-agnostic -- they describe the same NCAA
play-by-play line shape regardless of whether the underlying data came from
the men's or women's college basketball index. This module re-exports the
mbb core types and functions **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation the mbb side
uses, with no duplicated logic to drift out of sync.

**Phase 5e additions.** :class:`RosterEntry` (Task 5e.1, the roster-page
model), :class:`ConferenceId` (Task 5e.4, the team-parser conference id), and
:class:`ShotLocation` / :class:`ShotGeo` / :class:`ShotEvent` /
:class:`CutdownShotEvent` (Task 5e.5, the shot-chart models) were appended to
``mbb_ncaa_models.py`` by the HTML-parser-layer port and are re-exported here
by the same by-reference convention as the Phase 5a/5c models above.

``cbb-explorer`` is upstream-licensed under Apache License, Version 2.0; see
the full attribution (copyright notice, upstream URL, what was derived) in
the ``sportsdataverse.mbb.mbb_ncaa_models`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_models import TeamId, RawGameEvent

        ev = RawGameEvent(team=TeamId("Duke"), opponent=TeamId("UNC"), info="...")
        print(ev.team)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_models import (
    AssistEvent,
    AssistInfo,
    ConferenceId,
    CutdownShotEvent,
    Direction,
    FieldGoalStats,
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerEvent,
    PlayerId,
    PlayerShotInfo,
    PossCalcFragment,
    PossessionEvent,
    RawGameEvent,
    RosterEntry,
    Score,
    ScoreInfo,
    ShotClockStats,
    ShotEvent,
    ShotGeo,
    ShotLocation,
    TeamId,
    TeamSeasonId,
    Year,
    poss_calc_fragment_sum,
    score_to_tuple,
)

__all__ = [
    "LocationType",
    "Score",
    "TeamId",
    "PlayerId",
    "Year",
    "TeamSeasonId",
    "Direction",
    "RawGameEvent",
    "PossessionEvent",
    "ScoreInfo",
    "LineupId",
    "PlayerCodeId",
    "ShotClockStats",
    "FieldGoalStats",
    "AssistEvent",
    "AssistInfo",
    "PlayerShotInfo",
    "LineupEventStats",
    "LineupEvent",
    "PossCalcFragment",
    "poss_calc_fragment_sum",
    "score_to_tuple",
    "PlayerEvent",
    "RosterEntry",
    "ConferenceId",
    "ShotLocation",
    "ShotGeo",
    "ShotEvent",
    "CutdownShotEvent",
]
