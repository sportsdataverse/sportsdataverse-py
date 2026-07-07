"""NCAA team-id + team-schedule HTML parsers (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_team_parsers` -- the
faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``TeamIdParser.scala`` and
``TeamScheduleParser.scala``: :func:`get_team_triples` +
:func:`build_lineup_cli_array` / :func:`build_available_team_list` (the
pure, actively-tested helpers) and :func:`get_neutral_games`. The
HTML-parsing logic is entirely league-agnostic -- the same team-id/schedule
selector tables apply regardless of whether the underlying page came from
the men's or women's college basketball index. This module re-exports the
mbb core symbols **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``TeamIdParser.scala`` / ``TeamScheduleParser.scala`` are upstream-licensed
under Apache License, Version 2.0; see the full attribution (copyright
notice, upstream URL, what was derived) in the
``sportsdataverse.mbb.mbb_ncaa_team_parsers`` module docstring and in
``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_team_parsers import get_neutral_games

        neutral_by_team = get_neutral_games("attendance.html", html_text)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_team_parsers import (
    ScheduleBuilders,
    build_available_team_list,
    build_lineup_cli_array,
    get_neutral_games,
    get_team_triples,
    v0_builders,
    v1_builders,
)

__all__ = [
    "get_team_triples",
    "build_lineup_cli_array",
    "build_available_team_list",
    "ScheduleBuilders",
    "v0_builders",
    "v1_builders",
    "get_neutral_games",
]
