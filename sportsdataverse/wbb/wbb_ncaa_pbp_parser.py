"""NCAA play-by-play HTML parser + full-pipeline orchestration (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_pbp_parser` -- the faithful
port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``, package
``org.piggottfamily.cbb_explorer``) ``PlayByPlayParser.scala``: the v0/v1
event-line parsers, :func:`enrich_and_reverse_game_events`,
:func:`parse_game_events`, :func:`get_sorted_pbp_events`, and
:func:`create_lineup_data` -- the orchestrator that chains the ENTIRE ported
Phase 5a-5d surface into validated lineup stints. The HTML-parsing and
pipeline-orchestration logic is entirely league-agnostic -- the same v0/v1
builder tables and event-reconciliation rules apply regardless of whether
the underlying page came from the men's or women's college basketball index
(``PlayByPlayParser.is_women_game`` is itself just a first-event-clock
heuristic, ported unchanged in the mbb module). This module re-exports the
mbb core symbols **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``PlayByPlayParser.scala`` is upstream-licensed under Apache License,
Version 2.0; see the full attribution (copyright notice, upstream URL, what
was derived) in the ``sportsdataverse.mbb.mbb_ncaa_pbp_parser`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_pbp_parser import create_lineup_data

        good, bad = create_lineup_data("pbp.html", html_text, box_lineup, format_version=1)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_pbp_parser import (
    PbpBuilders,
    create_lineup_data,
    enrich_and_reverse_game_events,
    get_sorted_pbp_events,
    parse_desc_game_time,
    parse_game_event,
    parse_game_events,
    parse_game_score,
    v0_builders,
    v1_builders,
)

__all__ = [
    "PbpBuilders",
    "v0_builders",
    "v1_builders",
    "parse_game_score",
    "parse_desc_game_time",
    "parse_game_event",
    "enrich_and_reverse_game_events",
    "parse_game_events",
    "get_sorted_pbp_events",
    "create_lineup_data",
]
