"""NCAA shot-event SVG/HTML parser (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_shot_parser` -- the
faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``ShotEventParser.scala``:
:func:`create_shot_event_data` (the v1-only shot-chart page ->
:class:`~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` list pipeline),
the ``addShot(...)`` JS-to-HTML shim (:func:`shot_js_to_html`), the SVG
``circle.shot`` parser (:func:`parse_shot_html`), the court-geometry
transform (:func:`transform_shot_location`), and the self-correcting
side-flip pass. The SVG-parsing and geometry logic is entirely
league-agnostic -- the same shot-chart shape and court-orientation rules
apply regardless of whether the underlying page came from the men's or
women's college basketball index (this module's own
:func:`is_women_game` variant is copied byte-for-byte from the Scala, which
independently re-derives the same first-event-clock heuristic used by
``PlayByPlayParser.is_women_game``). This module re-exports the mbb core
symbols **by reference** (not a copy) so ``sportsdataverse.wbb`` callers get
the identical implementation the mbb side uses, with no duplicated logic to
drift out of sync.

``ShotEventParser.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_shot_parser`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_shot_parser import create_shot_event_data

        shots = create_shot_event_data("shots.html", html_text, box_lineup)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_shot_parser import (
    ShotEventBuilders,
    ShotMapDimensions,
    build_base_event,
    create_shot_event_data,
    get_ascending_time,
    is_team_shooting_left_to_start,
    is_women_game,
    parse_shot_html,
    phase1_shot_event_enrichment,
    shot_js_to_html,
    transform_shot_location,
    v1_builders,
)

__all__ = [
    "ShotMapDimensions",
    "ShotEventBuilders",
    "v1_builders",
    "create_shot_event_data",
    "shot_js_to_html",
    "parse_shot_html",
    "build_base_event",
    "phase1_shot_event_enrichment",
    "get_ascending_time",
    "is_team_shooting_left_to_start",
    "is_women_game",
    "transform_shot_location",
]
