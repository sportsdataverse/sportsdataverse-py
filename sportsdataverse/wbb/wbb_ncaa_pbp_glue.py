"""NCAA play-by-play / shot enrichment glue (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_pbp_glue` -- the faithful
port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``, package
``org.piggottfamily.cbb_explorer``) ``PlayByPlayUtils.scala``: the two
pure-logic glue functions that stitch the shot-event, play-by-play, and
lineup (stint) surfaces together --
:func:`inject_starting_lineup_into_box` (infer the starting five from
play-by-play sub sequencing) and :func:`enrich_shot_events_with_pbp` (match
each :class:`~sportsdataverse.mbb.mbb_ncaa_models.ShotEvent` to its
play-by-play event and on-floor lineup) -- plus the whole
``ShotEnrichmentUtils`` helper family (:func:`find_lineup`,
:func:`find_pbp_clump`, :func:`matching_player`,
:func:`extract_player_from_ev`, :func:`right_kind_of_shot`,
:func:`shot_value`, :class:`PeekableIterator`). The enrichment logic is pure
lineup/shot/play-by-play stitching over the already-league-agnostic
:mod:`~sportsdataverse.mbb.mbb_ncaa_models` types -- entirely league-agnostic
itself, so the same code serves both the men's and women's college
basketball index. This module re-exports the mbb core symbols **by
reference** (not a copy) so ``sportsdataverse.wbb`` callers get the
identical implementation the mbb side uses, with no duplicated logic to
drift out of sync.

``PlayByPlayUtils.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_pbp_glue`` module docstring
and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_pbp_glue import enrich_shot_events_with_pbp

        enriched = enrich_shot_events_with_pbp(shots, pbp_events, lineup_events)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    enrich_shot_events_with_pbp,
    extract_player_from_ev,
    find_lineup,
    find_pbp_clump,
    inject_starting_lineup_into_box,
    matching_player,
    right_kind_of_shot,
    shot_value,
)

__all__ = [
    "PeekableIterator",
    "inject_starting_lineup_into_box",
    "enrich_shot_events_with_pbp",
    "find_lineup",
    "find_pbp_clump",
    "matching_player",
    "extract_player_from_ev",
    "right_kind_of_shot",
    "shot_value",
]
