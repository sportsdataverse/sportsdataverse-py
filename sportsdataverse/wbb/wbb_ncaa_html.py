"""Shared JSoup->bs4 selector/text helpers for the NCAA HTML-parser layer (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_html` -- the faithful port
of hoop-explorer's ``cbb-explorer`` (Scala 2.12, package
``org.piggottfamily.cbb_explorer``) JSoup-selector-translation helpers that
every NCAA HTML parser (roster/box-score/play-by-play/team/shot) in Phase 5e
reuses. The selector/text translation is entirely league-agnostic -- the same
JSoup-vs-soupsieve divergences (``:eq()``, ``:matches()``/``:matchesOwn()``,
attribute-regex, ``:contains()`` case-sensitivity) apply regardless of
whether the underlying page came from the men's or women's college
basketball index. This module re-exports the mbb core helpers **by
reference** (not a copy) so ``sportsdataverse.wbb`` callers get the identical
implementation the mbb side uses, with no duplicated logic to drift out of
sync.

**Note on shim scope.** Unlike the parser modules this helper module
supports, ``mbb_ncaa_html`` has no dedicated upstream Scala file of its own
-- it is this port's shared translation layer for the selector/text idioms
used across ``RosterParser.scala`` / ``BoxscoreParser.scala`` /
``PlayByPlayParser.scala`` / ``TeamIdParser.scala`` / ``TeamScheduleParser
.scala`` / ``ShotEventParser.scala``. It is still shimmed here (rather than
left mbb-only) to match this port's established convention -- set by
:mod:`sportsdataverse.mbb.mbb_ncaa_names` / :mod:`sportsdataverse.wbb.wbb_ncaa_names`
-- of mirroring every distinct top-level ``mbb_ncaa_*`` module with a wbb
by-reference shim, whether or not the module is itself HTML/data-facing.

``cbb-explorer`` is upstream-licensed under Apache License, Version 2.0; see
the full attribution (copyright notice, upstream URL, what was derived) in
the ``sportsdataverse.mbb.mbb_ncaa_html`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_html import jsoup_text, parse_html, td_at

        soup = parse_html("<table><tr><td>1</td><td>Name</td></tr></table>")
        row = soup.select_one("tr")
        jsoup_text(td_at(row, 1))  # "Name"

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_html import (
    attr_regex_filter,
    filter_matching_own,
    jsoup_text,
    parse_html,
    select_contains,
    select_matching,
    select_matching_own,
    td_at,
)

__all__ = [
    "parse_html",
    "jsoup_text",
    "td_at",
    "select_matching",
    "select_matching_own",
    "attr_regex_filter",
    "select_contains",
    "filter_matching_own",
]
