"""NCAA roster HTML parser (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_roster_parser` -- the
faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``RosterParser.scala``:
:func:`parse_roster` (turns a saved NCAA roster page into a list of
:class:`~sportsdataverse.mbb.mbb_ncaa_models.RosterEntry`) and
:func:`get_unified_ncaa_id`. The HTML-parsing logic (including the
Scala-``HashMap``-iteration-order emulation used for same-``gp`` tie
ordering) is entirely league-agnostic -- the same v0/v1 selector tables and
dedup rules apply regardless of whether the underlying page came from the
men's or women's college basketball index. This module re-exports the mbb
core functions **by reference** (not a copy) so ``sportsdataverse.wbb``
callers get the identical implementation the mbb side uses, with no
duplicated logic to drift out of sync.

``RosterParser.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_roster_parser`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_roster_parser import parse_roster

        entries = parse_roster("roster.html", html_text, team_id, version_format=0)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_roster_parser import (
    get_unified_ncaa_id,
    parse_roster,
)

__all__ = [
    "parse_roster",
    "get_unified_ncaa_id",
]
