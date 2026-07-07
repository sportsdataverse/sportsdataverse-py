"""NCAA box-score HTML parser: ``get_box_lineup`` (women's basketball).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_boxscore_parser` -- the
faithful port of hoop-explorer's ``cbb-explorer`` (Scala 2.12, ``utest``,
package ``org.piggottfamily.cbb_explorer``) ``BoxscoreParser.scala``:
:func:`get_box_lineup` (turns a saved NCAA box-score page into a
:class:`~sportsdataverse.mbb.mbb_ncaa_models.LineupEvent`) plus its selector
helpers, :func:`inject_validated_players`, and :func:`validate_box_score`.
The HTML-parsing and validated-roster-reconciliation logic is entirely
league-agnostic -- the same v0/v1 selector tables and validation rules apply
regardless of whether the underlying page came from the men's or women's
college basketball index. This module re-exports the mbb core functions **by
reference** (not a copy) so ``sportsdataverse.wbb`` callers get the
identical implementation the mbb side uses, with no duplicated logic to
drift out of sync.

``BoxscoreParser.scala`` is upstream-licensed under Apache License, Version
2.0; see the full attribution (copyright notice, upstream URL, what was
derived) in the ``sportsdataverse.mbb.mbb_ncaa_boxscore_parser`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_boxscore_parser import get_box_lineup

        lineup = get_box_lineup("lineup.html", html_text, team_id, format_version=0)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import (
    get_box_lineup,
    inject_validated_players,
    parse_date,
    parse_final_score,
    parse_period_from_filename,
    parse_players_from_boxscore,
    validate_box_score,
)

__all__ = [
    "get_box_lineup",
    "inject_validated_players",
    "parse_period_from_filename",
    "parse_date",
    "parse_final_score",
    "parse_players_from_boxscore",
    "validate_box_score",
]
