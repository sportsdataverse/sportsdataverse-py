"""Women's college basketball NCAA data-quality curated tables + ``ParseError``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_data_quality` -- the
faithful port of hoop-explorer's ``cbb-explorer``
(`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_)
``DataQualityIssues.scala`` + ``ParseError.scala`` / ``ParseUtils.scala``.
The curated misspelling/alias tables and the minimal error-reporting
scaffolding are entirely league-agnostic -- they describe the same NCAA
box-score/play-by-play data-quality quirks regardless of whether the
underlying data came from the men's or women's college basketball index.
This module re-exports the mbb core tables and functions **by reference**
(not a copy) so ``sportsdataverse.wbb`` callers get the identical
implementation the mbb side uses, with no duplicated logic to drift out of
sync.

``DataQualityIssues.scala`` / ``ParseError.scala`` are upstream-licensed
under Apache License, Version 2.0; see the full attribution (copyright
notice, upstream URL, what was derived) in the
``sportsdataverse.mbb.mbb_ncaa_data_quality`` module docstring and in
``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_data_quality import team_aliases

        print(team_aliases)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_data_quality import (
    ParseError,
    alias_combos,
    build_sub_error,
    combos,
    fix_combos,
    generic_misspellings,
    misspellings,
    players_with_duplicate_names,
    team_aliases,
)

__all__ = [
    "ParseError",
    "build_sub_error",
    "combos",
    "fix_combos",
    "alias_combos",
    "generic_misspellings",
    "misspellings",
    "players_with_duplicate_names",
    "team_aliases",
]
