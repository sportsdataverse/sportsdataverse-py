"""NCAA lineup validation (women's basketball) -- ``ValidationError`` + ``validate_lineup``.

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_stint_validation` -- the
faithful port of the stint-**validation** half of hoop-explorer's
``cbb-explorer`` (Scala 2.12, ``utest``, package
``org.piggottfamily.cbb_explorer.utils.parsers.ncaa``)
``LineupErrorAnalysisUtils.scala``: ``ValidationError`` / ``validate_lineup``,
the clump-grouping (``BadLineupClump`` / ``clump_bad_lineups`` /
``categorize_bad_lineups``), and the self-healing fixers
(``handle_common_sub_bug`` / ``find_missing_subs`` / ``add_missing_players`` /
``analyze_and_fix_clumps``). The validation logic is entirely
league-agnostic -- it operates purely on the already-league-agnostic
:mod:`~sportsdataverse.mbb.mbb_ncaa_models` stat-tree types and the
already-league-agnostic :mod:`~sportsdataverse.mbb.mbb_ncaa_names` name-
resolution surface, so the same code serves both the men's and women's
college basketball index. This module re-exports the mbb core symbols **by
reference** (not a copy) so ``sportsdataverse.wbb`` callers get the
identical implementation the mbb side uses, with no duplicated logic to
drift out of sync.

``LineupErrorAnalysisUtils.scala`` is upstream-licensed under Apache License,
Version 2.0; see the full attribution (copyright notice, upstream URL, what
was derived) in the ``sportsdataverse.mbb.mbb_ncaa_stint_validation`` module
docstring and in ``THIRD_PARTY_NOTICES.md`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_stint_validation import validate_lineup

        errors = validate_lineup(lineup, plays)
        if not errors:
            ...  # lineup is internally consistent

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_stint_validation import (
    ALLOWED_ERRORS,
    BadLineupClump,
    ValidationError,
    add_missing_players,
    analyze_and_fix_clumps,
    categorize_bad_lineups,
    clump_bad_lineups,
    find_missing_subs,
    handle_common_sub_bug,
    validate_lineup,
)

__all__ = [
    "ValidationError",
    "ALLOWED_ERRORS",
    "validate_lineup",
    "BadLineupClump",
    "clump_bad_lineups",
    "categorize_bad_lineups",
    "handle_common_sub_bug",
    "find_missing_subs",
    "add_missing_players",
    "analyze_and_fix_clumps",
]
