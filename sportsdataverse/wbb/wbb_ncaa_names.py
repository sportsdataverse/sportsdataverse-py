"""Women's college basketball NCAA name-resolution chain (``tidy_player`` + ``NameFixer``).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_names` -- the faithful port
of the name-resolution half of ``LineupErrorAnalysisUtils.scala`` in
`Alex-At-Home/cbb-explorer <https://github.com/Alex-At-Home/cbb-explorer>`_
(the Scala NCAA play-by-play ingestion pipeline behind hoop-explorer.com).
The substitution-event name -> box-score-player reconciliation chain is
entirely league-agnostic -- the same fuzzy-matching / initials / digit
fallback logic applies regardless of whether the underlying data came from
the men's or women's college basketball index. This module re-exports the
mbb core types and functions **by reference** (not a copy) so
``sportsdataverse.wbb`` callers get the identical implementation the mbb
side uses, with no duplicated logic to drift out of sync.

``LineupErrorAnalysisUtils.scala`` is upstream-licensed under Apache
License, Version 2.0; see the full attribution (copyright notice, upstream
URL, what was derived) in the ``sportsdataverse.mbb.mbb_ncaa_names`` module
docstring and in ``NOTICE`` at the repository root.

Example:
    Quick start::

        from sportsdataverse.wbb.wbb_ncaa_names import tidy_player

        print(tidy_player)

See Also:
    * `wehoop`_ -- R-side women's college basketball data + on/off analysis.
    * `hoopR`_ -- men's college basketball counterpart.

.. _wehoop: https://wehoop.sportsdataverse.org
.. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_names import (
    MIN_FIRST_NAME_SCORE,
    MIN_OVERALL_SCORE,
    MIN_SURNAME_SCORE,
    MIN_USEFUL_FIRST_NAME_LEN,
    MIN_USEFUL_SURNAME_LEN,
    FuzzyMatchError,
    MatchResult,
    NoSurnameMatch,
    StrongSurnameMatch,
    TidyPlayerContext,
    WeakSurnameMatch,
    box_aware_compare,
    build_tidy_player_context,
    code_from_box,
    display_name_to_roster_key,
    convert_from_digits,
    convert_from_initials,
    fuzzy_box_match,
    tidy_player,
)

__all__ = [
    "MIN_SURNAME_SCORE",
    "MIN_FIRST_NAME_SCORE",
    "MIN_OVERALL_SCORE",
    "MIN_USEFUL_SURNAME_LEN",
    "MIN_USEFUL_FIRST_NAME_LEN",
    "TidyPlayerContext",
    "build_tidy_player_context",
    "tidy_player",
    "code_from_box",
    "display_name_to_roster_key",
    "convert_from_initials",
    "convert_from_digits",
    "NoSurnameMatch",
    "WeakSurnameMatch",
    "StrongSurnameMatch",
    "MatchResult",
    "FuzzyMatchError",
    "box_aware_compare",
    "fuzzy_box_match",
]
