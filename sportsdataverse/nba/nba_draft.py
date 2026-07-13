"""ESPN NBA draft release producer.

Port of the season-draft parser in ``hoopR-nba-data/R/espn_nba_08_draft_creation.R``.
That script is **byte-identical after league normalization** to its WNBA sibling
(``wehoop-wnba-data/R/espn_wnba_08_draft_creation.R``, similarity 1.0000): same
``parse_one_pick``, same 35-column output, same ``distinct()`` +
``arrange(across(any_of(c("overall_pick", "round", "pick"))))``. ``parse_one_pick``
is league-agnostic (it takes only ``payload`` + ``season``; the league lives in
the caller's raw path) and already handles NBA's payload shape -- scalar
``pk$teamId`` (via the ``team$id %|% pk$teamId`` fallback), the flat top-level
``picks[]`` array with ``rounds`` as an integer count, and the richer
``athlete.college{}`` block that populates the ``college_*`` columns for NBA.

So the NBA producer **reuses the shared** ``helper_wnba_draft`` verbatim; only
the season builder's raw path differs (``hoopR-nba-raw/nba/draft/json/{y}.json``).
The R-released ``espn_nba_draft`` parquet is the parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wnba.wnba_draft import helper_wnba_draft

__all__ = ["helper_nba_draft"]


def helper_nba_draft(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse one season's stored draft JSON into the released draft frame.

    Faithful polars port of the script-local ``parse_one_pick`` /
    ``build_season_draft`` parsers in
    ``hoopR-nba-data/R/espn_nba_08_draft_creation.R`` -- byte-identical to the
    WNBA parser after league normalization, so this delegates to the shared
    ``helper_wnba_draft``. Handles both payload shapes (``rounds[]`` of round
    objects, and the modern flat top-level ``picks[]`` with ``rounds`` as an
    integer count). Column set, order, and dtypes match the R-released
    ``espn_nba_draft`` parquet: 35 columns, Int32 ids/ordinals, String
    everything else.

    Args:
        payload: The season's ``nba/draft/json/{year}.json`` as a dict.
        season: Draft year the payload belongs to.

    Returns:
        pl.DataFrame: One row per pick, deduped and sorted by ``overall_pick``,
        ``round``, ``pick``. Empty (zero-column) frame when no picks parse --
        season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_draft
            payload = json.load(open("2025.json", encoding="utf-8"))
            df = helper_nba_draft(payload, season=2025)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("overall_pick", "athlete_display_name", "team_id").head()

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wnba_draft(payload, season=season)
