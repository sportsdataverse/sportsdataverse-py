"""ESPN NBA team-box producer -- polars port of ``hoopR:::helper_espn_nba_team_box``.

Source: ``hoopR/R/espn_nba_data.R`` lines 3287-3597. The NBA helper is
semantically identical to the WBB/WNBA one (82/82 statements; the only
non-``equal`` hunk is the ``make_hoopR_data`` attribution label), so this module
is a thin delegation to the shared WBB implementation. The canonical
``dplyr::select(any_of(c(game_id … team_winner)), everything())`` head is
character-identical, so the released column order matches and the stat tail
stays payload-driven. The R-released ``espn_nba_team_boxscores`` parquet is the
parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_team_box import helper_wbb_team_box

__all__ = ["helper_nba_team_box"]


def helper_nba_team_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released team-box frame.

    Faithful polars port of ``hoopR:::helper_espn_nba_team_box``
    (``hoopR/R/espn_nba_data.R:3287``). Returns two rows (one per team) whose
    column set, order, and dtypes match the R-released
    ``espn_nba_team_boxscores`` parquet. The NBA and WBB/WNBA helpers are
    value-identical, so this delegates to the shared basketball implementation;
    league-specific stats surface via the payload-driven stat spread.

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``hoopR-nba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: Two team rows. Empty (zero-column) frame when the
        payload has no available boxscore -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_team_box
            final = json.load(open("401766128.json", encoding="utf-8"))
            df = helper_nba_team_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("team_display_name", "team_score", "team_winner")

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_team_box(final)
