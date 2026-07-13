"""ESPN WNBA team-box producer -- polars port of ``wehoop:::helper_espn_wnba_team_box``.

Source: ``wehoop/R/espn_wnba_data.R`` lines 2338-2551 (wehoop 3.0.0). The WNBA
helper is semantically identical to the WBB one after normalization (the only
textual difference -- an early dead ``game_date`` assignment overwritten before
write -- does not change values or the released column order), so this module
is a thin delegation to the shared WBB implementation. WNBA-only stat columns
(e.g. ``flagrant_fouls``) flow through the payload-driven stat spread
automatically. The R-released ``espn_wnba_team_boxscores`` parquet is the
parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_team_box import helper_wbb_team_box

__all__ = ["helper_wnba_team_box"]


def helper_wnba_team_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released team-box frame.

    Faithful polars port of ``wehoop:::helper_espn_wnba_team_box``
    (``wehoop/R/espn_wnba_data.R:2338``). Returns two rows (one per team)
    whose column set, order, and dtypes match the R-released
    ``espn_wnba_team_boxscores`` parquet. The WNBA and WBB helpers are
    value-identical, so this delegates to the shared basketball
    implementation; WNBA-only stats (``flagrant_fouls``) surface via the
    payload-driven stat spread.

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``wehoop-wnba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: Two team rows. Empty (zero-column) frame when the
        payload has no available boxscore -- season builders skip empty
        frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_team_box
            final = json.load(open("401736126.json", encoding="utf-8"))
            df = helper_wnba_team_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("team_display_name", "team_score", "team_winner")

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return helper_wbb_team_box(final)
