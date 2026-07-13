"""ESPN MBB team-box producer -- polars port of ``hoopR:::helper_espn_mbb_team_box``.

Source: ``hoopR/R/espn_mbb_data.R`` lines 3236-3455. The MBB helper is
semantically identical to the WBB one (token-ratio 0.9835; the only
non-``equal`` hunk is the function name + ``'single'`` vs ``"double"`` quote
style), so this module is a thin delegation to the shared WBB implementation.
The canonical final ``dplyr::select`` (19 cols) is identical in order, so the
released column order matches and the stat tail stays payload-driven. The
R-released ``espn_mens_college_basketball_team_boxscores`` parquet is the
parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_team_box import helper_wbb_team_box

__all__ = ["helper_mbb_team_box"]


def helper_mbb_team_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released team-box frame.

    Faithful polars port of ``hoopR:::helper_espn_mbb_team_box``
    (``hoopR/R/espn_mbb_data.R:3236``). Returns two rows (one per team) whose
    column set, order, and dtypes match the R-released ``espn_mens_college_
    basketball_team_boxscores`` parquet. The MBB and WBB helpers are
    value-identical, so this delegates to the shared basketball
    implementation; league-specific stats surface via the payload-driven
    stat spread.

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``hoopR-mbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: Two team rows. Empty (zero-column) frame when the
        payload has no available boxscore -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_team_box
            final = json.load(open("401746082.json", encoding="utf-8"))
            df = helper_mbb_team_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("team_display_name", "team_score", "team_winner")

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_team_box(final)
