"""ESPN WNBA player-box producer -- polars port of ``wehoop:::helper_espn_wnba_player_box``.

Source: ``wehoop/R/espn_wnba_data.R`` lines 2552-2851 (wehoop 3.0.0). The WNBA
helper is semantically identical to the WBB one (``wbb_player_box.py``) except
that the canonical final select includes ``plus_minus`` between ``fouls`` and
``points`` (ESPN ships +/- for WNBA athletes; R keeps it String -- values like
``"+16"`` -- and never casts it). WNBA's laxer degenerate-payload guards
converge to the same empty-frame outcome via the script-level tryCatch, so the
shared core's probes are used unchanged. The R-released
``espn_wnba_player_boxscores`` parquet is the parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_player_box import (
    _FINAL_ORDER as _WBB_FINAL_ORDER,
)
from sportsdataverse.wbb.wbb_player_box import (
    _basketball_player_box,
)

# R's WNBA final select = the WBB select with plus_minus after fouls
# (espn_wnba_data.R final dplyr::select list).
_PLUS_MINUS_AT = _WBB_FINAL_ORDER.index("fouls") + 1
_FINAL_ORDER: tuple[str, ...] = _WBB_FINAL_ORDER[:_PLUS_MINUS_AT] + ("plus_minus",) + _WBB_FINAL_ORDER[_PLUS_MINUS_AT:]

__all__ = ["helper_wnba_player_box"]


def helper_wnba_player_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released player-box frame.

    Faithful polars port of ``wehoop:::helper_espn_wnba_player_box``
    (``wehoop/R/espn_wnba_data.R:2552``). Returns one row per athlete (DNP
    athletes included with null stats), sorted away-then-home, whose column
    set, order, and dtypes match the R-released
    ``espn_wnba_player_boxscores`` parquet -- including the WNBA-only
    ``plus_minus`` column (String; R never casts it).

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``wehoop-wnba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per athlete. Empty (zero-column) frame when the
        boxscore is unavailable or fails the validity probes (the outcome the
        R producer's tryCatch-skip yields) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_player_box
            final = json.load(open("401736126.json", encoding="utf-8"))
            df = helper_wnba_player_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points", "plus_minus")

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    # WNBA's R gate omits WBB's valid_athletes (both-teams) probe, so a game
    # whose second team ships no athletes still publishes its team-1 rows.
    return _basketball_player_box(final, final_order=_FINAL_ORDER, require_both_teams=False)
