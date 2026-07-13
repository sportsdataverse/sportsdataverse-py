"""ESPN NBA player-box producer -- polars port of ``hoopR:::helper_espn_nba_player_box``.

Source: ``hoopR/R/espn_nba_data.R`` lines 3604-3996. The NBA helper is
semantically identical to the WBB/WNBA one (50/50 statements; the only hunk is
the ``make_hoopR_data`` label + restyle) with two league facts that already
match WNBA:

* **``plus_minus`` -- NBA HAS IT**, in exactly the WNBA slot (final select runs
  ``… "fouls", "plus_minus", "points", "starter" …``). R keeps it String
  (values like ``"+16"``) and never casts it -- same as WNBA.
* **Degenerate-payload guard is the LAX gate** (no ``valid_athletes`` both-teams
  probe -- byte-for-byte the WNBA gate), so NBA wants ``require_both_teams=False``.

The WNBA final-select order (WBB order with ``plus_minus`` after ``fouls``) is
imported verbatim. The R-released ``espn_nba_player_boxscores`` parquet is the
parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_player_box import _basketball_player_box
from sportsdataverse.wnba.wnba_player_box import _FINAL_ORDER as _FINAL_ORDER

__all__ = ["helper_nba_player_box"]


def helper_nba_player_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released player-box frame.

    Faithful polars port of ``hoopR:::helper_espn_nba_player_box``
    (``hoopR/R/espn_nba_data.R:3604``). Returns one row per athlete (DNP
    athletes included with null stats), sorted away-then-home, whose column
    set, order, and dtypes match the R-released ``espn_nba_player_boxscores``
    parquet -- including the ``plus_minus`` column (String; R never casts it).

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``hoopR-nba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per athlete. Empty (zero-column) frame when the
        boxscore is unavailable or fails the validity probes (the outcome the
        R producer's tryCatch-skip yields) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_player_box
            final = json.load(open("401766128.json", encoding="utf-8"))
            df = helper_nba_player_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points", "plus_minus")

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # NBA's R gate omits WBB's valid_athletes (both-teams) probe (matches WNBA),
    # so a game whose second team ships no athletes still publishes team-1 rows.
    return _basketball_player_box(final, final_order=_FINAL_ORDER, require_both_teams=False)
