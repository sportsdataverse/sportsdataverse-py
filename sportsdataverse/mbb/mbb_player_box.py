"""ESPN MBB player-box producer -- polars port of ``hoopR:::helper_espn_mbb_player_box``.

Source: ``hoopR/R/espn_mbb_data.R`` lines 3456-3769. The MBB helper is
semantically identical to the WBB one (token-ratio 0.9854; the only residual
diffs are the function name and quote style -- the final ``dplyr::select``
runs 56 columns, byte-identical order to WBB's) with two league facts that
already match WBB, NOT WNBA/NBA:

* **No ``plus_minus``.** MBB never carries a ``plusMinus``/``plus_minus``
  field anywhere in the helper.
* **Degenerate-payload guard is the STRICT gate.** MBB requires the
  ``valid_athletes`` both-teams probe (R L3484:
  ``is.data.frame(valid_stats[["athletes"]][[1]]) &&
  is.data.frame(valid_stats[["athletes"]][[2]])``) -- byte-for-byte the WBB
  gate, not WNBA/NBA's laxer one -- so ``require_both_teams=True``.

MBB's column order is WBB's shared ``_FINAL_ORDER`` with one column moved:
``active`` is LAST in the R-released ``espn_mens_college_basketball_player_boxscores``
parquet (idx 54/55), where WBB keeps it mid-list -- the single confirmed
MBB/WBB order divergence, captured in ``_MBB_FINAL_ORDER`` below (still no
plus_minus insert). That parquet is the parity oracle.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_player_box import _FINAL_ORDER as _FINAL_ORDER
from sportsdataverse.wbb.wbb_player_box import _basketball_player_box

__all__ = ["helper_mbb_player_box"]

# The real MBB release orders ``active`` LAST (confirmed against the 2025
# ``espn_mens_college_basketball_player_boxscores`` parquet, idx 54 of 55),
# whereas WBB keeps it mid-list after ``did_not_play``/``reason``. Same column
# set as WBB's shared ``_FINAL_ORDER`` -- only ``active``'s position differs --
# so derive the MBB order from it rather than changing the shared template
# (which is correct for WBB). Pinned by tests/mbb/test_mbb_player_box_order.py.
_MBB_FINAL_ORDER: tuple[str, ...] = tuple(c for c in _FINAL_ORDER if c != "active") + ("active",)


def helper_mbb_player_box(final: dict) -> pl.DataFrame:
    """Parse one game's ESPN summary payload into the released player-box frame.

    Faithful polars port of ``hoopR:::helper_espn_mbb_player_box``
    (``hoopR/R/espn_mbb_data.R:3456``). Returns one row per athlete (DNP
    athletes included with null stats), sorted away-then-home, whose column
    set, order, and dtypes match the R-released ``espn_mens_college_
    basketball_player_boxscores`` parquet -- no ``plus_minus`` column (MBB
    never carries it, matching WBB).

    Args:
        final: One game's ESPN summary JSON (the ``final.json`` payload the
            ``hoopR-mbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per athlete. Empty (zero-column) frame when the
        boxscore is unavailable or fails the validity probes (the outcome the
        R producer's tryCatch-skip yields) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_player_box
            final = json.load(open("401746082.json", encoding="utf-8"))
            df = helper_mbb_player_box(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("did_not_play") == False).select("athlete_display_name", "points")

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # MBB's R gate carries WBB's valid_athletes (both-teams) probe, so a game
    # whose second team ships no athletes is skipped entirely, not partially
    # published.
    return _basketball_player_box(final, final_order=_MBB_FINAL_ORDER, require_both_teams=True)
