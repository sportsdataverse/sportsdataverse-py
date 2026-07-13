"""Women's college basketball possession segmenter (wbigballR port).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_possession_seg`. The
transform layer is league-agnostic (wbigballR's copy is byte-identical to
bigballR's; ``dev/bigballr_port/design.md``), so this is a pure delegation
providing the canonical ``ncaa_wbb_*`` name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_possession_seg import ncaa_mbb_possessions

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "ncaa_wbb_possessions",
]


def ncaa_wbb_possessions(
    pbp: pl.DataFrame,
    *,
    simple: bool = False,
    fix_cross_game_leak: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate WBB play-by-play into one row per possession (wbigballR ``get_possessions``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_possession_seg.ncaa_mbb_possessions`
    — see it for the algorithm, the 28/17-column contracts, and the fixed-vs-
    faithful flag convention.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``ncaa_wbb_game_pbp`` output).
        simple: Return only the 17-column possession/points frame.
        fix_cross_game_leak: When True (default, and the CORRECT behavior),
            window the ``start_event_type`` lag with ``.over("game_id")`` so a
            game's first possession does not inherit the previous game's last
            event. When False, reproduce R's ungrouped ``dplyr::lag``
            (``all_functions.R:3698``). Parity tests pass False.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per possession.

    Note:
        The technical/flagrant possession rule is applied UPSTREAM, in the
        chain that stamps ``poss_num`` / ``poss_team`` — see
        :func:`sportsdataverse.mbb.mbb_ncaa_game_pbp.parse_ncaa_bb_game_pbp`'s
        ``fix_technicals`` flag (``ncaa_wbb_game_pbp`` inherits it).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_possession_seg import ncaa_wbb_possessions
            poss = ncaa_wbb_possessions(pbp)
            print(poss.shape)

        Faithful (R-buggy) start-event lag::

            poss = ncaa_wbb_possessions(pbp, fix_cross_game_leak=False)
    """
    return ncaa_mbb_possessions(
        pbp,
        simple=simple,
        fix_cross_game_leak=fix_cross_game_leak,
        return_as_pandas=return_as_pandas,
    )
