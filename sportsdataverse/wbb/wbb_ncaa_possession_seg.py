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
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate WBB play-by-play into one row per possession (wbigballR ``get_possessions``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_possession_seg.ncaa_mbb_possessions`
    — see it for the algorithm, the 28/17-column contracts, and the faithful
    ungrouped-lag quirk.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``ncaa_wbb_game_pbp`` output).
        simple: Return only the 17-column possession/points frame.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per possession.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_possession_seg import ncaa_wbb_possessions
            poss = ncaa_wbb_possessions(pbp)
            print(poss.shape)
    """
    return ncaa_mbb_possessions(
        pbp,
        simple=simple,
        return_as_pandas=return_as_pandas,
    )
