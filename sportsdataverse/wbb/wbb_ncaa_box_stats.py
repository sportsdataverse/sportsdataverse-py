"""Women's college basketball box scores from stats.ncaa.org (wbigballR port).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_box_stats`. The
``/contests/{id}/individual_stats`` page layout and the parser are
league-agnostic (wbigballR's ``scrape_box`` is the same fork code; the
``PF``/``Fouls`` header difference is handled by the shared tolerant rename
dict — ``dev/bigballr_port/design.md``), so this is a pure delegation
providing the canonical ``ncaa_wbb_*`` name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

from sportsdataverse.mbb.mbb_ncaa_box_stats import ncaa_mbb_box_scores

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    import polars as pl

__all__ = [
    "ncaa_wbb_box_scores",
]


def ncaa_wbb_box_scores(
    game_ids: Sequence[object],
    *,
    multi_games: bool = False,
    fetcher: Optional[Any] = None,
    return_as_pandas: bool = False,
) -> Union["pl.DataFrame", "pd.DataFrame"]:
    """Scrape WBB per-player box scores (wbigballR ``get_box_scores``/``scrape_box``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_box_stats.ncaa_mbb_box_scores` — see
    it for the column contract, the tolerant header renames, and the fixed
    ``multi_games`` aggregation (R's groups by a ``Pos`` column the current
    markup no longer ships).

    Args:
        game_ids: NCAA contest ids; ``None``/NaN entries are dropped.
        multi_games: Aggregate per player across all games (fixed grouping on
            player/clean_name/team).
        fetcher: Optional injected fetcher exposing
            ``fetch_game_individual_stats`` (tests/offline). Defaults to a
            fresh ``NcaaFetcher.with_browser()`` context per call.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-player box rows (or per-player aggregates with ``multi_games``).

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
            box = ncaa_wbb_box_scores(["5722355"])
            print(box.shape)
    """
    return ncaa_mbb_box_scores(
        game_ids,
        multi_games=multi_games,
        fetcher=fetcher,
        return_as_pandas=return_as_pandas,
    )
