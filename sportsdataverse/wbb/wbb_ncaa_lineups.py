"""Women's college basketball lineup aggregation (wbigballR port).

Thin shims over :mod:`sportsdataverse.mbb.mbb_ncaa_lineups`. The transform
layer is league-agnostic (wbigballR's copy is byte-identical to bigballR's;
``dev/bigballr_port/design.md``) — every league knob lives upstream in the
scrape engine, so these are pure delegations that exist only to give the
``sportsdataverse.wbb`` surface its canonical ``ncaa_wbb_*`` names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, Union

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_lineups import (
    ncaa_mbb_lineups,
    ncaa_mbb_on_off,
    ncaa_mbb_player_combos,
    ncaa_mbb_player_lineups,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "ncaa_wbb_lineups",
    "ncaa_wbb_player_lineups",
    "ncaa_wbb_player_combos",
    "ncaa_wbb_on_off",
]


def ncaa_wbb_lineups(
    pbp: pl.DataFrame,
    *,
    include_transition: bool = False,
    fix_tip_in: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate WBB play-by-play into per-lineup stats (wbigballR ``get_lineups``).

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_lineups` — see it
    for the algorithm, column contract, and the ``fix_tip_in`` vocab fix.

    Args:
        pbp: Play-by-play frame in the sdv-py 35-column snake_case bigballR
            contract (``ncaa_wbb_game_pbp`` output).
        include_transition: Append the ``_trans``/``_half`` split surface.
        fix_tip_in: Count the real ``"Tip In"`` vocabulary (default); False
            reproduces R's ``"Tip-In"`` bug for oracle parity.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per lineup+team; see the MBB sibling for the column contract.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_lineups
            lineups = ncaa_wbb_lineups(pbp)
            print(lineups.shape)
    """
    return ncaa_mbb_lineups(
        pbp,
        include_transition=include_transition,
        fix_tip_in=fix_tip_in,
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_player_lineups(
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Filter a WBB lineups frame by on-court player membership.

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_player_lineups`
    (wbigballR ``get_player_lineups``).

    Args:
        lineups: Lineups frame from :func:`ncaa_wbb_lineups`.
        included: Player name(s) that must ALL be on the court.
        excluded: Player name(s) that must NONE be on the court.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Row-subset of ``lineups``; schema unchanged.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_player_lineups
            on = ncaa_wbb_player_lineups(lineups, included="TE-HINA.PAOPAO")
            print(on.shape)
    """
    return ncaa_mbb_player_lineups(
        lineups,
        included=included,
        excluded=excluded,
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_player_combos(
    lineups: pl.DataFrame,
    *,
    n: int = 2,
    min_mins: float = 0,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    include_transition: bool = False,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Team stats for every n-player WBB combination on the court together.

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_player_combos`
    (wbigballR ``get_player_combos``).

    Args:
        lineups: Lineups frame from :func:`ncaa_wbb_lineups`.
        n: Combination size, 1-5.
        min_mins: Keep combos with total on-court minutes strictly greater
            than this.
        included: Player name(s) that must be on the court in every lineup.
        excluded: Player name(s) that must be off the court in every lineup.
        include_transition: Re-derive the ``_trans``/``_half`` ratio surface.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per combo: ``team, p1..pn`` + the stat surface.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_player_combos
            combos = ncaa_wbb_player_combos(lineups, n=2)
            print(combos.shape)
    """
    return ncaa_mbb_player_combos(
        lineups,
        n=n,
        min_mins=min_mins,
        included=included,
        excluded=excluded,
        include_transition=include_transition,
        return_as_pandas=return_as_pandas,
    )


def ncaa_wbb_on_off(
    players: Union[str, Sequence[str]],
    lineups: pl.DataFrame,
    *,
    included: Union[str, Sequence[str], None] = None,
    excluded: Union[str, Sequence[str], None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Team stats for every on/off combination of the given WBB players.

    Pure delegation to
    :func:`sportsdataverse.mbb.mbb_ncaa_lineups.ncaa_mbb_on_off`
    (wbigballR ``on_off_generator``).

    Args:
        players: Player name(s) to split on (the ``status`` axis).
        lineups: Lineups frame from :func:`ncaa_wbb_lineups`.
        included: Optional membership filter forwarded to
            :func:`ncaa_wbb_player_lineups`.
        excluded: Optional membership filter forwarded to
            :func:`ncaa_wbb_player_lineups`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``2^k`` rows — ``status`` + the stat columns.

    Example:
        Quick start::

            from sportsdataverse.wbb.wbb_ncaa_lineups import ncaa_wbb_on_off
            onoff = ncaa_wbb_on_off("TE-HINA.PAOPAO", lineups)
            print(onoff.shape)
    """
    return ncaa_mbb_on_off(
        players,
        lineups,
        included=included,
        excluded=excluded,
        return_as_pandas=return_as_pandas,
    )
