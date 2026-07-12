"""UFL EP/WP/CP -- by-reference shim over the shared spring-football port."""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.football.spring_football_ep_wp import build_spring_football_pbp, enrich_spring_football_pbp
from sportsdataverse.football.ufl.ufl_espn_ext import espn_ufl_summary

if TYPE_CHECKING:
    import pandas as pd


def ufl_pbp(
    game_id: Union[str, int],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Enriched UFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

    Same shared spring-football core as :func:`~sportsdataverse.football.xfl.xfl_pbp`
    (see :mod:`sportsdataverse.football.spring_football_ep_wp`).

    **Capture finding:** ESPN publishes no play-by-play for UFL games as of
    this port -- verified empty (``summary.drives`` AND the Core v2
    ``.../plays`` endpoint) across every completed 2024 + 2025 UFL game. This
    function returns a zero-row (contract-shaped) frame on today's real data
    -- not a stub -- and will pick up real rows automatically once ESPN
    backfills UFL play-by-play. See
    ``tests/fixtures/league_ports/FEASIBILITY.md``.

    Args:
        game_id: ESPN UFL event id.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        One row per play with ``ep``/``epa``/``wp``/``wpa``/``cp``/``cpoe``
        and the other ``enrich_nfl_pbp`` output columns. Zero rows today for
        every UFL game (see capture finding above).

    Example:
        Quick start::

            from sportsdataverse.football.ufl import ufl_pbp

            df = ufl_pbp("401638299")
            print(df.height)  # 0 today -- see the capture-finding note above

        See Also:
            * `nflfastR`_ -- the R package whose EP/WP models this ports.

        .. _nflfastR: https://www.nflfastr.com
    """
    summary = espn_ufl_summary(game_id, return_parsed=False)
    pbp = build_spring_football_pbp(summary, league="ufl")
    return enrich_spring_football_pbp(pbp, league="ufl", return_as_pandas=return_as_pandas)
