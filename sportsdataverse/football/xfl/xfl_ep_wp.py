"""XFL EP/WP/CP -- by-reference shim over the shared spring-football port."""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.football.spring_football_ep_wp import build_spring_football_pbp, enrich_spring_football_pbp
from sportsdataverse.football.xfl.xfl_espn_ext import espn_xfl_summary

if TYPE_CHECKING:
    import pandas as pd


def xfl_pbp(
    game_id: Union[str, int],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Enriched XFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

    Fetches the ESPN game summary, unrolls its drives into an nflverse-shape
    frame, and scores it with the same parity-validated NFL EP/WP pipeline
    used league-wide (see
    :mod:`sportsdataverse.football.spring_football_ep_wp`).

    Args:
        game_id: ESPN XFL event id.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        One row per play with ``ep``/``epa``/``wp``/``wpa``/``cp``/``cpoe``
        and the other ``enrich_nfl_pbp`` output columns. Zero rows for a game
        ESPN has no play-by-play for.

    Example:
        Quick start::

            from sportsdataverse.football.xfl import xfl_pbp

            df = xfl_pbp("401517780")
            print(df.select("play_id", "epa", "wp").head())

        See Also:
            * `nflfastR`_ -- the R package whose EP/WP models this ports.

        .. _nflfastR: https://www.nflfastr.com
    """
    summary = espn_xfl_summary(game_id, return_parsed=False)
    pbp = build_spring_football_pbp(summary, league="xfl")
    return enrich_spring_football_pbp(pbp, league="xfl", return_as_pandas=return_as_pandas)
