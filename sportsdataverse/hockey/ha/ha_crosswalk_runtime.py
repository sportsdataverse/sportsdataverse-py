"""HockeyAllsvenskan player ids are SHL isa ids: one StatNet id space, so the crosswalk is an alias.

``player_statnet_id`` (play-by-play ``player.statNetId``, leaderboard ``playerStatNetId``) is the
StatNet player id, the same number the SHL calls the **isa** id (``player_id`` in every SHL frame);
:func:`ha_player_crosswalk` joins on it. No matching by name is done, so a player who never played
in the requested SHL season simply gets nulls.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.hockey.shl.shl_runtime import shl_player_crosswalk

__all__ = ["ha_player_crosswalk"]

Frame = Union[pl.DataFrame, pd.DataFrame]
_KEEP = ("athlete_uuid", "full_name", "date_of_birth")


def ha_player_crosswalk(
    players: Union[pl.DataFrame, Iterable[Any]],
    season: Optional[int] = None,
    game_type: str = "regular",
    *,
    shl_crosswalk: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Frame:
    """HockeyAllsvenskan ``player_statnet_id`` -> the SHL isa id and athlete uuid of the same player.

    The HA and the SHL share the StatNet player id, so ``shl_player_id`` always equals
    ``player_statnet_id`` where the SHL has the player; the lookup adds the SHL athlete uuid and
    confirms the player appears in that SHL season.

    Args:
        players: an HA frame with a ``player_statnet_id`` column (leaderboard, play-by-play, player
            directory), or an iterable of StatNet player ids.
        season: SHL season END year (2026 = 2025/26) to look the players up in; required unless
            ``shl_crosswalk`` is given.
        game_type: SHL game type: regular, playoff, qualdown or playin.
        shl_crosswalk: an already fetched :func:`~sportsdataverse.hockey.shl.shl_player_crosswalk`
            frame, to skip the request.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per distinct player id: ``player_statnet_id``
        and ``shl_player_id`` (``Int64``; null when the SHL season has no such player),
        ``athlete_uuid``, ``full_name`` and ``date_of_birth`` from the SHL.

    Raises:
        ValueError: neither ``season`` nor ``shl_crosswalk`` was given, or a frame has no
            ``player_statnet_id`` column.
        sportsdataverse.errors.NoDataError: the SHL has no such season (from the SHL lookup).
        sportsdataverse.errors.AssetFetchError: the SHL fetch failed.

    Example:
        Quick start::

            from sportsdataverse.hockey.ha import ha_leaders, ha_player_crosswalk
            ha_player_crosswalk(ha_leaders(), season=2026)
    """
    if isinstance(players, pd.DataFrame):
        players = pl.from_pandas(players)
    if isinstance(players, pl.DataFrame):
        if "player_statnet_id" not in players.columns:
            raise ValueError("ha_player_crosswalk: the frame needs a player_statnet_id column")
        ids = players["player_statnet_id"]
    else:
        ids = pl.Series("player_statnet_id", list(players))
    ids = ids.cast(pl.Int64).drop_nulls().unique(maintain_order=True)
    if shl_crosswalk is None:
        if season is None:
            raise ValueError("ha_player_crosswalk: give season (the SHL season to look in) or shl_crosswalk")
        shl_crosswalk = shl_player_crosswalk(season, game_type)
    shl = shl_crosswalk.unique("player_id").select(
        pl.col("player_id").alias("player_statnet_id"), pl.col("player_id").alias("shl_player_id"), *_KEEP
    )
    if shl.schema["player_statnet_id"] != ids.dtype:  # join keys must agree: both Int64
        raise ValueError(
            f"ha_player_crosswalk: SHL player_id is {shl.schema['player_statnet_id']}, expected {ids.dtype}"
        )
    out = ids.to_frame().join(shl, on="player_statnet_id", how="left")
    return out.to_pandas() if return_as_pandas else out
