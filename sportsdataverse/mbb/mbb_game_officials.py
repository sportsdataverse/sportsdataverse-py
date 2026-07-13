"""ESPN MBB game officials -- season-builder release producer.

Unlike WBB/WNBA (a live ``.../officials`` core-api endpoint backed by its own
``wbb/officials/json/{id}.json`` / ``wnba/officials/json/{id}.json`` raw
sidecar), MBB officials are NOT scraped as their own dataset --
``hoopR-mbb-raw`` has no ``mbb/officials/`` directory at all. They are
projected out of the game_rosters per-game summary payload's
``gameInfo.officials[]`` array, per
``hoopR-mbb-data/R/espn_mbb_10_officials_creation.R`` -- the SAME shape (7
columns, ``game_id`` Int32 via ``safe_int``) as
``hoopR-nba-data/R/espn_nba_10_officials_creation.R``. Officials is a
league-neutral dataset shared between MBB and NBA (both college and pro use
the identical inline ``purrr::map_dfr`` parser reading only ``gameInfo.
officials``), so this module re-exports the NBA implementation rather than
forking a second copy.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_game_officials import helper_nba_officials

__all__ = ["helper_mbb_officials"]


def helper_mbb_officials(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's ``gameInfo.officials[]`` into the released officials frame.

    Faithful polars port of the inline officials map in
    ``hoopR-mbb-data/R/espn_mbb_10_officials_creation.R`` -- byte-identical to
    the NBA parser after league normalization, so this delegates to the
    shared implementation. The raw source is the SAME per-game sidecar
    ``mbb/game_rosters/json/{game_id}.json`` that backs
    :func:`sportsdataverse.mbb.helper_mbb_game_rosters` -- there is no
    dedicated ``mbb/officials/`` raw directory. ``game_id`` is Int32 here (the
    R script casts with ``safe_int``, not ``as.character``) -- a genuine
    divergence from the WBB/WNBA officials release, which keeps ``game_id``
    as String.

    Args:
        payload: One game's ``mbb/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype Int32).

    Returns:
        pl.DataFrame: One row per official; empty (zero-column) frame when
        the payload carries no ``gameInfo.officials``.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_officials
            payload = json.load(open("401746082.json", encoding="utf-8"))
            df = helper_mbb_officials(payload, season=2025, game_id=401746082)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_nba_officials(payload, season=season, game_id=game_id)
