"""ESPN NBA game officials -- season-builder release producer.

Unlike WBB/WNBA (a live ``.../officials`` core-api endpoint backed by its own
``wbb/officials/json/{id}.json`` / ``wnba/officials/json/{id}.json`` raw
sidecar), NBA officials are NOT scraped as their own dataset --
``hoopR-nba-raw`` has no ``nba/officials/`` directory at all. They are
projected out of the game_rosters per-game summary payload's
``gameInfo.officials[]`` array, per
``hoopR-nba-data/R/espn_nba_10_officials_creation.R``.

Faithful polars port of that script's inline ``purrr::map_dfr`` (the R source
has no ``parse_one_official()`` function -- it inlines the map; it also
carries a dead, unused ``parse_one_athlete()`` copy-pasted from script 09,
which this port does not reproduce). Column count (7) and ``game_id`` dtype
(Int32, via R's ``safe_int`` -- NOT the WBB/WNBA ``as.character`` -> String
convention) both diverge from the WBB/WNBA officials release, so this does
NOT delegate to ``helper_wbb_officials``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.wbb.wbb_game_rosters import _rel_chr, _rel_int

__all__ = ["helper_nba_officials"]

_OFFICIAL_COLS: tuple[str, ...] = (
    "season",
    "game_id",
    "official_full_name",
    "official_display_name",
    "official_position",
    "official_position_id",
    "official_order",
)


def helper_nba_officials(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's ``gameInfo.officials[]`` into the released officials frame.

    Faithful polars port of the inline officials map in
    ``hoopR-nba-data/R/espn_nba_10_officials_creation.R``. The raw source is
    the SAME per-game sidecar ``nba/game_rosters/json/{game_id}.json`` that
    backs :func:`sportsdataverse.nba.helper_nba_game_rosters` -- there is no
    dedicated ``nba/officials/`` raw directory. ``game_id`` is Int32 here (the
    R script casts with ``safe_int``, not ``as.character``) -- a genuine
    divergence from the WBB/WNBA officials release, which keeps ``game_id``
    as String.

    Args:
        payload: One game's ``nba/game_rosters/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype Int32).

    Returns:
        pl.DataFrame: One row per official; empty (zero-column) frame when
        the payload carries no ``gameInfo.officials``.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_officials
            payload = json.load(open("401766128.json", encoding="utf-8"))
            df = helper_nba_officials(payload, season=2025, game_id=401766128)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    officials = (payload.get("gameInfo") or {}).get("officials") or []
    if not officials:
        return pl.DataFrame()
    rows: list[dict[str, Any]] = []
    for off in officials:
        pos = off.get("position") or {}
        rows.append(
            {
                "season": int(season),
                "game_id": _rel_int(game_id),
                "official_full_name": _rel_chr(off.get("fullName")),
                "official_display_name": _rel_chr(off.get("displayName")) or _rel_chr(off.get("fullName")),
                "official_position": _rel_chr(pos.get("name")) or _rel_chr(pos.get("displayName")),
                "official_position_id": _rel_int(pos.get("id")),
                "official_order": _rel_int(off.get("order")),
            }
        )
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in _OFFICIAL_COLS}, strict=False)
    int32_cols = ("season", "game_id", "official_position_id", "official_order")
    str_cols = [c for c in _OFFICIAL_COLS if c not in int32_cols]
    df = df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in int32_cols] + [pl.col(c).cast(pl.Utf8) for c in str_cols]
    )
    return df.unique(maintain_order=True, keep="first")
