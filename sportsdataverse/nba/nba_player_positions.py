"""Listed player positions (numeric 1-5) for the BPM 2.0 position blend."""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

from sportsdataverse.nba.nba_stats import nba_stats_playerindex

_BASE: dict[str, float] = {"PG": 1.0, "SG": 2.0, "SF": 3.0, "PF": 4.0, "C": 5.0, "G": 1.5, "F": 3.5}


def _position_to_num(position: str) -> float:
    """Map a listed position string to the BPM 1-5 scale (missing/unknown -> 3.0).

    Hyphenated positions (``G-F``) map to the mean of their parts.

    Args:
        position: Position string from playerindex, e.g. ``"PG"``, ``"G-F"``, ``"F-C"``.

    Returns:
        Numeric position on the 1-5 scale; 3.0 for unknown or empty.
    """
    if not position:
        return 3.0
    parts = [p.strip().upper() for p in position.replace("/", "-").split("-")]
    vals = [_BASE[p] for p in parts if p in _BASE]
    return sum(vals) / len(vals) if vals else 3.0


def nba_player_positions(
    season: str,
    *,
    league_id: str = "00",
    fetch: Optional[Callable[..., pl.DataFrame]] = None,
) -> pl.DataFrame:
    """Fetch league-wide listed positions for a season as numeric 1-5.

    Args:
        season: NBA season, e.g. ``"2023-24"``.
        league_id: LeagueID (``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League).
        fetch: Injectable ``nba_stats_playerindex`` replacement for offline tests.

    Returns:
        Frame with columns ``player_id:Int64, position_num:Float64``.

    Raises:
        ImportError: If ``curl_cffi`` is not installed (required by the stats runtime).

    Example:
        Listed positions for a season (residential IP)::

            from sportsdataverse.nba import nba_player_positions
            pos = nba_player_positions("2023-24")
            print(pos.head())

        Offline / injectable fetch for testing::

            import polars as pl
            stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
            pos = nba_player_positions("2023-24", fetch=stub)

        See Also:
            * `hoopR`_ -- R companion package for NBA/MBB data
            * `nba_api`_ -- Python NBA stats API client

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    get = fetch if fetch is not None else nba_stats_playerindex
    raw: pl.DataFrame = get(season=season, league_id=league_id)
    id_col = "person_id" if "person_id" in raw.columns else "player_id"
    return (
        raw.select(
            pl.col(id_col).cast(pl.Int64).alias("player_id"),
            pl.col("position").map_elements(_position_to_num, return_dtype=pl.Float64).alias("position_num"),
        )
        # One row per player_id (the documented grain). The playerindex lists a
        # mid-season-traded player once per team, so without this a traded
        # player's duplicate player_id fans out through the position join in
        # nba_bpm/nba_spm. Listed position is a player attribute (identical
        # across a player's team rows), so keeping the first is deterministic
        # and lossless.
        .unique(subset=["player_id"], keep="first", maintain_order=True)
    )
