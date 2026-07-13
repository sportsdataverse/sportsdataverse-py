"""ESPN WNBA game officials scraper.

Mirror of :func:`sportsdataverse.wbb.espn_wbb_game_officials` for the WNBA
league slug. The actual fetch + parse logic lives in
``sportsdataverse.wbb.wbb_game_officials._espn_basketball_game_officials``
to keep the wbb / wnba pair DRY.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.wbb.wbb_game_officials import _espn_basketball_game_officials

_LEAGUE_SLUG: str = "wnba"


@overload
def espn_wnba_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wnba_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wnba_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wnba_game_officials(
    game_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull the officials assigned to a WNBA game.

    See :func:`sportsdataverse.wbb.espn_wbb_game_officials` for full
    documentation of the column set, the empty-frame fallback when ESPN
    ships no officials, and the ``raw`` / ``return_as_pandas`` flag
    semantics.

    Args:
        game_id: ESPN WNBA event identifier (e.g. ``401620238`` for Game 1
            of the 2024 WNBA Finals).
        season: Season year (recorded as output column only).
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with the same columns documented in
        :func:`sportsdataverse.wbb.espn_wbb_game_officials`. If
        ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after retries.

    Example:
        Pull officials for the 2024 WNBA Finals Game 1::

            from sportsdataverse.wnba import espn_wnba_game_officials
            refs = espn_wnba_game_officials(game_id=401620238, season=2024)
            print(refs.shape)
            refs.select(["full_name", "position_name", "order"]).head()

        Pandas round-trip::

            refs_pd = espn_wnba_game_officials(
                game_id=401620238, season=2024, return_as_pandas=True
            )
            refs_pd[["full_name", "position_name"]].head()

        Inspect the raw ESPN payload (e.g. for fields not flattened)::

            payload = espn_wnba_game_officials(game_id=401620238, season=2024, raw=True)
            list(payload.keys())[:8]

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _espn_basketball_game_officials(
        league=_LEAGUE_SLUG,
        game_id=game_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def helper_wnba_officials(payload: dict, *, season: int, game_id: int | str) -> pl.DataFrame:
    """Parse one game's officials sidecar into the released officials frame.

    Faithful polars port of the script-local parsers in
    ``wehoop-wnba-data/R/espn_wnba_10_officials_creation.R`` -- byte-identical
    to the WBB parser after league normalization, so this delegates to the
    shared implementation (it consumes the stored
    ``wnba/officials/json/{game_id}.json`` sidecar, not the live endpoint
    wrapped above). The R-released ``espn_wnba_officials`` parquet is the
    parity oracle.

    Args:
        payload: One game's ``wnba/officials/json/{game_id}.json`` as a dict.
        season: Season year the sidecar belongs to.
        game_id: ESPN game id the sidecar belongs to (released dtype String).

    Returns:
        pl.DataFrame: One row per official; empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_officials
            payload = json.load(open("401736126.json", encoding="utf-8"))
            df = helper_wnba_officials(payload, season=2025, game_id=401736126)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_game_officials import helper_wbb_officials

    return helper_wbb_officials(payload, season=season, game_id=game_id)
