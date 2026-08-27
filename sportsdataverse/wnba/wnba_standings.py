"""ESPN WNBA standings scraper.

Mirror of :func:`sportsdataverse.wbb.espn_wbb_standings` for the WNBA
league slug. The actual fetch + parse logic lives in
``sportsdataverse.wbb.wbb_standings._espn_basketball_standings`` to keep
the wbb / wnba pair DRY.

Unlike the WBB endpoint, the WNBA standings call doesn't take a ``group``
filter — the league has a single division, so the helper is invoked with
``group=None``.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.wbb.wbb_standings import _espn_basketball_standings

_LEAGUE_SLUG: str = "wnba"


@overload
def espn_wnba_standings(
    season: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wnba_standings(
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wnba_standings(
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wnba_standings(
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN WNBA standings for a season.

    See :func:`sportsdataverse.wbb.espn_wbb_standings` for full
    documentation of the column set. The WNBA endpoint does not take a
    ``group`` filter.

    Args:
        season: Season year, forwarded to ESPN as ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with one row per team — see
        :func:`sportsdataverse.wbb.espn_wbb_standings` for the full
        column list. If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Pull WNBA standings for a season::

            from sportsdataverse.wnba import espn_wnba_standings
            standings = espn_wnba_standings(season=2024)
            print(standings.shape)
            standings.head()

        Sort by win percentage::

            import polars as pl
            standings.sort("win_percent", descending=True).select(
                ["team_display_name", "wins", "losses", "win_percent"]
            ).head(8)

        Pandas round-trip::

            standings_pd = espn_wnba_standings(season=2024, return_as_pandas=True)
            standings_pd[["team_display_name", "wins", "losses"]].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _espn_basketball_standings(
        league=_LEAGUE_SLUG,
        season=season,
        group=None,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )
