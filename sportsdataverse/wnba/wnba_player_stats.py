"""ESPN WNBA athlete season stats scraper.

Mirror of :func:`sportsdataverse.wbb.espn_wbb_player_stats` for the WNBA
league slug. The actual fetch + parse logic lives in
``sportsdataverse.wbb.wbb_player_stats._espn_basketball_player_stats`` to
keep the wbb / wnba pair DRY.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.wbb.wbb_player_stats import _espn_basketball_player_stats

_LEAGUE_SLUG: str = "wnba"


@overload
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> dict[str, pd.DataFrame]: ...
@overload
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]: ...
def espn_wnba_player_stats(
    athlete_id: int,
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]:
    """Pull ESPN season stats for a WNBA athlete.

    See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full
    documentation of the return shape, the canonical three category keys
    (``"Averages"``, ``"Totals"``, ``"Misc"``), the per-category column
    set, and the ``"Other"`` fallback bucket.

    Args:
        athlete_id: ESPN WNBA athlete identifier (e.g. ``3149391`` for A'ja
            Wilson).
        season: Season year, forwarded to ESPN as ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a dict of pandas DataFrames;
            otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Dict with one DataFrame per stat category — see
        :func:`sportsdataverse.wbb.espn_wbb_player_stats` for the full
        column / key documentation. If ``raw=True``, returns the raw
        response dict.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Pull A'ja Wilson's 2024 season stats and inspect the canonical category keys::

            from sportsdataverse.wnba import espn_wnba_player_stats
            frames = espn_wnba_player_stats(athlete_id=3149391, season=2024)
            sorted(frames.keys())  # at minimum: 'Averages', 'Totals', 'Misc'
            frames["Averages"].head()

        Combine the per-game ``Averages`` and full-season ``Totals``::

            avgs = frames["Averages"]
            totals = frames["Totals"]
            print(avgs.shape, totals.shape)
            avgs.select(["points_per_game", "rebounds_per_game", "assists_per_game"]).head()

        Pandas round-trip — returns a dict of DataFrames keyed by category::

            frames_pd = espn_wnba_player_stats(
                athlete_id=3149391, season=2024, return_as_pandas=True
            )
            frames_pd["Misc"].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _espn_basketball_player_stats(
        league=_LEAGUE_SLUG,
        athlete_id=athlete_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )
