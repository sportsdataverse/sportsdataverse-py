"""ESPN WNBA team season-stats scraper.

Mirror of :func:`sportsdataverse.wbb.espn_wbb_team_stats` for the WNBA
league slug. The actual fetch + parse logic lives in
``sportsdataverse.wbb.wbb_team_stats._espn_basketball_team_stats`` to keep
the wbb / wnba pair DRY.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.wbb.wbb_team_stats import _espn_basketball_team_stats

_LEAGUE_SLUG: str = "wnba"


@overload
def espn_wnba_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wnba_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> dict[str, pd.DataFrame]: ...
@overload
def espn_wnba_team_stats(
    team_id: int,
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> dict[str, pl.DataFrame]: ...
def espn_wnba_team_stats(
    team_id: int,
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]:
    """Pull ESPN team season stats for a WNBA team.

    See :func:`sportsdataverse.wbb.espn_wbb_team_stats` for full
    documentation of the return shape, the canonical three category keys
    (``"Averages"``, ``"Totals"``, ``"Misc"``), the per-category column
    set, and the ``"Other"`` fallback bucket.

    Args:
        team_id: ESPN WNBA team identifier (e.g. ``17`` for the Las Vegas
            Aces).
        season: Season year, forwarded to ESPN as ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a dict of pandas DataFrames;
            otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Dict with one DataFrame per stat category — see
        :func:`sportsdataverse.wbb.espn_wbb_team_stats` for the full
        column / key documentation. If ``raw=True``, returns the raw
        response dict.

    Raises:
        sportsdataverse.errors.NoDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Las Vegas Aces' 2024 team stats — keyed by category::

            from sportsdataverse.wnba import espn_wnba_team_stats
            frames = espn_wnba_team_stats(team_id=17, season=2024)
            sorted(frames.keys())  # 'Averages', 'Totals', 'Misc' (plus optional 'Other')
            frames["Averages"].head()

        Compare per-game and totals at a glance::

            avgs = frames["Averages"]
            totals = frames["Totals"]
            print(avgs.shape, totals.shape)
            avgs.select(["games_played", "points_per_game", "rebounds_per_game"])

        Pandas round-trip::

            frames_pd = espn_wnba_team_stats(team_id=17, season=2024, return_as_pandas=True)
            frames_pd["Misc"].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _espn_basketball_team_stats(
        league=_LEAGUE_SLUG,
        team_id=team_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )
