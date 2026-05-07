from __future__ import annotations

from functools import lru_cache

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


@lru_cache(maxsize=None)
def espn_nba_teams(return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_nba_teams - look up NBA teams

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams for the requested league.
        This function caches by default, so if you want to refresh the data, use the command
        sportsdataverse.nba.espn_nba_teams.clear_cache().

    Example:
        Quick start::

            from sportsdataverse.nba import espn_nba_teams
            teams = espn_nba_teams()
            print(teams.shape)

        Pandas round-trip::

            teams_pd = espn_nba_teams(return_as_pandas=True)
            teams_pd.head()

        Pipeline next step (build a team_id to abbreviation map)::

            teams = espn_nba_teams()
            abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for NBA team data
            * `nba_api <https://github.com/swar/nba_api>`_ -- Python alternative to the NBA Stats API
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
    params = {"limit": 1000}
    resp = download(url=url, params=params, **kwargs)
    if resp is not None:
        events_txt = resp.json()

        teams = events_txt.get("sports")[0].get("leagues")[0].get("teams")
        del_keys = ["record", "links"]
        for team in teams:
            for k in del_keys:
                team.get("team").pop(k, None)
        teams = pd.json_normalize(teams, sep="_")
    teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams if return_as_pandas else pl.from_pandas(teams)
