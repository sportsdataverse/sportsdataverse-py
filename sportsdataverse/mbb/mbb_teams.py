from __future__ import annotations

from functools import lru_cache

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore


@lru_cache(maxsize=None)
def espn_mbb_teams(groups=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mbb_teams - look up the men's college basketball teams

    Args:
        groups (int): Used to define different divisions. 50 is Division I, 51 is Division II/Division III.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams for the requested league.
        This function caches by default, so if you want to refresh the data, use the command
        sportsdataverse.mbb.espn_mbb_teams.clear_cache().

    Example:
        Default groups (D1)::

            from sportsdataverse.mbb import espn_mbb_teams
            teams = espn_mbb_teams()
            print(teams.shape)
            print(teams.columns[:8])

        Walk every team-id (handy for batched scrapes)::

            team_ids = teams["team_id"].to_list()
            print(len(team_ids), "D1 teams")

        Pandas round-trip + Division II/III::

            d2_d3 = espn_mbb_teams(groups=51, return_as_pandas=True)
            d2_d3.head()

        See Also:
            * `hoopR`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
    params = {"groups": groups if groups is not None else "50", "limit": 1000}
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
