from __future__ import annotations

import polars as pl

from sportsdataverse import _espn_teams


@_espn_teams.cache_clear_shim
def espn_cfb_teams(groups=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_cfb_teams - look up the college football teams

    Args:
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of the college football teams, one row per team, with the columns in the
        returns table. A payload without teams returns a zero-row frame with those columns. The
        result is not cached: every call fetches (through the package's opt-in TTL response
        cache, ``sportsdataverse.cache``, when it is on).

    Raises:
        NoDataError: ESPN answered 404 for the teams resource.
        AssetFetchError: the fetch failed (403, 429, 5xx, a dropped connection); never returned
            as an empty frame.

    Example:
        Quick start (FBS only by default)::

            from sportsdataverse.cfb import espn_cfb_teams
            teams = espn_cfb_teams()
            print(teams.shape)

        Pull FCS teams (group 81)::

            fcs = espn_cfb_teams(groups=81, return_as_pandas=True)
            fcs.head()

        Pipeline next step (build an abbreviation lookup)::

            teams = espn_cfb_teams()
            abbr_map = dict(zip(teams["team_id"], teams["team_abbreviation"]))

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB team data
            * `recruitR <https://github.com/sportsdataverse/recruitR>`_ -- recruiting data companion
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
    params = {"groups": groups if groups is not None else "80", "limit": 1000}
    return _espn_teams.teams_frame(url, params, return_as_pandas, **kwargs)
