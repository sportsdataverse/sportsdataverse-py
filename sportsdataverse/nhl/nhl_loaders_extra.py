"""Hand-written NHL loaders preserved alongside the generated
`sportsdataverse.nhl.nhl_loaders`: a season-less single-file loader and/or a
teams helper that the season-loop loader template can't express.
"""

from __future__ import annotations

from typing import List  # noqa: F401

import polars as pl

from sportsdataverse.config import (
    NHL_TEAM_LOGO_URL,
)

__all__ = ["nhl_teams"]


def nhl_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load NHL team ID information and logos

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available for the requested seasons.

    Example:
        Pull the static teams + logos table::

            from sportsdataverse.nhl import nhl_teams
            teams = nhl_teams()
            print(teams.shape)
            teams.head()

        Pandas round-trip — convenient for joining against your own roster table::

            teams_pd = nhl_teams(return_as_pandas=True)
            list(teams_pd.columns)[:10]

        See Also:
            * `fastRhockey`_ — R companion package; mirrors this surface
            * `nhl-api-py`_ — alternative Python source for the NHL stats API

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
        .. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
    """
    return pl.read_csv(NHL_TEAM_LOGO_URL).to_pandas if return_as_pandas else pl.read_csv(NHL_TEAM_LOGO_URL)
