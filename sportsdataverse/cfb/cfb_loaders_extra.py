"""Hand-written CFB loaders preserved alongside the generated
`sportsdataverse.cfb.cfb_loaders`: a season-less single-file loader and/or a
teams helper that the season-loop loader template can't express.
"""

from __future__ import annotations

from typing import List  # noqa: F401

import polars as pl

from sportsdataverse.config import (
    CFB_BETTING_LINES_URL,
    CFB_TEAM_LOGO_URL,
)

__all__ = ["load_cfb_betting_lines", "get_cfb_teams"]


def load_cfb_betting_lines(return_as_pandas=False) -> pl.DataFrame:
    """Load college football betting lines information

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing betting lines available for the available seasons.

    Example:
        Quick start::

            from sportsdataverse.cfb import load_cfb_betting_lines
            lines = load_cfb_betting_lines()
            print(lines.shape)

        Pandas round-trip::

            lines_pd = load_cfb_betting_lines(return_as_pandas=True)
            lines_pd.head()

        Pipeline next step (filter to one provider in 2023)::

            import polars as pl
            consensus_2023 = load_cfb_betting_lines().filter(
                (pl.col("season") == 2023) & (pl.col("provider") == "consensus")
            )

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB betting lines
            * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
    """

    return (
        pl.read_parquet(CFB_BETTING_LINES_URL, use_pyarrow=True, columns=None).to_pandas(
            use_pyarrow_extension_array=True,
        )
        if return_as_pandas
        else pl.read_parquet(CFB_BETTING_LINES_URL, use_pyarrow=True, columns=None)
    )


def get_cfb_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load college football team ID information and logos

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available.

    Example:
        Quick start::

            from sportsdataverse.cfb import get_cfb_teams
            teams = get_cfb_teams()
            print(teams.shape)

        Pandas round-trip::

            teams_pd = get_cfb_teams(return_as_pandas=True)
            teams_pd.head()

        Pipeline next step (build a team_id to logo URL map)::

            teams = get_cfb_teams()
            logo_map = dict(zip(teams["team_id"], teams["logo"]))

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB team metadata
    """

    return (
        pl.read_parquet(CFB_TEAM_LOGO_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(CFB_TEAM_LOGO_URL, use_pyarrow=True, columns=None)
    )
