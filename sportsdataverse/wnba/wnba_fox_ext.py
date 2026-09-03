"""Fox Sports "Bifrost" WNBA wrappers (``fox_wnba_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/wnba/*``. Thin shims over
the shared :mod:`sportsdataverse._fox_layout` parsers. Same
``return_parsed`` / ``return_as_pandas`` contract as the ESPN ext (polars by
default; raw ``Dict`` when ``return_parsed=False``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Literal, Union, overload

if TYPE_CHECKING:
    import pandas as pd

import polars as pl

from sportsdataverse._fox_layout import (
    fox_get,
    register_league_endpoints,
    frame,
    parse_boxscore,
    parse_league_leaders,
    parse_odds,
    parse_period_pbp,
    parse_roster,
    parse_standings,
    parse_team_gamelog,
    parse_team_stats,
    parse_teams,
)

__all__ = [
    "fox_wnba_pbp",
    "fox_wnba_boxscore",
    "fox_wnba_odds",
    "fox_wnba_team_roster",
    "fox_wnba_team_stats",
    "fox_wnba_team_gamelog",
    "fox_wnba_standings",
    "fox_wnba_league_leaders",
    "fox_wnba_teams",
]

_SPORT = "wnba"


@overload
def fox_wnba_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA play-by-play (one row per play; period-based).

    Args:
        game_id: Fox Bifrost event id.
        return_parsed: If ``True`` (default) flatten the pbp layout to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's plays as a polars frame::

            from sportsdataverse.wnba import fox_wnba_pbp
            df = fox_wnba_pbp("2278")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_period_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA boxscore (long: one row per player-stat).

    Args:
        game_id: Fox Bifrost event id.
        return_parsed: If ``True`` (default) flatten the per-team stat tables to
            long form; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's boxscore in long form::

            from sportsdataverse.wnba import fox_wnba_boxscore
            df = fox_wnba_boxscore("2278")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_odds(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA game odds six-pack (spread / to-win / total per team).

    Args:
        game_id: Fox Bifrost event id.
        return_parsed: If ``True`` (default) flatten the six-pack market to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's odds six-pack::

            from sportsdataverse.wnba import fox_wnba_odds
            df = fox_wnba_odds("2278")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA team roster (one row per player).

    Args:
        team_id: Fox Bifrost team id.
        return_parsed: If ``True`` (default) flatten the roster tables to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's roster::

            from sportsdataverse.wnba import fox_wnba_team_roster
            df = fox_wnba_team_roster("3")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA team stat leaders by category.

    Args:
        team_id: Fox Bifrost team id.
        return_parsed: If ``True`` (default) flatten the leader sections to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's stat leaders::

            from sportsdataverse.wnba import fox_wnba_team_stats
            df = fox_wnba_team_stats("3")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA team game log (long: one row per game-stat).

    Args:
        team_id: Fox Bifrost team id.
        return_parsed: If ``True`` (default) flatten to long form; if ``False``
            return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's per-game stat log::

            from sportsdataverse.wnba import fox_wnba_team_gamelog
            df = fox_wnba_team_gamelog("3")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wnba_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_standings(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA standings for a team's conference/division.

    Args:
        team_id: Fox Bifrost team id.
        return_parsed: If ``True`` (default) flatten the standings tables to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's conference standings::

            from sportsdataverse.wnba import fox_wnba_standings
            df = fox_wnba_standings("3")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wnba_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_wnba_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_wnba_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA statistical leaders (``stats-con``); who=player|team.

    Args:
        category: Stat category. Defaults to ``"scoring"``.
        who: ``"player"`` or ``"team"``. Defaults to ``"player"``.
        page: 0-based result page. Defaults to ``0``.
        return_parsed: If ``True`` (default) flatten the leader tables to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch the scoring leaders::

            from sportsdataverse.wnba import fox_wnba_league_leaders
            df = fox_wnba_league_leaders("scoring")

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw


_TEAMS_SCHEMA = {"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "fox_section": pl.Utf8}


@overload
def fox_wnba_teams(
    team_id: Union[int, str] = ..., *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wnba_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_wnba_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wnba_teams(
    team_id: Union[int, str] = "3",
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WNBA team directory (``fox_team_id`` / ``fox_team_name`` / ``fox_section``).

    Derived from the standings endpoint (one league-wide payload), this is the
    frame the wehoop WNBA team crosswalk consumes.

    Args:
        team_id: Seed Fox Bifrost team id whose standings page is read.
            Defaults to ``"3"`` — any WNBA team id returns the whole league.
        return_parsed: If ``True`` (default) flatten the standings to the team
            directory; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch the league team directory::

            from sportsdataverse.wnba import fox_wnba_teams
            df = fox_wnba_teams()

        See Also:
            * `wehoop`_ - R sister package for the WNBA
            * `nba_api`_ - Python alternative (stats.wnba.com)
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    if not return_parsed:
        return raw
    rows = parse_teams(raw)
    # Schema on EVERY path: parse_teams can emit None for fox_team_name /
    # fox_section, and an all-null column would otherwise infer Null while the
    # empty path infers Utf8 -- an unstable schema for crosswalk consumers.
    df = pl.DataFrame(rows, schema=_TEAMS_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


# The 17 shared Fox league / event / team endpoints, bound to this league's
# sport slug. One table in sportsdataverse._fox_layout drives every league.
__all__ += register_league_endpoints(_SPORT, "wnba", globals())
