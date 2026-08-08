"""Fox Sports "Bifrost" WBB wrappers (``fox_wbb_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/wcbk/*``. Thin shims over
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
from sportsdataverse.errors import NoESPNDataError

__all__ = [
    "fox_wbb_pbp",
    "fox_wbb_boxscore",
    "fox_wbb_odds",
    "fox_wbb_team_roster",
    "fox_wbb_team_stats",
    "fox_wbb_team_gamelog",
    "fox_wbb_standings",
    "fox_wbb_league_leaders",
    "fox_wbb_teams",
    "fox_wbb_teams_all",
]

_SPORT = "wcbk"


@overload
def fox_wbb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_pbp(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB play-by-play (one row per play; period-based).

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's plays::

            from sportsdataverse.wbb import fox_wbb_pbp
            df = fox_wbb_pbp("389046")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_period_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_boxscore(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB boxscore (long: one row per player-stat).

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's boxscore in long form::

            from sportsdataverse.wbb import fox_wbb_boxscore
            df = fox_wbb_boxscore("389046")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB game odds six-pack (spread / to-win / total per team).

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a game's odds six-pack::

            from sportsdataverse.wbb import fox_wbb_odds
            df = fox_wbb_odds("389046")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB team roster (one row per player).

    Args:
        team_id: Fox Bifrost team id.
        return_parsed: If ``True`` (default) flatten the position-group tables to
            a DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's roster::

            from sportsdataverse.wbb import fox_wbb_team_roster
            df = fox_wbb_team_roster("11")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB team stat leaders by category.

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's stat leaders::

            from sportsdataverse.wbb import fox_wbb_team_stats
            df = fox_wbb_team_stats("11")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB team game log (long: one row per game-stat).

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's per-game stat log::

            from sportsdataverse.wbb import fox_wbb_team_gamelog
            df = fox_wbb_team_gamelog("11")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_wbb_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB standings for a team's conference/division.

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch a team's conference standings::

            from sportsdataverse.wbb import fox_wbb_standings
            df = fox_wbb_standings("11")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_wbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_wbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_wbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB statistical leaders (``stats-con``); who=player|team.

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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch the scoring leaders::

            from sportsdataverse.wbb import fox_wbb_league_leaders
            df = fox_wbb_league_leaders("scoring")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw


_TEAMS_SCHEMA = {"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "fox_section": pl.Utf8}


def _teams_frame(rows: "list[dict[str, Any]]", return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    # Schema on EVERY path: parse_teams can emit None for fox_team_name /
    # fox_section, and an all-null column would otherwise infer Null while the
    # empty path infers Utf8 -- an unstable schema for crosswalk consumers.
    df = pl.DataFrame(rows, schema=_TEAMS_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


@overload
def fox_wbb_teams(
    team_id: Union[int, str] = ..., *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_wbb_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_wbb_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_wbb_teams(
    team_id: Union[int, str] = "11",
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """WBB team directory (``fox_team_id`` / ``fox_team_name`` / ``fox_section``).

    Derived from the seed team's standings endpoint, so a single call only
    covers that team's conference — see :func:`fox_wbb_teams_all` for the full
    directory. This is the frame the wehoop WBB team crosswalk consumes.

    Args:
        team_id: Seed Fox Bifrost team id whose conference standings are read.
            Defaults to ``"11"`` (UConn, Big East).
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
        sportsdataverse.errors.NoESPNDataError: Fox returned 404 for the requested id.
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Fetch the seed conference's team directory::

            from sportsdataverse.wbb import fox_wbb_teams
            df = fox_wbb_teams("11")

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return _teams_frame(parse_teams(raw), return_as_pandas) if return_parsed else raw


def fox_wbb_teams_all(
    max_id: int = 500,
    max_calls: int = 60,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Full WBB team directory by walking seed ids across conferences.

    A single :func:`fox_wbb_teams` call only returns the seed team's
    conference, so this walks candidate team ids (skipping ids already seen in
    an earlier conference) and unions the results, spending at most
    ``max_calls`` standings fetches. Mirrors R ``wehoop::fox_wbb_teams_all()``.

    Args:
        max_id: Highest candidate team id to try. Defaults to ``500``.
        max_calls: Budget of standings fetches. Defaults to ``60``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default) or pandas DataFrame, one row per team:
        ``fox_team_id`` / ``fox_team_name`` / ``fox_section``.

    Raises:
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries. A 404 on an individual
            candidate id (``NoESPNDataError``) is expected during the scan and is
            skipped; every other failure propagates rather than silently
            truncating the directory.

    Example:
        Build the full directory::

            from sportsdataverse.wbb import fox_wbb_teams_all
            df = fox_wbb_teams_all()

        See Also:
            * `wehoop`_ - R sister package for women's college basketball
            * `sportsdataverse.wbb ESPN wrappers`_ - the ESPN-sourced alternative
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _sportsdataverse.wbb ESPN wrappers: https://py.sportsdataverse.org/docs/wbb
        .. _Fox Sports: https://www.foxsports.com
    """
    seen: "set[str]" = set()
    rows: "list[dict[str, Any]]" = []
    calls = 0
    for cand in range(1, max_id + 1):
        if calls >= max_calls:
            break
        if str(cand) in seen:
            continue
        try:
            part = parse_teams(fox_get(f"{_SPORT}/team/{cand}/standings", **kwargs))
        except NoESPNDataError:
            # Only "this candidate id does not exist" is expected while scanning.
            # Transport / auth / rate-limit failures must NOT be laundered into an
            # empty directory the caller can't tell apart from a valid scan.
            part = []
        calls += 1
        for r in part:
            if r["fox_team_id"] not in seen:
                seen.add(r["fox_team_id"])
                rows.append(r)
    return _teams_frame(rows, return_as_pandas)
