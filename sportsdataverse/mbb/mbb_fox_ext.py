"""Fox Sports "Bifrost" MBB wrappers (``fox_mbb_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/cbk/*``. Thin shims over
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
    "fox_mbb_pbp",
    "fox_mbb_boxscore",
    "fox_mbb_odds",
    "fox_mbb_team_roster",
    "fox_mbb_team_stats",
    "fox_mbb_team_gamelog",
    "fox_mbb_standings",
    "fox_mbb_league_leaders",
    "fox_mbb_teams",
    "fox_mbb_teams_all",
]

_SPORT = "cbk"


@overload
def fox_mbb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_pbp(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB play-by-play (one row per play; period-based).

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

    Example:
        Fetch a game's plays::

            from sportsdataverse.mbb import fox_mbb_pbp
            df = fox_mbb_pbp("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_period_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_boxscore(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB boxscore (long: one row per player-stat).

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

    Example:
        Fetch a game's boxscore in long form::

            from sportsdataverse.mbb import fox_mbb_boxscore
            df = fox_mbb_boxscore("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB game odds six-pack (spread / to-win / total per team).

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

    Example:
        Fetch a game's odds six-pack::

            from sportsdataverse.mbb import fox_mbb_odds
            df = fox_mbb_odds("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB team roster (one row per player).

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

    Example:
        Fetch a team's roster::

            from sportsdataverse.mbb import fox_mbb_team_roster
            df = fox_mbb_team_roster("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB team stat leaders by category.

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

    Example:
        Fetch a team's stat leaders::

            from sportsdataverse.mbb import fox_mbb_team_stats
            df = fox_mbb_team_stats("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB team game log (long: one row per game-stat).

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

    Example:
        Fetch a team's per-game stat log::

            from sportsdataverse.mbb import fox_mbb_team_gamelog
            df = fox_mbb_team_gamelog("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mbb_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB standings for a team's conference/division.

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

    Example:
        Fetch a team's conference standings::

            from sportsdataverse.mbb import fox_mbb_standings
            df = fox_mbb_standings("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_mbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_mbb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB statistical leaders (``stats-con``); who=player|team.

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

    Example:
        Fetch the scoring leaders::

            from sportsdataverse.mbb import fox_mbb_league_leaders
            df = fox_mbb_league_leaders("scoring")
    """
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw


_TEAMS_SCHEMA = {"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "fox_section": pl.Utf8}


def _teams_frame(rows: "list[dict[str, Any]]", return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    df = pl.DataFrame(rows, schema=_TEAMS_SCHEMA) if not rows else pl.DataFrame(rows)
    return df.to_pandas() if return_as_pandas else df


@overload
def fox_mbb_teams(
    team_id: Union[int, str] = ..., *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mbb_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_mbb_teams(
    team_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mbb_teams(
    team_id: Union[int, str] = "150",
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MBB team directory for one seed team's conference.

    College basketball standings are per-conference, so one call returns only
    the seed team's league. Use :func:`fox_mbb_teams_all` for the full
    directory the hoopR MBB team crosswalk consumes.

    Args:
        team_id: Seed Fox Bifrost team id whose standings page is read.
            Defaults to ``"150"``.
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
        Fetch one conference's team directory::

            from sportsdataverse.mbb import fox_mbb_teams
            df = fox_mbb_teams("150")

        See Also:
            * `hoopR`_ - R sister package for men's college basketball
            * `Fox Sports`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Fox Sports: https://www.foxsports.com
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return _teams_frame(parse_teams(raw), return_as_pandas) if return_parsed else raw


def fox_mbb_teams_all(
    max_id: int = 500,
    max_calls: int = 60,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Full MBB team directory by walking seed ids across conferences.

    A single :func:`fox_mbb_teams` call only returns the seed team's
    conference, so this walks candidate team ids (skipping ids already seen in
    an earlier conference) and unions the results, spending at most
    ``max_calls`` standings fetches. Mirrors ``fox_wbb_teams_all``.

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

            from sportsdataverse.mbb import fox_mbb_teams_all
            df = fox_mbb_teams_all()

        Pipeline next step (one line)::

            df.group_by("fox_section").len().sort("len", descending=True).head()

        See Also:
            * `hoopR`_ - R sister package for men's college basketball
            * `Fox Sports`_ - data origin

        .. _hoopR: https://hoopR.sportsdataverse.org
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
