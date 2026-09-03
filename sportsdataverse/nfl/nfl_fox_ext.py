"""Fox Sports "Bifrost" NFL wrappers (``fox_nfl_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/nfl/*``. Thin shims over
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
    parse_drive_pbp,
    parse_roster,
    parse_standings,
    parse_team_gamelog,
    parse_team_stats,
)

__all__ = [
    "fox_nfl_pbp",
    "fox_nfl_boxscore",
    "fox_nfl_odds",
    "fox_nfl_team_roster",
    "fox_nfl_team_stats",
    "fox_nfl_team_gamelog",
    "fox_nfl_standings",
    "fox_nfl_league_leaders",
]

_SPORT = "nfl"


@overload
def fox_nfl_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_pbp(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL play-by-play (one row per play; drive-based).

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
        Fetch a game's plays as a polars frame::

            from sportsdataverse.nfl import fox_nfl_pbp
            df = fox_nfl_pbp("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_drive_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_boxscore(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL boxscore (long: one row per player-stat).

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

            from sportsdataverse.nfl import fox_nfl_boxscore
            df = fox_nfl_boxscore("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL game odds six-pack (spread / to-win / total per team).

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

            from sportsdataverse.nfl import fox_nfl_odds
            df = fox_nfl_odds("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL team roster (one row per player).

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

            from sportsdataverse.nfl import fox_nfl_team_roster
            df = fox_nfl_team_roster("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL team stat leaders by category.

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

            from sportsdataverse.nfl import fox_nfl_team_stats
            df = fox_nfl_team_stats("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL team game log (long: one row per game-stat).

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

            from sportsdataverse.nfl import fox_nfl_team_gamelog
            df = fox_nfl_team_gamelog("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_nfl_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_nfl_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL standings for a team's conference/division.

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
        Fetch a team's conference/division standings::

            from sportsdataverse.nfl import fox_nfl_standings
            df = fox_nfl_standings("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_nfl_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_nfl_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_nfl_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_nfl_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """NFL statistical leaders (``stats-con``); who=player|team.

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

            from sportsdataverse.nfl import fox_nfl_league_leaders
            df = fox_nfl_league_leaders("scoring")
    """
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw


# The 17 shared Fox league / event / team endpoints, bound to this league's
# sport slug. One table in sportsdataverse._fox_layout drives every league.
__all__ += register_league_endpoints(_SPORT, "nfl", globals())
