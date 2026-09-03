"""Fox Sports "Bifrost" MLB wrappers (``fox_mlb_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/mlb/*``. Thin shims over
the shared :mod:`sportsdataverse._fox_layout` parsers.

NOTE: unlike the other sports, Fox does not expose MLB play-by-play or boxscore
in ``event/{id}/data`` (it returns only header / top-performers), so there is no
``fox_mlb_pbp`` / ``fox_mlb_boxscore``. The team/league endpoints behave like the
other sports.
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
    parse_league_leaders,
    parse_odds,
    parse_roster,
    parse_standings,
    parse_team_gamelog,
    parse_team_stats,
)

__all__ = [
    "fox_mlb_odds",
    "fox_mlb_team_roster",
    "fox_mlb_team_stats",
    "fox_mlb_team_gamelog",
    "fox_mlb_standings",
    "fox_mlb_league_leaders",
]

_SPORT = "mlb"


@overload
def fox_mlb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mlb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mlb_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB game odds six-pack (run line / to-win / total per team).

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

            from sportsdataverse.mlb import fox_mlb_odds
            df = fox_mlb_odds("...")
    """
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mlb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mlb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mlb_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB team roster (one row per player).

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

            from sportsdataverse.mlb import fox_mlb_team_roster
            df = fox_mlb_team_roster("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mlb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mlb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mlb_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB team stat leaders by category.

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

            from sportsdataverse.mlb import fox_mlb_team_stats
            df = fox_mlb_team_stats("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mlb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mlb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mlb_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB team game log (long: one row per game-stat).

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

            from sportsdataverse.mlb import fox_mlb_team_gamelog
            df = fox_mlb_team_gamelog("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mlb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_mlb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_mlb_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB standings for a team's division/league.

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
        Fetch a team's standings::

            from sportsdataverse.mlb import fox_mlb_standings
            df = fox_mlb_standings("...")
    """
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


@overload
def fox_mlb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_mlb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_mlb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_mlb_league_leaders(
    category: str = "batting",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """MLB statistical leaders (``stats-con``); who=player|team.

    Args:
        category: Stat category. Defaults to ``"batting"``.
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
        Fetch the batting leaders::

            from sportsdataverse.mlb import fox_mlb_league_leaders
            df = fox_mlb_league_leaders("batting")
    """
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw


# The 17 shared Fox league / event / team endpoints, bound to this league's
# sport slug. One table in sportsdataverse._fox_layout drives every league.
__all__ += register_league_endpoints(_SPORT, "mlb", globals())
