"""Fox Sports "Bifrost" college-football wrappers (hand-written ext).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/cfb/*``. The Bifrost API
is a layout API (sections -> tables -> rows -> cells); these functions flatten
it into tidy frames. Reverse-engineering notes + an OpenAPI spec live in the
sdv-internal-refs repo (``_notes/foxsportsapi/``). Vertical slice: pbp, roster,
boxscore. The public apikey ships in the foxsports.com web bundle.

Mirrors the codegen ESPN-ext contract (``return_parsed`` / ``return_as_pandas``;
polars by default, raw ``Dict`` when ``return_parsed=False``). When the Fox YAML
is added to ``tools/codegen``, this module can be regenerated.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Sequence, Union, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._codegen_runtime import _get
from sportsdataverse._fox_layout import DATA_KEY as FOX_DATA_KEY  # single source of truth for the public Fox key

logger = logging.getLogger(__name__)

__all__ = [
    "fox_cfb_teams",
    "fox_cfb_schedule",
    "fox_cfb_pbp",
    "fox_cfb_team_roster",
    "fox_cfb_boxscore",
    "fox_cfb_standings",
    "fox_cfb_team_stats",
    "fox_cfb_team_gamelog",
    "fox_cfb_league_leaders",
    "fox_cfb_odds",
]

FOX_BASE = "https://api.foxsports.com/bifrost/v1"
# FOX_DATA_KEY (the public foxsports.com data-tier key, env-overridable via
# SDV_PY_FOX_DATA_KEY) is imported from sportsdataverse._fox_layout above so the
# bundled default and its env override live in exactly one place.

_HEADERS = {"Origin": "https://www.foxsports.com", "Referer": "https://www.foxsports.com/"}


def _fox_get(path: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    merged = {"apikey": FOX_DATA_KEY, "api-version": "1.1"}
    if params:
        merged.update(params)
    return _get(f"{FOX_BASE}/{path}", params=merged, headers=_HEADERS, **kwargs)


def _clean(name: Any) -> str:
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _table_rows(tbl: Optional[dict], extra: Optional[dict] = None) -> List[Dict]:
    """A Bifrost table {headers, rows} -> list of wide dict rows."""
    extra = extra or {}
    if not tbl:
        return []
    headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
    names = [_clean(h) if h not in (None, "") else f"v{i}" for i, h in enumerate(headers)]
    out: List[Dict] = []
    for r in tbl.get("rows", []) or []:
        cells = _cells(r.get("columns"))
        row = dict(extra)
        for name, val in zip(names, cells):
            row[name] = val
        row["entity_id"] = _uri_id((r.get("entityLink") or {}).get("contentUri"))
        out.append(row)
    return out


def _cells(columns: Any) -> List[Optional[str]]:
    return [c.get("text") if isinstance(c, dict) else c for c in (columns or [])]


def _uri_id(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    m = re.search(r"(\d+)$", uri)
    return m.group(1) if m else None


# Stable column sets -- what each parser always emits, so an empty response still
# carries a usable schema. Captured from live responses 2026-09-02, not guessed.
# Fox serialises every value as a string, hence Utf8 throughout.
_TEAMS_COLUMNS: Sequence[str] = (
    "fox_team_id",
    "abbreviation",
    "name",
    "slug",
    "color",
    "logo_url",
)
_SCHEDULE_COLUMNS: Sequence[str] = (
    "game_id",
    "date",
    "status",
    "week_label",
    "home_team",
    "home_team_id",
    "away_team",
    "away_team_id",
    "segment_id",
)
_PBP_COLUMNS: Sequence[str] = (
    "game_id",
    "quarter",
    "drive_id",
    "drive_result",
    "drive_summary",
    "drive_team",
    "play_id",
    "period",
    "clock",
    "field_position",
    "play_text",
    "play_team",
)
_TEAM_ROSTER_COLUMNS: Sequence[str] = (
    "team_id",
    "position_group",
    "player",
    "pos",
    "cls",
    "ht",
    "wt",
    "athlete_id",
)
_BOXSCORE_COLUMNS: Sequence[str] = (
    "game_id",
    "team",
    "stat_group",
    "player",
    "athlete_id",
    "stat",
    "value",
)
_STANDINGS_COLUMNS: Sequence[str] = (
    "team_id",
    "section",
    "entity_id",
)
_TEAM_STATS_COLUMNS: Sequence[str] = (
    "team_id",
    "category",
    "stat",
    "stat_abbreviation",
    "player",
    "value",
)
_TEAM_GAMELOG_COLUMNS: Sequence[str] = (
    "team_id",
    "season_type",
    "category",
    "game_id",
    "game_date",
    "opponent",
    "stat",
    "value",
)
_ODDS_COLUMNS: Sequence[str] = (
    "game_id",
    "team",
    "spread",
    "to_win",
    "total",
)
# league_leaders is deliberately absent: every one of its columns is response-shaped
# (the stat category becomes the columns), so there is no stable set to declare.


def _frame(
    rows: List[Dict[str, Any]],
    return_as_pandas: bool,
    columns: Optional[Sequence[str]] = None,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Rows -> frame, carrying the documented schema when there are no rows.

    A bare ``pl.DataFrame([])`` is zero-COLUMN, not just zero-row, which makes an
    upstream-empty response indistinguishable from a broken parser: both surface
    as a frame with nothing in it, and a caller checking ``df.columns`` learns
    nothing. Fox returns empty payloads routinely -- a team's gamelog in week 1,
    a category with no qualifiers -- so this is the normal path, not the edge.

    ``columns`` is the stable key set the parser always emits. Populated frames
    may be wider (``standings`` and ``league_leaders`` pivot response headers
    into columns, so their shape depends on the conference or stat requested);
    the guarantee is only that these keys are always present.
    """
    if return_as_pandas:
        import pandas as pd

        return pd.DataFrame(rows, columns=list(columns) if not rows and columns else None)
    if not rows and columns:
        return pl.DataFrame(schema={c: pl.Utf8 for c in columns})
    return pl.DataFrame(rows)


@overload
def fox_cfb_teams(*, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any) -> Dict[str, Any]: ...
@overload
def fox_cfb_teams(
    *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_teams(
    *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[False] = ..., **kwargs: Any
) -> pl.DataFrame: ...
def fox_cfb_teams(
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB team directory (one row per team).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/league/teamnav``

    The team-nav payload is the canonical Fox directory: it maps every team's
    Bifrost id to its abbreviation, full name, and web slug. This is the lookup
    you need to translate a human team name into the numeric ``team_id`` the
    other ``fox_cfb_*`` wrappers expect, and it is the Fox side of
    :func:`sportsdataverse.cfb.cfb_teams_crosswalk`.

    Args:
        return_parsed: If ``True`` (default) flatten the nav items to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default) with columns ``fox_team_id``,
        ``abbreviation``, ``name``, ``slug``, ``color``, ``logo_url``; a pandas
        DataFrame when ``return_as_pandas=True``; or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Example:
        Build an abbreviation -> Fox id lookup::

            from sportsdataverse.cfb import fox_cfb_teams
            teams = fox_cfb_teams()
            fox_id = dict(zip(teams["abbreviation"], teams["fox_team_id"]))
    """
    raw = _fox_get("cfb/league/teamnav", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for it in raw.get("navItems", []) or []:
        link = it.get("entityLink") or {}
        m = re.search(r"teams/(\d+)", link.get("contentUri") or "")
        if not m:  # skip non-team nav entries (conference headers, etc.)
            continue
        slug = re.sub(r"-team$", "", re.sub(r"^/college-football/", "", link.get("webUrl") or "")) or None
        rows.append(
            {
                "fox_team_id": m.group(1),
                "abbreviation": it.get("title"),
                "name": link.get("title"),
                "slug": slug,
                "color": link.get("color"),
                "logo_url": it.get("logoUrl"),
            }
        )
    return _frame(rows, return_as_pandas, _TEAMS_COLUMNS)


def _fox_team_fullname(team: Optional[dict]) -> Optional[str]:
    """A Fox scoreboard team block -> ``"Location Nickname"`` full name."""
    if not team:
        return None
    stacked = f"{team.get('stackedNameTop') or ''} {team.get('stackedNameBottom') or ''}".strip()
    return stacked or team.get("longName") or team.get("name")


def _parse_fox_segment(raw: Dict[str, Any], segment_id: str) -> List[Dict[str, Any]]:
    """A Fox ``scores-segment`` payload -> one tidy row per game."""
    rows: List[Dict[str, Any]] = []
    for sec in raw.get("sectionList", []) or []:
        week_label = sec.get("title")
        for ev in sec.get("events", []) or []:
            tokens = ((ev.get("entityLink") or {}).get("layout") or {}).get("tokens") or {}
            home_uri, away_uri = tokens.get("homeUri"), tokens.get("awayUri")
            upper, lower = ev.get("upperTeam") or {}, ev.get("lowerTeam") or {}
            by_uri = {t.get("uri"): t for t in (upper, lower) if t.get("uri")}
            # explicit home/away uri wins; else the US convention (away on top).
            home_team = by_uri.get(home_uri) if home_uri else lower
            away_team = by_uri.get(away_uri) if away_uri else upper
            rows.append(
                {
                    "game_id": tokens.get("id") or _uri_id(ev.get("contentUri")),
                    "date": ev.get("eventTime"),
                    "status": ev.get("statusLine"),
                    "week_label": week_label,
                    "home_team": _fox_team_fullname(home_team),
                    "home_team_id": _uri_id(home_uri),
                    "away_team": _fox_team_fullname(away_team),
                    "away_team_id": _uri_id(away_uri),
                    "segment_id": segment_id,
                }
            )
    return rows


def _fox_segment_ids(season: int, group_id: Union[int, str], **kwargs: Any) -> List[str]:
    """Every Fox scoreboard segment id for a season (regular weeks + postseason).

    Enumerated from the live ``selectionGroupList`` for both the requested group
    and the ``cfp`` group, with each id's season prefix remapped to ``season`` so
    historical seasons resolve too (the CFB segment scheme — ``{season}-{week}-1``
    for regular weeks, ``{season}-bowls-2`` / ``{season}-cfp-2`` for the
    postseason — is stable year to year).
    """
    mains = [_fox_get("cfb/scoreboard/main", params={"groupId": group_id}, **kwargs)]
    try:
        # The CFP lives under its own group; it is supplemental (the bowls segment
        # already carries CFP games), so a failure here must not abort enumeration.
        mains.append(_fox_get("cfb/scoreboard/main", params={"groupId": "cfp"}, **kwargs))
    except Exception as exc:
        logger.warning("Fox CFP group enumeration failed for %s (continuing without it): %s", season, exc)
    ids: List[str] = []
    seen: set[str] = set()
    for main in mains:
        for grp in main.get("selectionGroupList") or []:
            for sel in grp.get("selectionList") or []:
                sid = sel.get("id")
                if not sid:
                    continue
                head, _, tail = sid.partition("-")
                remapped = f"{season}-{tail}" if tail and head.isdigit() else sid
                if remapped not in seen:
                    seen.add(remapped)
                    ids.append(remapped)
    return ids


@overload
def fox_cfb_schedule(
    season: Optional[int] = ...,
    *,
    segment_id: Optional[str] = ...,
    group_id: Union[int, str] = ...,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_cfb_schedule(
    season: Optional[int] = ...,
    *,
    segment_id: Optional[str] = ...,
    group_id: Union[int, str] = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_cfb_schedule(
    season: Optional[int] = ...,
    *,
    segment_id: Optional[str] = ...,
    group_id: Union[int, str] = ...,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_schedule(
    season: Optional[int] = None,
    *,
    segment_id: Optional[str] = None,
    group_id: Union[int, str] = "2",
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB full-season schedule (one row per game).

    Fox lists games behind a two-step *selector -> segment* flow: ``scoreboard/main``
    enumerates the season's segments (its ``selectionGroupList``), and
    ``league/scores-segment/{segmentId}`` returns the games for one segment.
    Pass a ``season`` to scrape the **whole season** -- every regular week plus
    conference championships, bowls, and every College Football Playoff round --
    enumerated from the live selector and unioned, deduplicated by ``game_id``.

    Segment ids encode the phase, not an ESPN-style integer week:
    ``"{season}-{week}-1"`` for a regular-season week, ``"{season}-bowls-2"`` for
    the bowls, ``"{season}-cfp-2"`` for the CFP (conference championships fall in
    the final regular-season week). Pass ``segment_id`` to fetch just one of them.

    The numeric ``game_id`` is the Fox Bifrost event id that :func:`fox_cfb_pbp` /
    :func:`fox_cfb_odds` accept; ``week_label`` is the section title.

    Args:
        season: Season year -> scrape the full season. Ignored when ``segment_id``
            is given; if both are ``None`` the current segment is returned.
        segment_id: Explicit Fox segment id (e.g. ``"2025-5-1"``, ``"2025-cfp-2"``)
            -> fetch just that segment.
        group_id: Conference/division group filter. Defaults to ``"2"`` (FBS).
        return_parsed: If ``True`` (default) flatten to a DataFrame; if ``False``
            return the raw JSON (a single segment's ``dict``, or a
            ``{segment_id: dict}`` map in full-season mode).
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default) with columns ``game_id``, ``date``,
        ``status``, ``week_label``, ``home_team``, ``home_team_id``,
        ``away_team``, ``away_team_id``, ``segment_id``; a pandas DataFrame when
        ``return_as_pandas=True``; or raw JSON when ``return_parsed=False``.

    Example:
        Scrape a whole season (regular + conf championships + bowls + CFP)::

            from sportsdataverse.cfb import fox_cfb_schedule
            season = fox_cfb_schedule(2025)

        Fetch just one segment (a week, or the playoff)::

            wk5 = fox_cfb_schedule(segment_id="2025-5-1")
            cfp = fox_cfb_schedule(segment_id="2025-cfp-2")
    """
    if segment_id is not None:
        seg_ids = [segment_id]
    elif season is not None:
        seg_ids = _fox_segment_ids(season, group_id, **kwargs)  # full season
    else:
        main = _fox_get("cfb/scoreboard/main", params={"groupId": group_id}, **kwargs)
        cur = main.get("currentSelectionId")
        seg_ids = [cur] if cur else []

    raws: Dict[str, Any] = {}
    for sid in seg_ids:
        raws[sid] = _fox_get(f"cfb/league/scores-segment/{sid}", params={"groupId": group_id}, **kwargs)
    if not return_parsed:
        if not raws:
            return {}
        return next(iter(raws.values())) if len(raws) == 1 else raws

    rows: List[Dict[str, Any]] = []
    seen_games: set[Any] = set()
    for sid, raw in raws.items():
        for row in _parse_fox_segment(raw, sid):
            gid = row.get("game_id")
            if gid is not None and gid in seen_games:  # dedup overlap (bowls vs cfp)
                continue
            if gid is not None:
                seen_games.add(gid)
            rows.append(row)
    return _frame(rows, return_as_pandas, _SCHEDULE_COLUMNS)


@overload
def fox_cfb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_pbp(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_pbp(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB play-by-play (one row per play).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data``

    Args:
        game_id: Fox Bifrost event id (e.g. ``"41616"``) -- not the ESPN id.
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

            from sportsdataverse.cfb import fox_cfb_pbp
            df = fox_cfb_pbp("41616")
    """
    raw = _fox_get(f"cfb/event/{game_id}/data", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in (raw.get("pbp", {}) or {}).get("sections", []) or []:
        quarter = sec.get("title")
        for drv in sec.get("groups", []) or []:
            for p in drv.get("plays", []) or []:
                rows.append(
                    {
                        "game_id": str(game_id),
                        "quarter": quarter,
                        "drive_id": drv.get("id"),
                        "drive_result": drv.get("title"),
                        "drive_summary": drv.get("subtitle"),
                        "drive_team": (drv.get("entityLink") or {}).get("title"),
                        "play_id": p.get("id"),
                        "period": p.get("periodOfPlay"),
                        "clock": p.get("timeOfPlay"),
                        "field_position": p.get("title"),
                        "play_text": p.get("playDescription"),
                        "play_team": (p.get("entityLink") or {}).get("title"),
                    }
                )
    return _frame(rows, return_as_pandas, _PBP_COLUMNS)


@overload
def fox_cfb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_team_roster(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_team_roster(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB team roster (one row per player).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/roster``

    Args:
        team_id: Fox Bifrost team id (e.g. ``"11"`` = Miami (FL)); discover via
            the league team directory (``cfb/league/teamnav``).
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

            from sportsdataverse.cfb import fox_cfb_team_roster
            df = fox_cfb_team_roster("11")
    """
    raw = _fox_get(f"cfb/team/{team_id}/roster", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for g in raw.get("groups", []) or []:
        headers = _cells((g.get("headers") or [{}])[0].get("columns"))
        group_label = g.get("title") or (headers[0] if headers else None)
        col_names = ["player"] + [str(h).lower() for h in headers[1:]]
        for r in g.get("rows", []) or []:
            uri = (r.get("entityLink") or {}).get("contentUri")
            if not uri or "athletes/" not in uri:  # players only
                continue
            cells = _cells(r.get("columns"))
            row = {"team_id": str(team_id), "position_group": group_label}
            for name, val in zip(col_names, cells):
                row[name] = val
            row["athlete_id"] = _uri_id(uri)
            rows.append(row)
    return _frame(rows, return_as_pandas, _TEAM_ROSTER_COLUMNS)


@overload
def fox_cfb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_boxscore(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_boxscore(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB boxscore (long: one row per player-stat).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/data``
    (the ``boxscore`` block).

    Args:
        game_id: Fox Bifrost event id (e.g. ``"41616"``).
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

            from sportsdataverse.cfb import fox_cfb_boxscore
            df = fox_cfb_boxscore("41616")
    """
    raw = _fox_get(f"cfb/event/{game_id}/data", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in (raw.get("boxscore", {}) or {}).get("boxscoreSections", []) or []:
        team = sec.get("title")
        for item in sec.get("boxscoreItems", []) or []:
            tbl = item.get("boxscoreTable")
            if not tbl:
                continue
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            stat_group = headers[0] if headers else None
            stat_names = [re.sub(r"\W+", "_", str(h)).strip("_").lower() for h in headers[1:]]
            for r in tbl.get("rows", []) or []:
                cells = _cells(r.get("columns"))
                player = cells[0] if cells else None
                aid = _uri_id((r.get("entityLink") or {}).get("contentUri"))
                for name, val in zip(stat_names, cells[1:]):
                    rows.append(
                        {
                            "game_id": str(game_id),
                            "team": team,
                            "stat_group": stat_group,
                            "player": player,
                            "athlete_id": aid,
                            "stat": name,
                            "value": val,
                        }
                    )
    return _frame(rows, return_as_pandas, _BOXSCORE_COLUMNS)


@overload
def fox_cfb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_standings(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_standings(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_standings(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB conference standings for a team's conference.

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/standings``
    (the league-wide ``league/standings`` endpoint returns header-only tables, so
    standings are keyed by team).

    Args:
        team_id: Fox Bifrost team id (e.g. ``"11"`` = Miami (FL)).
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

            from sportsdataverse.cfb import fox_cfb_standings
            df = fox_cfb_standings("11")
    """
    raw = _fox_get(f"cfb/team/{team_id}/standings", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in raw.get("standingsSections", []) or []:
        for tbl in sec.get("standings", []) or []:  # a list of tables per section
            rows += _table_rows(tbl, extra={"team_id": str(team_id), "section": sec.get("title")})
    return _frame(rows, return_as_pandas, _STANDINGS_COLUMNS)


@overload
def fox_cfb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_team_stats(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_team_stats(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB team stat leaders (one row per category leader).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/stats``

    Args:
        team_id: Fox Bifrost team id (e.g. ``"11"`` = Miami (FL)).
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

            from sportsdataverse.cfb import fox_cfb_team_stats
            df = fox_cfb_team_stats("11")
    """
    raw = _fox_get(f"cfb/team/{team_id}/stats", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in raw.get("leadersSections", []) or []:
        for ld in sec.get("leaders", []) or []:
            rows.append(
                {
                    "team_id": str(team_id),
                    "category": sec.get("title"),
                    "stat": ld.get("title"),
                    "stat_abbreviation": ld.get("statAbbreviation"),
                    "player": ld.get("name"),
                    "value": ld.get("statValue"),
                }
            )
    return _frame(rows, return_as_pandas, _TEAM_STATS_COLUMNS)


@overload
def fox_cfb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_team_gamelog(
    team_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB team game log -- tidy long: one row per (game, stat).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/team/{team_id}/gamelog``
    The endpoint groups team per-game stats by category (passing, rushing,
    defense, ...) and season-type split; this flattens to columns
    ``team_id, season_type, category, game_id, game_date, opponent, stat, value``.

    Args:
        team_id: Fox Bifrost team id (e.g. ``"11"`` = Miami (FL)).
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

            from sportsdataverse.cfb import fox_cfb_team_gamelog
            df = fox_cfb_team_gamelog("11")
    """
    raw = _fox_get(f"cfb/team/{team_id}/gamelog", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        category = sec.get("id")
        for tbl in sec.get("tables", []) or []:
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            season_type = headers[0] if headers else None  # first header = split label
            raw_stats = headers[2:]  # skip date + opponent columns
            stat_names: List[str] = []
            seen: Dict[str, int] = {}
            for h in raw_stats:  # dedupe repeated names (e.g. two "YDS")
                base = _clean(h)
                seen[base] = seen.get(base, 0) + 1
                stat_names.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
            for r in tbl.get("rows", []) or []:
                cells = _cells(r.get("columns"))
                gid = _uri_id((r.get("entityLink") or {}).get("contentUri"))
                game_date = cells[0] if len(cells) > 0 else None
                opponent = cells[1] if len(cells) > 1 else None
                for name, val in zip(stat_names, cells[2:]):
                    rows.append(
                        {
                            "team_id": str(team_id),
                            "season_type": season_type,
                            "category": category,
                            "game_id": gid,
                            "game_date": game_date,
                            "opponent": opponent,
                            "stat": name,
                            "value": val,
                        }
                    )
    return _frame(rows, return_as_pandas, _TEAM_GAMELOG_COLUMNS)


@overload
def fox_cfb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    group_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[False],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> Dict[str, Any]: ...
@overload
def fox_cfb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    group_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> "pd.DataFrame": ...
@overload
def fox_cfb_league_leaders(
    category: str = ...,
    who: str = ...,
    page: int = ...,
    group_id: Union[int, str] = ...,
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_league_leaders(
    category: str = "passing",
    who: str = "player",
    page: int = 0,
    group_id: Union[int, str] = "2",
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB statistical leaders (one row per player/team).

    Endpoint: ``GET .../bifrost/v1/cfb/league/stats-con/{who}/{category}/{page}``

    Args:
        category: Stat category -- passing, rushing, receiving, defense, kicking,
            returning, scoring, yardage (team adds downs, turnovers). Defaults to
            ``"passing"``.
        who: ``"player"`` or ``"team"``. Defaults to ``"player"``.
        page: 0-based result page. Defaults to ``0``.
        group_id: Conference/group filter. Defaults to ``"2"``.
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
        Fetch the passing leaders::

            from sportsdataverse.cfb import fox_cfb_league_leaders
            df = fox_cfb_league_leaders("passing")
    """
    raw = _fox_get(
        f"cfb/league/stats-con/{who}/{category}/{page}",
        params={"groupId": group_id},
        **kwargs,
    )
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        rows += _table_rows(sec.get("table"))
    return _frame(rows, return_as_pandas)


@overload
def fox_cfb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[False], return_as_pandas: bool = ..., **kwargs: Any
) -> Dict[str, Any]: ...
@overload
def fox_cfb_odds(
    game_id: Union[int, str], *, return_parsed: Literal[True] = ..., return_as_pandas: Literal[True], **kwargs: Any
) -> "pd.DataFrame": ...
@overload
def fox_cfb_odds(
    game_id: Union[int, str],
    *,
    return_parsed: Literal[True] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def fox_cfb_odds(
    game_id: Union[int, str],
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]:
    """Fox Sports CFB game odds six-pack (spread / to win / total per team).

    Endpoint: ``GET https://api.foxsports.com/bifrost/v1/cfb/event/{game_id}/odds``

    Args:
        game_id: Fox Bifrost event id (e.g. ``"41616"``).
        return_parsed: If ``True`` (default) flatten the six-pack market to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default; empty when no market is posted), a pandas
        DataFrame when ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Example:
        Fetch a game's odds six-pack::

            from sportsdataverse.cfb import fox_cfb_odds
            df = fox_cfb_odds("41616")
    """
    raw = _fox_get(f"cfb/event/{game_id}/odds", **kwargs)
    if not return_parsed:
        return raw
    rows: List[Dict] = []
    odds = (raw.get("sixPack") or {}).get("odds")
    if odds:
        names = [_clean(c) for c in _cells(odds.get("columnHeaders"))]
        for r in odds.get("rows", []) or []:
            row = {"game_id": str(game_id), "team": r.get("fullText") or r.get("text")}
            for name, v in zip(names, r.get("values", []) or []):
                row[name] = (v or {}).get("odds")
            rows.append(row)
    return _frame(rows, return_as_pandas, _ODDS_COLUMNS)
