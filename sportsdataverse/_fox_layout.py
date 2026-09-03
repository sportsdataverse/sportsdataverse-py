"""Shared parsing layer for Fox Sports "Bifrost" wrappers (``fox_<sport>_*``).

The Bifrost API is a layout API (sections -> tables -> rows -> cells) that is
uniform across sports; only the ``{sport}`` slug and the play-by-play shape
differ. This module centralizes the HTTP call, the generic table flattener, and
the per-shape parsers so each league module (``cfb``/``nba``/``mbb``/``nhl``/
``mlb``) stays a thin set of public ``fox_<sport>_*`` wrappers.

pbp shapes:
  - period-based (nba/mbb/nhl): ``pbp.sections[0].groups[]`` are periods
    (QUARTER/HALF/PERIOD) each with ``plays[]``.
  - drive-based (cfb): handled in ``sportsdataverse.cfb.cfb_fox_ext``.
Reverse-engineering notes + an OpenAPI 3.1 spec live in the ``sdv-internal-refs``
repo.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._codegen_runtime import _get

API = "https://api.foxsports.com/bifrost/v1"
# Public data-tier key shipped in the foxsports.com web bundle. Overridable via
# the SDV_PY_FOX_DATA_KEY env var so a key rotation does not require a release.
DATA_KEY = os.getenv("SDV_PY_FOX_DATA_KEY", "jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq")
_HEADERS = {"Origin": "https://www.foxsports.com", "Referer": "https://www.foxsports.com/"}


def fox_get(path: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a Bifrost path with the public data-tier key + api-version.

    Args:
        path: Bifrost path under ``/bifrost/v1`` (e.g. ``"cbk/team/11/roster"``).
        params: Extra query params merged on top of ``apikey`` / ``api-version``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        The parsed JSON response body as a ``dict``.
    """
    merged = {"apikey": DATA_KEY, "api-version": "1.1"}
    if params:
        merged.update(params)
    return _get(f"{API}/{path}", params=merged, headers=_HEADERS, **kwargs)


def frame(rows: List[Dict[str, Any]], return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Materialize parsed rows as a polars (default) or pandas DataFrame.

    Args:
        rows: Flattened row dicts produced by a ``parse_*`` function.
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A ``polars.DataFrame`` (default) or ``pandas.DataFrame``.
    """
    if return_as_pandas:
        import pandas as pd

        return pd.DataFrame(rows)
    return pl.DataFrame(rows)


def _cells(columns: Any) -> List[Optional[str]]:
    return [c.get("text") if isinstance(c, dict) else c for c in (columns or [])]


def _uri_id(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    m = re.search(r"(\d+)$", uri)
    return m.group(1) if m else None


def _clean(name: Any) -> str:
    return re.sub(r"\W+", "_", str(name)).strip("_").lower() or "v"


def _table_rows(tbl: Optional[dict], extra: Optional[dict] = None) -> List[Dict]:
    """A Bifrost table ``{headers, rows}`` -> list of wide dict rows."""
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


# ---- entity/league parsers (generic across sports) ------------------------
def parse_roster(raw: Dict, team_id: Union[int, str]) -> List[Dict]:
    """team/{id}/roster groups -> one row per player (athletes only)."""
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
    return rows


def parse_team_stats(raw: Dict, team_id: Union[int, str]) -> List[Dict]:
    """team/{id}/stats leadersSections -> one row per category stat leader."""
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
    return rows


def parse_team_gamelog(raw: Dict, team_id: Union[int, str]) -> List[Dict]:
    """sectionList -> tables; long: one row per (game, category, stat)."""
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        category = sec.get("id")
        for tbl in sec.get("tables", []) or []:
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            season_type = headers[0] if headers else None
            stat_names: List[str] = []
            seen: Dict[str, int] = {}
            for h in headers[2:]:  # skip date + opponent columns
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
    return rows


def parse_standings(raw: Dict, team_id: Optional[Union[int, str]] = None) -> List[Dict]:
    """team/{id}/standings (or league/standings): standingsSections[].standings
    is a *list* of tables."""
    rows: List[Dict] = []
    for sec in raw.get("standingsSections", []) or []:
        extra = {"section": sec.get("title")}
        if team_id is not None:
            extra = {"team_id": str(team_id), **extra}
        for tbl in sec.get("standings", []) or []:
            rows += _table_rows(tbl, extra=extra)
    return rows


def _title_case(name: str) -> str:
    """Title case matching R ``stringr::str_to_title`` on team names.

    ``str.title()`` capitalizes each *alphabetic run*, treating ``&``/``-``/
    ``(``/``)`` as word boundaries the same way R's ICU-backed
    ``str_to_title`` does (``A&T`` -> ``A&T``, ``(OH)`` -> ``(Oh)``,
    ``MARYLAND-EASTERN`` -> ``Maryland-Eastern``) -- but it also
    (re-)capitalizes the letter right after an apostrophe (``ST. JOHN'S`` ->
    ``St. John'S``), which R does not. Undo just that one artifact.
    """
    return re.sub(r"(?<=[A-Za-z]')[A-Z]", lambda m: m.group(0).lower(), name.lower().title())


def parse_teams(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """team/{id}/standings -> team directory, the shape the R basketball crosswalks consume.

    ``fox_section`` prefers the conference name from
    ``standingsSections[].metadata.parameters.groupName`` (e.g. ``"Big East"``),
    falling back to ``pageTitle`` then the generic section ``title``.
    ``fox_team_name`` comes from the row's ``entityLink.title`` (full team name,
    title-cased), falling back to the second table cell (short name). Rows are
    de-duplicated on ``fox_team_id`` keeping the first occurrence.

    Args:
        raw: Decoded ``team/{id}/standings`` Bifrost payload (as returned by
            :func:`fox_get`). A missing / empty ``standingsSections`` yields ``[]``.

    Returns:
        One dict per team with keys ``fox_team_id`` (``str``), ``fox_team_name``
        (``str`` or ``None``) and ``fox_section`` (``str`` or ``None``). Feed it to
        :func:`frame` (or the league wrappers' ``_teams_frame``) for a DataFrame.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising --
        transport failures surface from :func:`fox_get`, not from this parser.

    Example:
        Parse a seed team's conference directory::

            from sportsdataverse._fox_layout import fox_get, parse_teams
            rows = parse_teams(fox_get("wcbk/team/11/standings"))
            rows[0]

        Or go straight to the public wrappers::

            from sportsdataverse.wbb import fox_wbb_teams
            df = fox_wbb_teams("11")

        See Also:
            * `wehoop`_ - R sister package whose basketball crosswalk consumes this frame
            * `hoopR`_ - R sister package for men's basketball
            * `Fox Sports`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Fox Sports: https://www.foxsports.com
    """
    rows: List[Dict[str, Any]] = []
    seen: set = set()
    for sec in raw.get("standingsSections", []) or []:
        params = (sec.get("metadata") or {}).get("parameters") or {}
        group = params.get("groupName") if isinstance(params, dict) else None
        section = (
            group.strip()
            if isinstance(group, str) and group.strip()
            else (sec.get("pageTitle") or "").strip() or sec.get("title")
        )
        for tbl in sec.get("standings", []) or []:
            for r in tbl.get("rows", []) or []:
                link = r.get("entityLink") or {}
                eid = _uri_id(link.get("contentUri"))
                if eid is None or eid in seen:
                    continue
                seen.add(eid)
                title = (link.get("title") or "").strip()
                cells = _cells(r.get("columns"))
                name = _title_case(title) if title else (cells[1] if len(cells) > 1 else None)
                rows.append({"fox_team_id": eid, "fox_team_name": name, "fox_section": section})
    return rows


def parse_league_leaders(raw: Dict) -> List[Dict]:
    """league/stats-con sectionList tables -> one row per ranked entity."""
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        rows += _table_rows(sec.get("table"))
    return rows


def parse_odds(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/odds sixPack -> one row per team (spread/to-win/total)."""
    rows: List[Dict] = []
    odds = (raw.get("sixPack") or {}).get("odds")
    if odds:
        names = [_clean(c) for c in _cells(odds.get("columnHeaders"))]
        for r in odds.get("rows", []) or []:
            row = {"game_id": str(game_id), "team": r.get("fullText") or r.get("text")}
            for name, v in zip(names, r.get("values", []) or []):
                row[name] = (v or {}).get("odds")
            rows.append(row)
    return rows


def parse_boxscore(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/data boxscore -> long (one row per player-stat). Sections with
    no boxscoreItems (e.g. the "MATCHUP" summary in nba/nhl) are skipped."""
    rows: List[Dict] = []
    for sec in (raw.get("boxscore", {}) or {}).get("boxscoreSections", []) or []:
        team = sec.get("title")
        for item in sec.get("boxscoreItems", []) or []:
            tbl = item.get("boxscoreTable")
            if not tbl:
                continue
            headers = _cells((tbl.get("headers") or [{}])[0].get("columns"))
            stat_group = headers[0] if headers else None
            stat_names = [_clean(h) for h in headers[1:]]
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
    return rows


def parse_period_pbp(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/data pbp for period sports (nba/mbb/nhl): one row per play.

    Structure: ``pbp.sections[0].groups[]`` are periods (1ST QUARTER / 1ST HALF /
    1ST PERIOD) each with ``plays[]``.
    """
    rows: List[Dict] = []
    for sec in (raw.get("pbp", {}) or {}).get("sections", []) or []:
        for grp in sec.get("groups", []) or []:
            period = grp.get("title")
            left, right = grp.get("leftTeamAbbr"), grp.get("rightTeamAbbr")
            for p in grp.get("plays", []) or []:
                rows.append(
                    {
                        "game_id": str(game_id),
                        "period": period,
                        "left_team": left,
                        "right_team": right,
                        "play_id": p.get("id"),
                        "clock": p.get("timeOfPlay"),
                        "team": (p.get("entityLink") or {}).get("title") or p.get("imageAltText"),
                        "left_score_change": p.get("leftTeamScoreChange"),
                        "right_score_change": p.get("rightTeamScoreChange"),
                        "play_text": p.get("playDescription"),
                    }
                )
    return rows


def parse_drive_pbp(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/data pbp for drive sports (cfb/nfl): one row per play.

    Structure: ``pbp.sections[]`` are quarters, each ``groups[]`` entry is a
    drive carrying ``plays[]``.

    Args:
        raw: Decoded ``{sport}/event/{id}/data`` payload.
        game_id: Fox Bifrost event id, stamped on every row as ``str``.

    Returns:
        One dict per play with drive context (``drive_id``, ``drive_result``,
        ``drive_summary``, ``drive_team``) plus the play fields. A payload with
        no ``pbp`` section yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Flatten a Fox NFL game's drive-based play-by-play::

            from sportsdataverse._fox_layout import fox_get, parse_drive_pbp
            rows = parse_drive_pbp(fox_get("nfl/event/1234/data"), 1234)
    """
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
    return rows


def parse_selection_nav(raw: Dict) -> List[Dict]:
    """scoreboard/main + league/scores + league/schedule -> one row per nav selection.

    Those three endpoints are *navigation* payloads: they enumerate the weeks
    (``groupList``), dates (``dailyList``) and season segments
    (``selectionGroupList[].selectionList``) whose ids feed
    ``league/scores-segment/{segmentId}``.

    Args:
        raw: Decoded ``{sport}/scoreboard/main``, ``{sport}/league/scores`` or
            ``{sport}/league/schedule`` payload.

    Returns:
        One dict per selection with ``selection_list`` (which of the three lists
        it came from), ``id``, ``title``, ``date``, ``uri``, ``web_url``,
        ``selected`` and ``group_id``. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Enumerate a league's scoreboard segments::

            from sportsdataverse._fox_layout import fox_get, parse_selection_nav
            rows = parse_selection_nav(fox_get("cfb/scoreboard/main"))
    """
    rows: List[Dict] = []

    def _row(list_name: str, item: Dict[str, Any]) -> Dict[str, Any]:
        params = item.get("parameters") or {}
        return {
            "selection_list": list_name,
            "id": item.get("id"),
            "title": item.get("title") or item.get("name"),
            "date": item.get("date"),
            "uri": item.get("uri"),
            "web_url": item.get("webUrl"),
            "selected": item.get("selected"),
            "group_id": params.get("groupId") if isinstance(params, dict) else None,
        }

    for list_name in ("groupList", "dailyList"):
        for item in raw.get(list_name, []) or []:
            rows.append(_row(list_name, item))
    for grp in raw.get("selectionGroupList", []) or []:
        for item in grp.get("selectionList", []) or []:
            rows.append(_row("selectionList", item))
    return rows


def _full_team_name(team: Optional[dict]) -> Optional[str]:
    """A Fox scoreboard team block -> ``"Location Nickname"`` full name."""
    if not team:
        return None
    stacked = f"{team.get('stackedNameTop') or ''} {team.get('stackedNameBottom') or ''}".strip()
    return stacked or team.get("longName") or team.get("name")


def parse_segment_events(raw: Dict, segment_id: Optional[Union[int, str]] = None) -> List[Dict]:
    """league/scores-segment + topevents/scoreboard/segment -> one row per game.

    Events live either directly under ``sectionList[].events`` (scores-segment,
    topevents segment) or under ``sectionList[].modules[].model.events`` (the
    ``featured-events`` module of ``explore/odds/main``); both are scanned.

    Args:
        raw: Decoded segment payload.
        segment_id: Optional segment id stamped on every row as ``str``.

    Returns:
        One dict per game -- ``game_id`` / ``home_team_id`` / ``away_team_id``
        are the numeric Bifrost ids as ``str``. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Flatten one CFB scoreboard segment::

            from sportsdataverse._fox_layout import fox_get, parse_segment_events
            raw = fox_get("cfb/league/scores-segment/2025-1-1")
            rows = parse_segment_events(raw, "2025-1-1")
    """
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        events: List[Dict[str, Any]] = list(sec.get("events") or [])
        for mod in sec.get("modules", []) or []:
            events += list((mod.get("model") or {}).get("events") or [])
        for ev in events:
            tokens = ((ev.get("entityLink") or {}).get("layout") or {}).get("tokens") or {}
            home_uri, away_uri = tokens.get("homeUri"), tokens.get("awayUri")
            upper, lower = ev.get("upperTeam") or {}, ev.get("lowerTeam") or {}
            by_uri = {t.get("uri"): t for t in (upper, lower) if t.get("uri")}
            # explicit home/away uri wins; else the US convention (away on top).
            home = by_uri.get(home_uri) if home_uri else lower
            away = by_uri.get(away_uri) if away_uri else upper
            rows.append(
                {
                    "segment_id": None if segment_id is None else str(segment_id),
                    "section_id": sec.get("id"),
                    "section_title": sec.get("title"),
                    "game_id": tokens.get("id") or _uri_id(ev.get("contentUri")),
                    "chip_id": ev.get("id"),
                    "league": ev.get("league"),
                    "date": ev.get("eventTime"),
                    "event_status": ev.get("eventStatus"),
                    "status": ev.get("statusLine"),
                    "tv_station": ev.get("tvStation"),
                    "headline": ev.get("eventHeadline"),
                    "odds_line": ev.get("oddsLine"),
                    "over_under_line": ev.get("overUnderLine"),
                    "home_team": _full_team_name(home),
                    "home_team_id": _uri_id(home_uri) or _uri_id((home or {}).get("uri")),
                    "home_score": (home or {}).get("score"),
                    "home_record": (home or {}).get("record"),
                    "away_team": _full_team_name(away),
                    "away_team_id": _uri_id(away_uri) or _uri_id((away or {}).get("uri")),
                    "away_score": (away or {}).get("score"),
                    "away_record": (away or {}).get("record"),
                }
            )
    return rows


def parse_nav_items(raw: Dict) -> List[Dict]:
    """league/teamnav + league/conferences + explore/browse -> one row per entity.

    All three ship the same nav-item shape; ``teamnav`` puts them in a flat
    ``navItems`` list while ``conferences`` / ``explore/browse`` bucket them into
    ``groups[].items``.

    Args:
        raw: Decoded ``{sport}/league/teamnav``, ``{sport}/league/conferences``
            or ``explore/browse/{section}/main`` payload.

    Returns:
        One dict per entity with ``fox_id`` (the trailing numeric id of
        ``entityLink.contentUri``, as ``str``; ``None`` for browse rows that
        carry no entity uri), ``abbreviation``, ``name``, ``content_uri``,
        ``content_type``, ``web_url``, ``color``, ``logo_url`` and the bucket
        ``group``. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Pull a league's whole team directory::

            from sportsdataverse._fox_layout import fox_get, parse_nav_items
            rows = parse_nav_items(fox_get("nfl/league/teamnav"))
    """
    buckets: List[tuple] = [(None, raw.get("navItems") or [])]
    for g in raw.get("groups", []) or []:
        buckets.append(((g.get("header") or {}).get("title"), g.get("items") or []))
    rows: List[Dict] = []
    for group_title, items in buckets:
        for it in items:
            link = it.get("entityLink") or {}
            rows.append(
                {
                    "group": group_title,
                    "fox_id": _uri_id(link.get("contentUri")),
                    "abbreviation": it.get("title"),
                    "name": link.get("title") or it.get("imageAltText"),
                    "content_uri": link.get("contentUri"),
                    "content_type": link.get("contentType"),
                    "web_url": link.get("webUrl") or it.get("webUrl"),
                    "color": link.get("color"),
                    "logo_url": it.get("logoUrl"),
                }
            )
    return rows


def parse_header(raw: Dict) -> List[Dict]:
    """league/header + team/{id}/header -> a single summary row.

    Args:
        raw: Decoded ``{sport}/league/header`` or ``{sport}/team/{id}/header``
            payload (a flat entity-header object).

    Returns:
        A one-element list with ``template``, ``title``, ``entity_id`` (``str``),
        ``content_uri``, ``content_type``, ``color``, ``logo_url``,
        ``image_alt_text``, ``rank`` and the joined ``details`` line. An empty
        payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Read a team's header row::

            from sportsdataverse._fox_layout import fox_get, parse_header
            rows = parse_header(fox_get("nfl/team/10/header"))
    """
    if not raw or not (raw.get("title") or raw.get("contentUri")):
        return []
    details = [d.get("text") for d in raw.get("details") or [] if isinstance(d, dict)]
    return [
        {
            "template": raw.get("template"),
            "title": raw.get("title"),
            "entity_id": _uri_id(raw.get("contentUri")),
            "content_uri": raw.get("contentUri"),
            "content_type": raw.get("contentType"),
            "color": raw.get("color"),
            "logo_url": raw.get("logoUrl"),
            "image_alt_text": raw.get("imageAltText"),
            "rank": raw.get("rank"),
            "details": " · ".join(str(d) for d in details if d) or None,
        }
    ]


def parse_player_news(raw: Dict) -> List[Dict]:
    """league/playernews -> one row per news item.

    Args:
        raw: Decoded ``{sport}/league/playernews`` payload.

    Returns:
        One dict per item with ``title``, ``subtitle``, ``headline``,
        ``description``, ``impact_title``, ``impact``, ``date``, ``source``,
        ``athlete_id`` (``str``), ``content_uri`` and ``web_url``. The feed is
        empty in the offseason, which yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Read the league-wide player news feed::

            from sportsdataverse._fox_layout import fox_get, parse_player_news
            rows = parse_player_news(fox_get("nfl/league/playernews"))
    """
    rows: List[Dict] = []
    for it in raw.get("items", []) or []:
        link = it.get("entityLink") or {}
        rows.append(
            {
                "title": it.get("title"),
                "subtitle": it.get("subtitle"),
                "headline": it.get("headline"),
                "description": it.get("description"),
                "impact_title": it.get("impactTitle"),
                "impact": it.get("impact"),
                "date": it.get("date"),
                "source": it.get("source"),
                "athlete_id": _uri_id(link.get("contentUri")),
                "content_uri": link.get("contentUri"),
                "web_url": link.get("webUrl"),
            }
        )
    return rows


def parse_stat_leaders(raw: Dict) -> List[Dict]:
    """league/stats leadersSections -> one row per category stat leader.

    The league-wide twin of :func:`parse_team_stats` (same ``leadersSections``
    shape, no ``team_id`` to stamp).

    Args:
        raw: Decoded ``{sport}/league/stats`` payload.

    Returns:
        One dict per leader with ``category``, ``stat``, ``stat_abbreviation``,
        ``player`` and ``value``. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Read the league stats landing leaders::

            from sportsdataverse._fox_layout import fox_get, parse_stat_leaders
            rows = parse_stat_leaders(fox_get("nfl/league/stats"))
    """
    rows: List[Dict] = []
    for sec in raw.get("leadersSections", []) or []:
        for ld in sec.get("leaders", []) or []:
            rows.append(
                {
                    "category": sec.get("title"),
                    "stat": ld.get("title"),
                    "stat_abbreviation": ld.get("statAbbreviation"),
                    "player": ld.get("name"),
                    "value": ld.get("statValue"),
                }
            )
    return rows


def parse_odds_board(raw: Dict) -> List[Dict]:
    """league/odds -> one row per team per game (the six-pack modules).

    The league odds board wraps the same ``odds`` table :func:`parse_odds`
    reads from a single event, once per ``six-pack`` module.

    Args:
        raw: Decoded ``{sport}/league/odds`` payload.

    Returns:
        One dict per team per game with ``section``, ``game_id`` (``str``),
        ``event_time``, ``event_status``, ``team`` and one column per odds
        column header. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Flatten the NBA odds board::

            from sportsdataverse._fox_layout import fox_get, parse_odds_board
            rows = parse_odds_board(fox_get("nba/league/odds"))
    """
    rows: List[Dict] = []
    for sec in raw.get("sectionList", []) or []:
        for mod in sec.get("modules", []) or []:
            model = mod.get("model") or {}
            odds = model.get("odds")
            if not odds:
                continue
            gid = _uri_id(model.get("contentUri")) or _uri_id((model.get("entityLink") or {}).get("contentUri"))
            names = [_clean(c) for c in _cells(odds.get("columnHeaders"))]
            for r in odds.get("rows", []) or []:
                row: Dict[str, Any] = {
                    "section": sec.get("title"),
                    "game_id": gid,
                    "event_time": model.get("eventTime"),
                    "event_status": model.get("eventStatus"),
                    "team": r.get("fullText") or r.get("text"),
                }
                for name, v in zip(names, r.get("values", []) or []):
                    row[name] = (v or {}).get("odds")
                rows.append(row)
    return rows


def parse_matchup(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/matchup teamStatsComparison -> one row per compared stat.

    ``teamStatsComparison.items[]`` is the one block that is shape-identical
    across every Fox sport; the rest of the matchup payload diverges per sport.

    Args:
        raw: Decoded ``{sport}/event/{id}/matchup`` payload.
        game_id: Fox Bifrost event id, stamped on every row as ``str``.

    Returns:
        One dict per stat with ``left_team`` / ``left_team_id`` (``str``) /
        ``left_value`` / ``left_emphasized`` and the ``right_*`` twins. An empty
        payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Compare the two teams' season stats::

            from sportsdataverse._fox_layout import fox_get, parse_matchup
            rows = parse_matchup(fox_get("nba/event/1234/matchup"), 1234)
    """
    comp = raw.get("teamStatsComparison") or {}
    left_link = comp.get("leftEntityLink") or {}
    right_link = comp.get("rightEntityLink") or {}
    rows: List[Dict] = []
    for it in comp.get("items", []) or []:
        left = it.get("leftItemDetails") or {}
        right = it.get("rightItemDetails") or {}
        rows.append(
            {
                "game_id": str(game_id),
                "stat": it.get("title"),
                "left_team": comp.get("leftName"),
                "left_team_id": _uri_id(left_link.get("contentUri")),
                "left_value": left.get("title"),
                "left_emphasized": left.get("emphasized"),
                "right_team": comp.get("rightName"),
                "right_team_id": _uri_id(right_link.get("contentUri")),
                "right_value": right.get("title"),
                "right_emphasized": right.get("emphasized"),
            }
        )
    return rows


def parse_top_performers(raw: Dict, game_id: Union[int, str]) -> List[Dict]:
    """event/{id}/recap topPerformers -> one row per highlighted player.

    Args:
        raw: Decoded ``{sport}/event/{id}/recap`` payload.
        game_id: Fox Bifrost event id, stamped on every row as ``str``.

    Returns:
        One dict per performer with ``player``, ``team_position``,
        ``stat_line``, ``athlete_id`` (``str``), ``content_uri`` and
        ``web_url``. A pregame recap carries no ``topPerformers`` and yields
        ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Read a finished game's top performers::

            from sportsdataverse._fox_layout import fox_get, parse_top_performers
            rows = parse_top_performers(fox_get("nba/event/1234/recap"), 1234)
    """
    rows: List[Dict] = []
    for it in (raw.get("topPerformers") or {}).get("items", []) or []:
        link = it.get("entityLink") or {}
        rows.append(
            {
                "game_id": str(game_id),
                "player": it.get("title"),
                "team_position": it.get("subtitle"),
                "stat_line": it.get("statLine"),
                "athlete_id": _uri_id(link.get("contentUri")),
                "content_uri": link.get("contentUri"),
                "web_url": link.get("webUrl"),
            }
        )
    return rows


def parse_search_results(raw: Dict) -> List[Dict]:
    """search/{content,entities,popular} -> one row per result component.

    Args:
        raw: Decoded ``search/content``, ``search/entities`` or
            ``search/popular`` payload (all three share ``results[].components[]``).

    Returns:
        One dict per component with ``group`` (the result bucket title),
        ``type``, ``entity_id`` (``str``), ``title``, ``subtitle``,
        ``content_type``, ``content_uri``, ``web_url``, ``analytics_name`` and
        ``image_url``. An empty payload yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Resolve a team by name::

            from sportsdataverse._fox_layout import fox_get, parse_search_results
            rows = parse_search_results(fox_get("search/entities", params={"text": "chiefs"}))
    """
    rows: List[Dict] = []
    for res in raw.get("results", []) or []:
        group = res.get("title")
        for comp in res.get("components", []) or []:
            model = comp.get("model") or {}
            img = model.get("image")
            rows.append(
                {
                    "group": group,
                    "type": comp.get("type"),
                    "entity_id": _uri_id(model.get("contentUri")),
                    "title": model.get("title"),
                    "subtitle": model.get("subtitle"),
                    "content_type": model.get("contentType"),
                    "content_uri": model.get("contentUri"),
                    "web_url": model.get("webUrl"),
                    "analytics_name": model.get("analyticsName"),
                    "image_url": img.get("url") if isinstance(img, dict) else None,
                }
            )
    return rows


def parse_trending(raw: Dict) -> List[Dict]:
    """general/trending/{articles,videos} -> one row per CMS item.

    The trending payloads are full editorial CMS documents (100+ keys per
    result); this keeps the stable identification / linking subset.

    Args:
        raw: Decoded ``general/trending/articles`` or
            ``general/trending/videos`` payload.

    Returns:
        One dict per result with ``id``, ``spark_id``, ``title``,
        ``description``, ``content_type``, ``component_type``,
        ``publication_date``, ``last_published_date``, ``canonical_url``,
        ``thumbnail_url`` and ``playback_url`` (videos only). An empty payload
        yields ``[]``.

    Raises:
        None: a malformed or empty payload returns ``[]`` rather than raising.

    Example:
        Read the trending video feed::

            from sportsdataverse._fox_layout import fox_get_feed, parse_trending
            raw = fox_get_feed("bifrost/v1/general/trending/videos", params={"duration": 4})
            rows = parse_trending(raw)
    """
    rows: List[Dict] = []
    for it in (raw.get("data") or {}).get("results") or []:
        thumb = it.get("thumbnail")
        thumb_url = None
        if isinstance(thumb, dict):
            content = thumb.get("content")
            thumb_url = thumb.get("url") or (content.get("url") if isinstance(content, dict) else None)
        rows.append(
            {
                "id": it.get("id"),
                "spark_id": it.get("spark_id"),
                "title": it.get("title"),
                "description": it.get("description") or it.get("dek") or it.get("meta_description") or None,
                "content_type": it.get("content_type"),
                "component_type": it.get("component_type"),
                "publication_date": it.get("publication_date"),
                "last_published_date": it.get("last_published_date"),
                "canonical_url": it.get("canonical_url"),
                "thumbnail_url": thumb_url,
                "playback_url": it.get("playback_url"),
            }
        )
    return rows


# ---- feed-tier transport (trending + fan polls) ---------------------------
# Public feed-tier key shipped in the foxsports.com web bundle. Overridable via
# SDV_PY_FOX_FEED_KEY so a key rotation does not require a release.
FEED_KEY = os.getenv("SDV_PY_FOX_FEED_KEY", "SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg")
HOST = "https://api.foxsports.com"


def fox_get_feed(path: str, params: Optional[dict] = None, **kwargs: Any) -> Dict[str, Any]:
    """GET a feed-tier ``api.foxsports.com`` path with the public feed key.

    The trending feeds and the fan-poll service sit behind a different Apigee
    key tier than :func:`fox_get`, and ``/foxpolls`` is not under ``/bifrost/v1``
    at all -- so ``path`` here is relative to the host root, not to Bifrost.

    Args:
        path: Path under ``https://api.foxsports.com`` (e.g.
            ``"bifrost/v1/general/trending/videos"`` or ``"foxpolls/v1/polls"``).
        params: Extra query params merged on top of ``apikey`` / ``api-version``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        The parsed JSON response body as a ``dict``.

    Raises:
        None: transport failures surface from the underlying getter.

    Example:
        Read the trending article feed::

            from sportsdataverse._fox_layout import fox_get_feed
            raw = fox_get_feed("bifrost/v1/general/trending/articles", params={"duration": 4})
    """
    merged = {"apikey": FEED_KEY, "api-version": "1.1"}
    if params:
        merged.update(params)
    return _get(f"{HOST}/{path}", params=merged, headers=_HEADERS, **kwargs)


_Result = Union[pl.DataFrame, "pd.DataFrame", Dict[str, Any]]


def fox_search_content(
    text: str, *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> _Result:
    """Fox Sports full content search (``search/content``).

    Args:
        text: Search term.
        return_parsed: If ``True`` (default) flatten ``results[].components[]``
            to a DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Search across teams, players and shows::

            from sportsdataverse._fox_layout import fox_search_content
            df = fox_search_content("chiefs")
    """
    raw = fox_get("search/content", params={"text": text}, **kwargs)
    return frame(parse_search_results(raw), return_as_pandas) if return_parsed else raw


def fox_search_entities(
    text: str, *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> _Result:
    """Fox Sports entity typeahead (``search/entities``) -- teams, players, leagues.

    Args:
        text: Search term.
        return_parsed: If ``True`` (default) flatten ``results[].components[]``
            to a DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Resolve a Fox team id from a name::

            from sportsdataverse._fox_layout import fox_search_entities
            df = fox_search_entities("chiefs")
    """
    raw = fox_get("search/entities", params={"text": text}, **kwargs)
    return frame(parse_search_results(raw), return_as_pandas) if return_parsed else raw


def fox_search_popular(*, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any) -> _Result:
    """Fox Sports popular-search suggestions (``search/popular``).

    Args:
        return_parsed: If ``True`` (default) flatten ``results[].components[]``
            to a DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Read today's popular entities::

            from sportsdataverse._fox_layout import fox_search_popular
            df = fox_search_popular()
    """
    raw = fox_get("search/popular", **kwargs)
    return frame(parse_search_results(raw), return_as_pandas) if return_parsed else raw


def fox_explore_browse(
    section: str = "sports", *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> _Result:
    """Fox Sports browse directory (``explore/browse/{section}/main``).

    Args:
        section: One of ``"sports"``, ``"players"``, ``"shows"``,
            ``"personalities"``, ``"topics"``.
        return_parsed: If ``True`` (default) flatten ``groups[].items[]`` to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        List every browsable sport::

            from sportsdataverse._fox_layout import fox_explore_browse
            df = fox_explore_browse("sports")
    """
    raw = fox_get(f"explore/browse/{section}/main", **kwargs)
    return frame(parse_nav_items(raw), return_as_pandas) if return_parsed else raw


def fox_explore_odds(*, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any) -> _Result:
    """Cross-sport odds landing (``explore/odds/main``) -- one row per featured game.

    Args:
        return_parsed: If ``True`` (default) flatten the ``featured-events``
            modules to a DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Read the featured cross-sport betting board::

            from sportsdataverse._fox_layout import fox_explore_odds
            df = fox_explore_odds()
    """
    raw = fox_get("explore/odds/main", **kwargs)
    return frame(parse_segment_events(raw), return_as_pandas) if return_parsed else raw


def fox_topevents_scoreboard(*, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any) -> _Result:
    """Cross-sport top-events board nav (``topevents/scoreboard/main``).

    Thin: it enumerates the board's segment selections, whose ids feed
    :func:`fox_topevents_segment`.

    Args:
        return_parsed: If ``True`` (default) flatten the nav selections to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        List the top-events board segments::

            from sportsdataverse._fox_layout import fox_topevents_scoreboard
            df = fox_topevents_scoreboard()
    """
    raw = fox_get("topevents/scoreboard/main", **kwargs)
    return frame(parse_selection_nav(raw), return_as_pandas) if return_parsed else raw


def fox_topevents_segment(
    segment: Union[int, str] = 0, *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs: Any
) -> _Result:
    """Cross-sport top-events board page (``topevents/scoreboard/segment/{segment}``).

    Args:
        segment: 0-based page index of the paginated cross-sport board.
        return_parsed: If ``True`` (default) flatten the events to one row per
            game; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Read today's cross-sport games::

            from sportsdataverse._fox_layout import fox_topevents_segment
            df = fox_topevents_segment(1)
    """
    raw = fox_get(f"topevents/scoreboard/segment/{segment}", **kwargs)
    return frame(parse_segment_events(raw, segment), return_as_pandas) if return_parsed else raw


def fox_trending_articles(
    *,
    duration: int = 4,
    tags: Optional[str] = None,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> _Result:
    """Fox Sports trending articles (``general/trending/articles``) -- feed-tier key.

    Args:
        duration: Trailing window in hours.
        tags: Optional tag filter, e.g. ``"league:football/cfb/league/1"``.
        return_parsed: If ``True`` (default) flatten ``data.results[]`` to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Trending college-football articles::

            from sportsdataverse._fox_layout import fox_trending_articles
            df = fox_trending_articles(tags="league:football/cfb/league/1")
    """
    params: Dict[str, Any] = {"duration": duration}
    if tags:
        params["tags"] = tags
    raw = fox_get_feed("bifrost/v1/general/trending/articles", params=params, **kwargs)
    return frame(parse_trending(raw), return_as_pandas) if return_parsed else raw


def fox_trending_videos(
    *,
    duration: int = 4,
    max_items: int = 12,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> _Result:
    """Fox Sports trending videos (``general/trending/videos``) -- feed-tier key.

    Args:
        duration: Trailing window in hours.
        max_items: Maximum number of clips to return.
        return_parsed: If ``True`` (default) flatten ``data.results[]`` to a
            DataFrame; if ``False`` return the raw JSON ``dict``.
        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise
            polars. Ignored when ``return_parsed=False``.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars DataFrame (default), a pandas DataFrame when
        ``return_as_pandas=True``, or the raw JSON ``dict`` when
        ``return_parsed=False``.

    Raises:
        None: an empty or malformed payload yields a zero-row frame.

    Example:
        Trending clips with playback urls::

            from sportsdataverse._fox_layout import fox_trending_videos
            df = fox_trending_videos(max_items=25)
    """
    raw = fox_get_feed(
        "bifrost/v1/general/trending/videos", params={"duration": duration, "maxItems": max_items}, **kwargs
    )
    return frame(parse_trending(raw), return_as_pandas) if return_parsed else raw


def fox_polls(entity_ids: str, *, include_answers: bool = True, **kwargs: Any) -> Dict[str, Any]:
    """Fan polls attached to an entity (``foxpolls/v1/polls``) -- feed-tier key.

    Raw-only: the service answers ``{"values": []}`` for every entity captured
    in ``sdv-internal-refs`` (no live poll was running), so there is no
    ground-truth row shape to flatten against. Callers get the payload as-is.

    Args:
        entity_ids: One or more ``contentUri`` values (comma-separated), e.g.
            ``"football/nfl/teams/11"``.
        include_answers: Whether to include answer tallies.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        The raw JSON ``dict`` -- ``{"values": [...]}``.

    Raises:
        None: transport failures surface from the underlying getter.

    Example:
        Ask for a team's active fan poll::

            from sportsdataverse._fox_layout import fox_polls
            raw = fox_polls("football/nfl/teams/11")
    """
    return fox_get_feed(
        "foxpolls/v1/polls",
        params={"associatedEntityIds": entity_ids, "includeAnswers": str(bool(include_answers)).lower()},
        **kwargs,
    )


# ---- per-league endpoint builder -----------------------------------------
# (short, summary, path template, path-arg name, parser, parser takes the arg,
#  accepts a groupId filter). ``parser=None`` -> raw-passthrough wrapper.
_LEAGUE_ENDPOINTS: List[Any] = [
    (
        "scoreboard",
        "scoreboard nav selections (weeks / dates / groups)",
        "{sport}/scoreboard/main",
        None,
        parse_selection_nav,
        False,
        True,
    ),
    ("league_scores", "league scores nav selections", "{sport}/league/scores", None, parse_selection_nav, False, True),
    (
        "league_schedule",
        "league schedule nav selections",
        "{sport}/league/schedule",
        None,
        parse_selection_nav,
        False,
        True,
    ),
    (
        "scores_segment",
        "one row per game in a scoreboard segment",
        "{sport}/league/scores-segment/{arg}",
        "segment_id",
        parse_segment_events,
        True,
        True,
    ),
    ("teamnav", "team directory (one row per team)", "{sport}/league/teamnav", None, parse_nav_items, False, False),
    (
        "league_conferences",
        "conference / group directory",
        "{sport}/league/conferences",
        None,
        parse_nav_items,
        False,
        False,
    ),
    (
        "league_standings",
        "league-wide standings tables",
        "{sport}/league/standings",
        None,
        parse_standings,
        False,
        True,
    ),
    (
        "league_polls",
        "rankings / polls rendered as standings tables",
        "{sport}/league/polls",
        None,
        parse_standings,
        False,
        False,
    ),
    ("league_header", "league header (one row)", "{sport}/league/header", None, parse_header, False, False),
    (
        "league_player_news",
        "league-wide player news feed",
        "{sport}/league/playernews",
        None,
        parse_player_news,
        False,
        False,
    ),
    (
        "league_stat_leaders",
        "league stats landing leaders",
        "{sport}/league/stats",
        None,
        parse_stat_leaders,
        False,
        False,
    ),
    (
        "league_odds",
        "league odds board (one row per team per game)",
        "{sport}/league/odds",
        None,
        parse_odds_board,
        False,
        True,
    ),
    ("team_header", "team header (one row)", "{sport}/team/{arg}/header", "team_id", parse_header, False, False),
    (
        "event_matchup",
        "pregame team-stat comparison (one row per stat)",
        "{sport}/event/{arg}/matchup",
        "game_id",
        parse_matchup,
        True,
        False,
    ),
    (
        "event_recap",
        "postgame top performers (one row per player)",
        "{sport}/event/{arg}/recap",
        "game_id",
        parse_top_performers,
        True,
        False,
    ),
    (
        "event_standings",
        "the two teams' standings context",
        "{sport}/event/{arg}/standings",
        "game_id",
        parse_standings,
        False,
        False,
    ),
    (
        "scorechip",
        "compact live score chip (raw dict -- live-only, uncaptured shape)",
        "{sport}/scorechip/{arg}",
        "chip_id",
        None,
        False,
        False,
    ),
]

_ARG_EXAMPLE = {"segment_id": '"..."', "team_id": '"..."', "game_id": '"..."', "chip_id": '"nfl12345"'}


def _build_fox_wrapper(
    sport: str,
    full_name: str,
    module: str,
    summary: str,
    path_tmpl: str,
    arg_name: Optional[str],
    parser: Optional[Any],
    parser_takes_arg: bool,
    supports_group: bool,
) -> Any:
    """Mint one ``fox_<prefix>_<short>`` wrapper bound to ``sport``."""

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        value: Any = None
        if arg_name is not None:
            if args:
                value = args[0]
            elif arg_name in kwargs:
                value = kwargs.pop(arg_name)
            else:
                raise TypeError(f"{full_name}() missing required argument: '{arg_name}'")
        return_parsed = bool(kwargs.pop("return_parsed", True))
        return_as_pandas = bool(kwargs.pop("return_as_pandas", False))
        group_id = kwargs.pop("group_id", None) if supports_group else None
        params = {"groupId": group_id} if group_id is not None else None
        raw = fox_get(path_tmpl.format(sport=sport, arg=value), params=params, **kwargs)
        if parser is None or not return_parsed:
            return raw
        rows = parser(raw, value) if parser_takes_arg else parser(raw)
        return frame(rows, return_as_pandas)

    arg_doc = f"        {arg_name}: Fox Bifrost {arg_name.replace('_', ' ')}.\n" if arg_name else ""
    group_doc = "        group_id: Optional Fox conference / group filter (``groupId``).\n" if supports_group else ""
    if parser is None:
        parsed_doc = (
            "    Returns:\n        The raw JSON ``dict``.\n\n"
            "    Raises:\n        None: transport failures surface from the underlying getter.\n"
        )
        flag_doc = ""
    else:
        parsed_doc = (
            "    Returns:\n        A polars DataFrame (default), a pandas DataFrame when\n"
            "        ``return_as_pandas=True``, or the raw JSON ``dict`` when\n"
            "        ``return_parsed=False``.\n\n"
            "    Raises:\n        None: an empty or malformed payload yields a zero-row frame.\n"
        )
        flag_doc = (
            "        return_parsed: If ``True`` (default) flatten the layout payload to a\n"
            "            DataFrame; if ``False`` return the raw JSON ``dict``.\n"
            "        return_as_pandas: If ``True`` return a pandas DataFrame; otherwise\n"
            "            polars. Ignored when ``return_parsed=False``.\n"
        )
    example_arg = _ARG_EXAMPLE.get(arg_name or "", "")
    wrapper.__name__ = full_name
    wrapper.__qualname__ = full_name
    wrapper.__doc__ = (
        f"Fox Sports {sport} {summary}.\n\n"
        f"    Wraps ``{path_tmpl.format(sport=sport, arg='{' + (arg_name or 'id') + '}')}``.\n\n"
        "    Args:\n"
        f"{arg_doc}{group_doc}{flag_doc}"
        "        **kwargs: Forwarded to the underlying HTTP getter.\n\n"
        f"{parsed_doc}\n"
        "    Example:\n"
        "        Fetch the frame::\n\n"
        f"            from {module} import {full_name}\n"
        f"            df = {full_name}({example_arg})\n"
    )
    return wrapper


def register_league_endpoints(sport: str, prefix: str, namespace: Dict[str, Any]) -> List[str]:
    """Register the shared Fox league/event/team wrappers into a league module.

    The Bifrost layout API is uniform across sports, so every league gets the
    same 17 endpoint wrappers off one table; only the ``{sport}`` slug differs.
    Mirrors the ESPN ``make_league_module`` idiom: call it once from
    ``<league>_fox_ext.py`` and extend the module's ``__all__`` with the result.

    Args:
        sport: Fox sport slug (``"cfb"``, ``"nfl"``, ``"cbk"``, ``"wcbk"``,
            ``"nba"``, ``"wnba"``, ``"nhl"``, ``"mlb"``, ...).
        prefix: sdv-py league prefix used in the public names
            (``fox_<prefix>_<short>``).
        namespace: The calling module's ``globals()``.

    Returns:
        The list of public names registered, in table order -- feed it straight
        into the module's ``__all__``.

    Raises:
        None.

    Example:
        Wire a league module::

            from sportsdataverse._fox_layout import register_league_endpoints
            __all__ = [...] + register_league_endpoints("nba", "nba", globals())
    """
    module = ".".join(str(namespace.get("__name__", "sportsdataverse")).split(".")[:2])
    names: List[str] = []
    for short, summary, path_tmpl, arg_name, parser, takes_arg, supports_group in _LEAGUE_ENDPOINTS:
        full_name = f"fox_{prefix}_{short}"
        namespace[full_name] = _build_fox_wrapper(
            sport, full_name, module, summary, path_tmpl, arg_name, parser, takes_arg, supports_group
        )
        names.append(full_name)
    return names
