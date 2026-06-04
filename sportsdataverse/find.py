"""sportsdataverse.find — name-to-ID resolver helpers.

ESPN's wrappers expect numeric IDs everywhere (``team_id``,
``athlete_id``, ``event_id``). New users almost always start with a
name ("the Lakers", "LeBron James", "Game 5 of the 2024 NBA Finals")
and have to detour through a teams-list / search call before they can
do anything useful. This module collapses that step.

All helpers accept a ``league`` string (one of ``"nba"``, ``"wnba"``,
``"mbb"``, ``"wbb"``, ``"cfb"``, ``"nfl"``, ``"mlb"``, ``"nhl"``) and
return either a single match or a list. They use fuzzy substring
matching (case-insensitive) so partial names work.

::

    from sportsdataverse.find import find_team, find_event

    find_team("lakers", league="nba")
    # {'id': '13', 'abbreviation': 'LAL', 'display_name': 'Los Angeles Lakers'}

    find_event(date="2024-06-17", league="nba", home="Boston")
    # {'id': '401585607', 'name': 'Dallas Mavericks at Boston Celtics', ...}

All resolvers cache the team list per league for the lifetime of the
process — the team set rarely changes so we avoid repeating the
teams-site call on every lookup.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

# Map each league to its raw site-v2 teams + scoreboard wrappers. We use the
# raw-Dict ``espn_<league>_teams_site`` endpoint (not the parsed-DataFrame
# ``espn_<league>_teams``) because this resolver walks the raw JSON. Lazy-import
# the league modules so users who don't call find() pay nothing.
_LEAGUE_SLUG = {
    "nba": ("nba", "espn_nba_teams_site", "espn_nba_scoreboard"),
    "wnba": ("wnba", "espn_wnba_teams_site", "espn_wnba_scoreboard"),
    "mbb": ("mbb", "espn_mbb_teams_site", "espn_mbb_scoreboard"),
    "wbb": ("wbb", "espn_wbb_teams_site", "espn_wbb_scoreboard"),
    "cfb": ("cfb", "espn_cfb_teams_site", "espn_cfb_scoreboard"),
    "nfl": ("nfl", "espn_nfl_teams_site", "espn_nfl_scoreboard"),
    "mlb": ("mlb", "espn_mlb_teams_site", "espn_mlb_scoreboard"),
    "nhl": ("nhl", "espn_nhl_teams_site", "espn_nhl_scoreboard"),
}

# Per-process team-list cache. Keyed by league name.
_TEAM_CACHE: Dict[str, List[Dict[str, Any]]] = {}


def _resolve_league_callable(league: str, fn_attr: str):
    """Lazy-import the league module and return its named callable."""
    league = league.lower()
    if league not in _LEAGUE_SLUG:
        raise ValueError(
            f"Unknown league {league!r}. Choose one of {sorted(_LEAGUE_SLUG)}.",
        )
    league_attr, *_ = _LEAGUE_SLUG[league]
    import importlib

    mod = importlib.import_module(f"sportsdataverse.{league_attr}")
    return getattr(mod, fn_attr)


def _list_teams(league: str) -> List[Dict[str, Any]]:
    """Return a flat list of team dicts for the league (cached)."""
    league = league.lower()
    if league not in _LEAGUE_SLUG:
        raise ValueError(
            f"Unknown league {league!r}. Choose one of {sorted(_LEAGUE_SLUG)}.",
        )
    if league in _TEAM_CACHE:
        return _TEAM_CACHE[league]

    _, teams_attr, _ = _LEAGUE_SLUG[league]
    teams_fn = _resolve_league_callable(league, teams_attr)
    payload = teams_fn()  # raw Dict
    teams_raw = ((payload or {}).get("sports") or [{}])[0].get("leagues") or [{}]
    teams_raw = (teams_raw[0] or {}).get("teams") or [] if teams_raw else []
    # Each entry is {"team": {id, displayName, abbreviation, location, ...}}
    flat = [(t or {}).get("team") or {} for t in teams_raw]
    flat = [t for t in flat if t]
    _TEAM_CACHE[league] = flat
    return flat


def _matches(needle: str, *fields: Optional[str]) -> bool:
    """Case-insensitive substring match against any of the supplied fields."""
    needle = (needle or "").strip().lower()
    if not needle:
        return False
    for f in fields:
        if f and needle in f.lower():
            return True
    return False


# ---------------------------------------------------------------------------
# find_team
# ---------------------------------------------------------------------------


def find_team(
    name: str,
    league: str,
    *,
    multi: bool = False,
) -> Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """Resolve a team name to ESPN team metadata.

    Performs case-insensitive substring matching against the team's
    ``displayName``, ``location``, ``shortDisplayName``, ``name``,
    ``abbreviation``, and ``nickname`` fields. Returns the first match
    by default; pass ``multi=True`` to get every candidate.

    Args:
        name: Team identifier — full or partial name, location, or
            abbreviation. Case-insensitive.
        league: League slug (``"nba"``, ``"wnba"``, ``"mbb"``, ``"wbb"``,
            ``"cfb"``, ``"nfl"``, ``"mlb"``, ``"nhl"``).
        multi: If ``True``, return every match. Default is single-match
            (first hit; ``None`` if nothing matches).

    Returns:
        Single team dict (``{id, displayName, abbreviation, location, ...}``)
        or ``None`` when nothing matches; or a list of all matches when
        ``multi=True``.

    Examples::

        >>> find_team("lakers", league="nba")["id"]
        '13'
        >>> find_team("LAL", league="nba")["display_name"]
        'Los Angeles Lakers'
        >>> find_team("new york", league="nba", multi=True)
        [{... 'Knicks' ...}, {... 'Liberty' ...}]  # WNBA is separate
    """
    teams = _list_teams(league)
    matches = [
        t
        for t in teams
        if _matches(
            name,
            t.get("displayName"),
            t.get("location"),
            t.get("shortDisplayName"),
            t.get("name"),
            t.get("abbreviation"),
            t.get("nickname"),
        )
    ]
    if multi:
        return matches
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# find_athlete
# ---------------------------------------------------------------------------


def find_athlete(
    name: str,
    league: str,
    *,
    team: Optional[str] = None,
    multi: bool = False,
) -> Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """Resolve an athlete name to ESPN athlete metadata.

    Strategy: when a ``team=`` filter is given, hit that team's roster
    endpoint and search there (single fast call); otherwise iterate
    every team in the league and search their rosters (slower).

    Args:
        name: Athlete identifier — full or partial name.
        league: League slug.
        team: Optional team filter (passed through to :func:`find_team`).
            Strongly recommended for performance.
        multi: If ``True``, return all matches.

    Returns:
        Single athlete dict or list. Each entry contains the athlete's
        ESPN ID plus the team they were found on (so callers can
        immediately call ``espn_{league}_athlete_overview(athlete_id=...)``
        or other athlete-scoped wrappers).

    Examples::

        >>> find_athlete("lebron", league="nba", team="lakers")["id"]
        '1966'
        >>> find_athlete("Aaron Judge", league="mlb", team="Yankees")["id"]
        '33192'
    """
    league = league.lower()
    league_attr, *_ = _LEAGUE_SLUG[league]
    roster_fn = _resolve_league_callable(league, f"espn_{league_attr}_team_roster")

    candidate_teams: List[Dict[str, Any]]
    if team is not None:
        t = find_team(team, league=league)
        candidate_teams = [t] if t else []
    else:
        candidate_teams = _list_teams(league)

    matches: List[Dict[str, Any]] = []
    for t in candidate_teams:
        team_id = t.get("id")
        if team_id is None:
            continue
        try:
            payload = roster_fn(team_id=team_id)
        except Exception:
            continue
        # Roster shape varies — handle both flat and position-grouped
        raw_athletes = payload.get("athletes") or []
        flat_athletes: List[Dict[str, Any]] = []
        for entry in raw_athletes:
            if not isinstance(entry, dict):
                continue
            if "items" in entry and isinstance(entry["items"], list):
                # Position-grouped shape (MLB/NFL/NHL/CFB)
                for p in entry["items"]:
                    if isinstance(p, dict):
                        p = {**p, "position_group": entry.get("position")}
                        flat_athletes.append(p)
            else:
                # Flat shape (NBA/WNBA/MBB/WBB)
                flat_athletes.append(entry)
        for ath in flat_athletes:
            if _matches(
                name,
                ath.get("fullName"),
                ath.get("displayName"),
                ath.get("shortName"),
                ath.get("firstName"),
                ath.get("lastName"),
            ):
                ath_with_team = {**ath, "team_id": team_id, "team_display_name": t.get("displayName")}
                matches.append(ath_with_team)
                if not multi:
                    return ath_with_team
    if multi:
        return matches
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# find_event
# ---------------------------------------------------------------------------


def find_event(
    date: str,
    league: str,
    *,
    home: Optional[str] = None,
    away: Optional[str] = None,
    multi: bool = False,
) -> Union[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """Resolve a game to its ESPN event ID via the scoreboard endpoint.

    Args:
        date: YYYYMMDD (e.g. ``"20240617"``) or YYYY-MM-DD (e.g.
            ``"2024-06-17"``) — the latter is auto-normalized.
        league: League slug.
        home: Optional home-team filter (partial name OK).
        away: Optional away-team filter (partial name OK).
        multi: If ``True``, return every event on that date matching the
            filters (useful for double-headers / Game 1 vs Game 2).

    Returns:
        Single event dict (with the ``id`` you'd pass to
        ``espn_{league}_summary(event_id=...)`` / ``espn_{league}_pbp(game_id=...)``)
        or a list when ``multi=True``.

    Examples::

        >>> find_event("2024-06-17", league="nba", home="Boston")["id"]
        '401585607'
        >>> find_event("20250209", league="nfl", away="KC")["name"]
        'Kansas City Chiefs at Philadelphia Eagles'
    """
    league = league.lower()
    _, _, sb_attr = _LEAGUE_SLUG[league]
    scoreboard_fn = _resolve_league_callable(league, sb_attr)
    date_int = int(date.replace("-", ""))
    payload = scoreboard_fn(dates=date_int)
    events = (payload or {}).get("events") or []

    def _team_label(competitor: Dict[str, Any]) -> str:
        team = competitor.get("team") or {}
        return " ".join(
            str(v)
            for v in (
                team.get("displayName"),
                team.get("location"),
                team.get("abbreviation"),
                team.get("name"),
            )
            if v
        )

    matches: List[Dict[str, Any]] = []
    for ev in events:
        comp = (ev.get("competitions") or [{}])[0]
        cs = comp.get("competitors") or []
        h = next((c for c in cs if c.get("homeAway") == "home"), {})
        a = next((c for c in cs if c.get("homeAway") == "away"), {})
        h_label = _team_label(h)
        a_label = _team_label(a)
        if home and not _matches(home, h_label):
            continue
        if away and not _matches(away, a_label):
            continue
        matches.append(ev)
        if not multi:
            return ev
    if multi:
        return matches
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Cache control (for callers who want to refresh, e.g. mid-season trades)
# ---------------------------------------------------------------------------


def clear_team_cache(league: Optional[str] = None) -> None:
    """Reset the in-process team-list cache.

    Args:
        league: If given, only clear that league's cache. With no arg,
            clear every league.
    """
    if league is None:
        _TEAM_CACHE.clear()
    else:
        _TEAM_CACHE.pop(league.lower(), None)


__all__ = [
    "find_team",
    "find_athlete",
    "find_event",
    "clear_team_cache",
]
