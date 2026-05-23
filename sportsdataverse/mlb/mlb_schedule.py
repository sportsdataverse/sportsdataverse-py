from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download


def espn_mlb_schedule(dates=None, season_type=None, limit=500, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mlb_schedule - look up the MLB schedule for a given date or season-year.

    Args:
        dates (int): Date filter. Either a calendar date as YYYYMMDD or a season-year (e.g. 2024).
            When a 4-digit year is passed, the call returns the full season slate (paginated by ``limit``).
        season_type (int): Season type — 1 = spring training, 2 = regular, 3 = postseason, 4 = all-star.
        limit (int): Number of records to return. Default 500.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False (default),
            returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule. Returns ``None`` if no games.

    Example:
        Pull a single date's slate (Opening Day 2024)::

            from sportsdataverse.mlb import espn_mlb_schedule
            sched = espn_mlb_schedule(dates=20240328)
            print(sched.shape)
            sched.select(["game_id", "home_name", "away_name", "status_type_description"]).head()

        Pull a regular-season slate from a season-year::

            reg = espn_mlb_schedule(dates=2024, season_type=2, limit=500)
            reg.group_by("status_type_description").len().sort("len", descending=True)

        Pandas round-trip for one date::

            espn_mlb_schedule(dates=20240328, return_as_pandas=True).head()
    """
    url = "http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
    params = {"limit": limit}
    if dates is not None:
        params["dates"] = dates
    if season_type is not None:
        params["seasontype"] = season_type

    resp = download(url=url, params=params, **kwargs)
    if resp is None:
        return None
    payload = resp.json()
    events = payload.get("events", [])
    if not events:
        return None

    rows = [_scoreboard_event_parsing(ev) for ev in events]
    df = pd.DataFrame(rows)
    return df if return_as_pandas else pl.from_pandas(df)


def _scoreboard_event_parsing(event: dict) -> dict:
    """Flatten one ESPN scoreboard event into a single row keyed by ``game_id``."""
    comp = (event.get("competitions") or [{}])[0]
    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") == "away"), {})
    status = (event.get("status") or {}).get("type") or {}
    venue = comp.get("venue") or {}

    def _team(side):
        team = side.get("team") or {}
        return {
            "id": team.get("id"),
            "name": team.get("name"),
            "abbreviation": team.get("abbreviation"),
            "displayName": team.get("displayName"),
            "location": team.get("location"),
            "color": team.get("color"),
            "alternateColor": team.get("alternateColor"),
            "score": side.get("score"),
            "winner": side.get("winner"),
        }

    h, a = _team(home), _team(away)
    return {
        "game_id": event.get("id"),
        "date": event.get("date"),
        "season_year": (event.get("season") or {}).get("year"),
        "season_type": (event.get("season") or {}).get("type"),
        "status_type_state": status.get("state"),
        "status_type_completed": status.get("completed"),
        "status_type_description": status.get("description"),
        "venue_id": venue.get("id"),
        "venue_full_name": venue.get("fullName"),
        "venue_city": (venue.get("address") or {}).get("city"),
        "venue_state": (venue.get("address") or {}).get("state"),
        "home_id": h["id"],
        "home_name": h["name"],
        "home_abbreviation": h["abbreviation"],
        "home_display_name": h["displayName"],
        "home_score": h["score"],
        "home_winner": h["winner"],
        "away_id": a["id"],
        "away_name": a["name"],
        "away_abbreviation": a["abbreviation"],
        "away_display_name": a["displayName"],
        "away_score": a["score"],
        "away_winner": a["winner"],
    }


def espn_mlb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> pl.DataFrame:
    """espn_mlb_calendar - look up the MLB game-day calendar for a season.

    Wraps the Core v2 endpoint::

        https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/{season}/types/2/calendar/ondays

    Args:
        season (int): Season year (e.g. 2024).
        ondays (bool): When True, returns the date-only list ESPN exposes; when False,
            returns a one-row-per-date frame with `eventDate` plus parsed year/month/day.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: Polars dataframe of valid game dates.

    Example:
        Get every game date in the 2024 MLB regular season::

            from sportsdataverse.mlb import espn_mlb_calendar
            cal = espn_mlb_calendar(season=2024)
            print(cal.shape)
            cal.head()
    """
    if season is None:
        season = datetime.date.today().year
    url = f"https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/{season}/types/2/calendar/ondays"
    resp = download(url=url, **kwargs)
    if resp is None:
        return None
    payload = resp.json()
    eventdates = payload.get("eventDate", {}).get("dates", []) or []
    rows = []
    for d in eventdates:
        try:
            dt = datetime.datetime.fromisoformat(d.replace("Z", "+00:00"))
        except Exception:
            dt = None
        rows.append(
            {
                "event_date": d,
                "year": dt.year if dt else None,
                "month": dt.month if dt else None,
                "day": dt.day if dt else None,
                "season": season,
            },
        )
    df = pd.DataFrame(rows)
    return df if return_as_pandas else pl.from_pandas(df)


def most_recent_mlb_season() -> int:
    """most_recent_mlb_season - return the most recent / current MLB season year.

    MLB seasons run calendar-year. Before April we still consider the *previous* year
    the "most recent" season (since spring training only starts in late February).

    Returns:
        int: The most recent MLB season year (e.g. ``2024``).
    """
    today = datetime.date.today()
    return today.year if today.month >= 4 else today.year - 1
