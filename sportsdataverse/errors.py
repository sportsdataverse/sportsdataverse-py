"""
Custom exceptions for sportsdataverse module
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import requests


class SportsDataverseError(Exception):
    """Base class for every error raised by sportsdataverse.

    Catch this to handle any package-specific failure with one ``except``
    clause, while still being able to catch the narrower subclasses
    (:class:`SeasonNotFoundError`, :class:`NoESPNDataError`) individually::

        from sportsdataverse.errors import SportsDataverseError

        try:
            df = some_loader(...)
        except SportsDataverseError as exc:
            ...  # any sportsdataverse-originated failure

    Re-parenting the existing errors under this base is backwards compatible:
    code catching ``Exception`` or the specific subclasses keeps working.
    """


class SeasonNotFoundError(SportsDataverseError):
    """Raised when a caller requests a season earlier than the loader supports.

    Each sport submodule has a per-source minimum season (e.g. CFB PBP
    starts at ``2003``, NFL PBP at ``1999``). Loaders call
    :func:`season_not_found_error` at the top of each call to fail fast
    with a clear message when the requested season is below that floor.

    Example:
        Catch the error at the call site::

            from sportsdataverse.errors import (
                SeasonNotFoundError,
                season_not_found_error,
            )

            try:
                season_not_found_error(season=1995, min_season=2003)
            except SeasonNotFoundError as exc:
                print(f"unsupported season: {exc}")
                # unsupported season: Season 1995 not found, season cannot be less than 2003
    """

    pass


def season_not_found_error(season: int, min_season: int) -> None:
    """Raise :class:`SeasonNotFoundError` when ``season`` predates ``min_season``.

    Args:
        season: Caller-supplied season (int or coercible).
        min_season: Inclusive minimum supported season for the data source.

    Raises:
        SeasonNotFoundError: When ``int(season) < int(min_season)``.

    Example:
        Guard a custom loader::

            from sportsdataverse.errors import season_not_found_error

            def load_thing(season):
                season_not_found_error(season, min_season=2003)
                # ... fetch parquet ...
    """
    if int(season) >= int(min_season):
        return
    else:
        raise SeasonNotFoundError(f"Season {season} not found, season cannot be less than {min_season}")


class NoESPNDataError(SportsDataverseError):
    """Raised when an ESPN endpoint has no payload for the request.

    Triggered both by a raw HTTP 404 and by the legacy ESPN convention of
    returning a JSON body with ``{"code": 404, ...}`` and a 200 status.
    Both cases mean the same thing for callers: there is no data for the
    requested game / team / season.

    Example:
        Catch missing-data responses around an ESPN call::

            from sportsdataverse.errors import NoESPNDataError
            from sportsdataverse.cfb import espn_cfb_pbp

            try:
                pbp = espn_cfb_pbp(game_id=1)  # not a real ESPN game id
            except NoESPNDataError as exc:
                print(f"no data: {exc}")
                pbp = None
    """

    pass


def no_espn_data(response: "requests.Response") -> "requests.Response":
    """Validate an ESPN response, raising :class:`NoESPNDataError` if empty.

    Used by :func:`sportsdataverse.dl_utils.download` to normalize ESPN's
    two flavors of "no data" (HTTP 404 and 200-with-``code=404``-body)
    into a single exception type. The raised error message now includes
    a *next-action suggestion* derived from the URL (e.g. a 404 on
    ``/teams/99999/roster`` suggests ``find_team(name, league=...)``).

    Args:
        response: A ``requests.Response`` from an ESPN endpoint.

    Returns:
        The same ``response`` object unchanged when the payload is valid.

    Raises:
        NoESPNDataError: When the response is a 404 or carries a
            ``code: 404`` JSON body.

    Example:
        Use directly when you have a hand-rolled ``requests`` call::

            import requests
            from sportsdataverse.errors import NoESPNDataError, no_espn_data

            try:
                resp = no_espn_data(
                    requests.get("https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard")
                )
            except NoESPNDataError:
                resp = None
    """
    if response.status_code == 404:
        raise NoESPNDataError(_format_404(response.url))

    # ESPN's "200-but-empty" envelope is `{"code": 404, ...}`. Other
    # endpoints (e.g. jsonplaceholder.typicode.com/posts, stats.wnba.com
    # paginated payloads) return JSON arrays at the top level, where
    # ``.get("code")`` would raise AttributeError. Only probe for the
    # ESPN envelope when the body is a dict; non-dict bodies pass
    # through unchanged.
    try:
        body = response.json()
    except ValueError:
        # Non-JSON response (e.g. raw bytes, HTML error page). Treat as
        # valid — the caller is responsible for parsing.
        return response

    if isinstance(body, dict) and body.get("code") == 404:
        raise NoESPNDataError(_format_404(response.url, body=body))
    return response


# ---------------------------------------------------------------------------
# Suggestion engine — turn 404 URLs into actionable hints
# ---------------------------------------------------------------------------

import re as _re  # noqa: E402  (placed here to keep the file's existing top intact)

_SPORT_TO_LEAGUE = {
    "basketball/nba": "nba",
    "basketball/wnba": "wnba",
    "basketball/mens-college-basketball": "mbb",
    "basketball/womens-college-basketball": "wbb",
    "football/nfl": "nfl",
    "football/college-football": "cfb",
    "baseball/mlb": "mlb",
    "hockey/nhl": "nhl",
}


def _infer_league(url: str) -> str | None:
    """Extract the league slug from an ESPN URL, if recognisable.

    Recognises both ESPN URL shapes:

    * ``site.api.espn.com/.../sports/<sport>/<league>/...`` — flat
    * ``sports.core.api.espn.com/v2/sports/<sport>/leagues/<league>/...``
      — nested under ``/leagues/``
    """
    for slug, league in _SPORT_TO_LEAGUE.items():
        sport, league_slug = slug.split("/", 1)
        # site.api shape: /sports/<sport>/<league>/
        if f"/sports/{slug}/" in url or f"/sports/{slug}?" in url:
            return league
        # core.api shape: /sports/<sport>/leagues/<league>/
        if f"/sports/{sport}/leagues/{league_slug}/" in url or f"/sports/{sport}/leagues/{league_slug}?" in url:
            return league
    return None


def suggest_next_action(url: str) -> str | None:
    """Return a human-readable next-action hint for a failed ESPN URL.

    Inspects the URL for the failing entity (team / athlete / event /
    season) and suggests the right ``find_*`` resolver call to recover.
    Returns ``None`` when no specific suggestion fits.

    Examples::

        suggest_next_action(
            "https://site.api.espn.com/.../basketball/nba/teams/99999/roster")
        "Try `find_team(name, league='nba')` to look up a valid team_id."

        suggest_next_action(
            "https://site.api.espn.com/.../baseball/mlb/athletes/0/overview")
        "Try `find_athlete(name, league='mlb', team=...)` ..."

        suggest_next_action(
            "https://site.api.espn.com/.../basketball/nba/summary?event=999")
        "Try `find_event(date, league='nba', home=..., away=...)` ..."
    """
    league = _infer_league(url) or "<league>"

    if _re.search(r"/teams/\d+/roster", url):
        return (
            f"Try `find_team(name, league={league!r})` to look up a valid team_id, then pass its `id` to the wrapper."
        )
    if _re.search(r"/teams/\d+/schedule", url):
        return (
            f"Try `find_team(name, league={league!r})` to look up a valid team_id, then pass its `id` to the wrapper."
        )
    if _re.search(r"/teams/\d+(?:/|$|\?)", url):
        return f"Try `find_team(name, league={league!r})` to verify the team_id."
    if _re.search(r"/athletes?/\d+", url):
        return f"Try `find_athlete(name, league={league!r}, team=<team>)` to look up a valid athlete_id."
    if _re.search(r"summary\?event=\d+", url) or _re.search(r"/events?/\d+", url):
        return f"Try `find_event(date, league={league!r}, home=..., away=...)` to look up a valid event_id."
    if _re.search(r"/seasons/(\d{4})", url):
        return (
            "Season ID may predate available data. Try a more recent "
            "season or check coverage on the league's docs page."
        )
    if "/scoreboard" in url:
        return (
            "Scoreboard returned no data for the given date — verify the "
            "`dates=YYYYMMDD` parameter is correct and the date had games."
        )
    return None


def _format_404(url: str, body: object | None = None) -> str:
    """Build a NoESPNDataError message with an optional next-action hint."""
    base = f"NoESPNDataError: No data found for {url}"
    if body is not None:
        base += f", response: {body}"
    hint = suggest_next_action(url)
    if hint:
        # Import find_* names in the message for absolute clarity. The
        # user gets the exact line to paste into a notebook.
        base += "\n\n  Suggestion: " + hint + "\n  (from `sportsdataverse import find_team, find_athlete, find_event`)"
    return base
