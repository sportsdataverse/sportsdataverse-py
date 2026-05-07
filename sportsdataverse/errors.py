"""
Custom exceptions for sportsdataverse module
"""

from __future__ import annotations


class SeasonNotFoundError(Exception):
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


def season_not_found_error(season, min_season):
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


class NoESPNDataError(Exception):
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


def no_espn_data(response):
    """Validate an ESPN response, raising :class:`NoESPNDataError` if empty.

    Used by :func:`sportsdataverse.dl_utils.download` to normalize ESPN's
    two flavors of "no data" (HTTP 404 and 200-with-``code=404``-body)
    into a single exception type.

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
        raise NoESPNDataError(f"NoESPNDataError: No response for {response.url}")

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
        raise NoESPNDataError(f"NoESPNDataError: No data found for {response.url}, response: {body}")
    return response
