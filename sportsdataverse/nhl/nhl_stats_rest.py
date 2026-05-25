"""sportsdataverse.nhl.nhl_stats_rest — wrappers for ``api.nhle.com/stats/rest/``.

**Documentation**:

* NHL Stats REST endpoint reference: https://py.sportsdataverse.org/docs/nhl/stats-rest
* Parser module: :mod:`sportsdataverse.nhl.nhl_stats_rest_parsers`

This module covers the NHL Stats REST API, which serves historical and
aggregate player/team/game statistics with Cayenne filter expressions.  It is
a **different surface** from the modern game-feed API in
:mod:`sportsdataverse.nhl.nhl_api_web` (``api-web.nhle.com/v1/``).

Endpoint catalog sourced from the OpenAPI 3.0 spec at
``fastRhockey/data-raw/nhl_stats_rest_openapi.yaml``.

Conventions
-----------

* **lang** defaults to ``"en"`` for every function.  Other locale codes
  (``"fr"``, ``"es"``, etc.) may work where the API supports them.
* **Cayenne filter expressions** — the Stats REST API uses a SQL-like filter
  syntax in the ``cayenneExp`` query parameter, e.g.
  ``cayenneExp="seasonId=20242025 and gameTypeId=2"``.  ``factCayenneExp``
  applies a secondary filter on fact/aggregate columns.
* **report** endpoints (``skater``, ``goalie``, ``team``) accept names such as
  ``"summary"``, ``"advanced"``, ``"powerplay"``, ``"penaltykill"``, etc.
* **attribute** endpoints (``leaders/goalies``, ``leaders/skaters``) accept
  stat-column names such as ``"wins"``, ``"savePct"``, ``"goals"``, ``"points"``.
* All functions return ``Dict`` (the raw JSON payload).  Parsing into tidy
  polars frames is a per-endpoint follow-up.
* ``**filters`` kwargs are passed directly as URL query parameters; ``None``
  values are stripped before the request is made.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sportsdataverse.dl_utils import download

_STATS_REST_BASE = "https://api.nhle.com/stats/rest"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_rest(path: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Dict:
    """Internal ``download() → .json()`` helper for the Stats REST base URL.

    Returns ``{}`` on any failure.
    """
    url = f"{_STATS_REST_BASE}{path}"
    resp = download(url=url, params=params, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _build_params(**filters: Any) -> Optional[Dict[str, Any]]:
    """Filter out ``None`` values from the caller-supplied query params dict."""
    cleaned = {k: v for k, v in filters.items() if v is not None}
    return cleaned if cleaned else None


# ---------------------------------------------------------------------------
# Ping
# ---------------------------------------------------------------------------


def nhl_stats_rest_ping(**kwargs) -> Dict:
    """Ping the NHL Stats REST API database.

    Wraps ``GET /ping``.  Useful as a liveness check before issuing heavier
    queries.

    Returns:
        Dict: ``{"ping": "moo"}`` (or similar API liveness payload) on success,
        ``{}`` on failure.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_ping
        print(nhl_stats_rest_ping())
    """
    return _fetch_rest("/ping", **kwargs)


# ---------------------------------------------------------------------------
# componentSeason
# ---------------------------------------------------------------------------


def nhl_stats_rest_component_season(lang: str = "en", **kwargs) -> Dict:
    """Retrieve the component-season configuration.

    Wraps ``GET /{lang}/componentSeason``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Component-season data object.
    """
    return _fetch_rest(f"/{lang}/componentSeason", **kwargs)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def nhl_stats_rest_config(lang: str = "en", **kwargs) -> Dict:
    """Retrieve the Stats REST API configuration payload.

    Wraps ``GET /{lang}/config``.  The config object describes available
    report names, attribute codes, and filter expression syntax.

    Args:
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Configuration data object.
    """
    return _fetch_rest(f"/{lang}/config", **kwargs)


# ---------------------------------------------------------------------------
# content/module
# ---------------------------------------------------------------------------


def nhl_stats_rest_content_module(template_key: str, lang: str = "en", **kwargs) -> Dict:
    """Retrieve a content module by template key.

    Wraps ``GET /{lang}/content/module/{templateKey}``.

    Args:
        template_key: The template key identifying the content module.
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Content module payload.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_content_module
        mod = nhl_stats_rest_content_module(template_key="homepage")
    """
    return _fetch_rest(f"/{lang}/content/module/{template_key}", **kwargs)


# ---------------------------------------------------------------------------
# country
# ---------------------------------------------------------------------------


def nhl_stats_rest_country(lang: str = "en", **kwargs) -> Dict:
    """Retrieve the list of countries used in NHL data.

    Wraps ``GET /{lang}/country``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Country list payload.
    """
    return _fetch_rest(f"/{lang}/country", **kwargs)


# ---------------------------------------------------------------------------
# draft
# ---------------------------------------------------------------------------


def nhl_stats_rest_draft(lang: str = "en", **filters) -> Dict:
    """Retrieve draft data, optionally filtered with Cayenne expressions.

    Wraps ``GET /{lang}/draft``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys include ``cayenneExp`` (e.g.
            ``cayenneExp="draftYear=2024"``), ``sort``, ``start``,
            ``limit``, ``include``, ``exclude``, ``factCayenneExp``,
            ``isAggregate``, ``isGame``, ``dir``.

    Returns:
        Dict: Draft records object with ``data`` array and ``total`` count.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_draft
        picks = nhl_stats_rest_draft(cayenneExp="draftYear=2024")
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/draft", params=params)


# ---------------------------------------------------------------------------
# franchise
# ---------------------------------------------------------------------------


def nhl_stats_rest_franchise(lang: str = "en", **filters) -> Dict:
    """Retrieve franchise data.

    Wraps ``GET /{lang}/franchise``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters.  Common keys: ``cayenneExp``,
            ``sort``, ``start``, ``limit``, ``include``, ``exclude``,
            ``factCayenneExp``, ``isAggregate``, ``isGame``, ``dir``.

    Returns:
        Dict: Franchise records object.
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/franchise", params=params)


# ---------------------------------------------------------------------------
# game
# ---------------------------------------------------------------------------


def nhl_stats_rest_game(lang: str = "en", **filters) -> Dict:
    """Retrieve game-level data.

    Wraps ``GET /{lang}/game``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters.  Common keys: ``cayenneExp``
            (e.g. ``cayenneExp="seasonId=20242025 and gameTypeId=2"``),
            ``sort``, ``start``, ``limit``, ``include``, ``exclude``,
            ``factCayenneExp``, ``isAggregate``, ``isGame``, ``dir``.

    Returns:
        Dict: Game records object with ``data`` array and ``total`` count.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_game
        games = nhl_stats_rest_game(cayenneExp="seasonId=20242025 and gameTypeId=2")
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/game", params=params)


# ---------------------------------------------------------------------------
# glossary
# ---------------------------------------------------------------------------


def nhl_stats_rest_glossary(lang: str = "en", **kwargs) -> Dict:
    """Retrieve the NHL Stats glossary of stat definitions.

    Wraps ``GET /{lang}/glossary``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Glossary payload mapping stat codes to human-readable
        descriptions.
    """
    return _fetch_rest(f"/{lang}/glossary", **kwargs)


# ---------------------------------------------------------------------------
# goalie / report
# ---------------------------------------------------------------------------


def nhl_stats_rest_goalie_report(report: str, lang: str = "en", **filters) -> Dict:
    """Retrieve a goalie statistical report.

    Wraps ``GET /{lang}/goalie/{report}``.

    Args:
        report: Report name.  Common values include ``"summary"``,
            ``"advanced"``, ``"daysRest"``, ``"savesByStrength"``,
            ``"shootout"``, ``"startRelieved"``.  Check
            :func:`nhl_stats_rest_config` for the full enumeration.
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``factCayenneExp``, ``include``,
            ``exclude``, ``sort``, ``dir``, ``start``, ``limit``,
            ``isAggregate``, ``isGame``.

    Returns:
        Dict: Goalie report records object with ``data`` array and ``total``.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_goalie_report
        summ = nhl_stats_rest_goalie_report(
            "summary",
            cayenneExp="seasonId=20242025 and gameTypeId=2",
            sort="wins",
            limit=50,
        )
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/goalie/{report}", params=params)


# ---------------------------------------------------------------------------
# leaders / goalies
# ---------------------------------------------------------------------------


def nhl_stats_rest_leaders_goalies(attribute: str, lang: str = "en", **filters) -> Dict:
    """Retrieve league leaders for a goalie statistical attribute.

    Wraps ``GET /{lang}/leaders/goalies/{attribute}``.

    Args:
        attribute: Stat attribute name (e.g. ``"wins"``, ``"savePct"``,
            ``"goalsAgainstAverage"``, ``"shutouts"``).
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.

    Returns:
        Dict: Goalie leaders payload.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_leaders_goalies
        leaders = nhl_stats_rest_leaders_goalies("wins")
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/leaders/goalies/{attribute}", params=params)


# ---------------------------------------------------------------------------
# leaders / skaters
# ---------------------------------------------------------------------------


def nhl_stats_rest_leaders_skaters(attribute: str, lang: str = "en", **filters) -> Dict:
    """Retrieve league leaders for a skater statistical attribute.

    Wraps ``GET /{lang}/leaders/skaters/{attribute}``.

    Args:
        attribute: Stat attribute name (e.g. ``"goals"``, ``"assists"``,
            ``"points"``, ``"plusMinus"``).
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.

    Returns:
        Dict: Skater leaders payload.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_leaders_skaters
        leaders = nhl_stats_rest_leaders_skaters("points")
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/leaders/skaters/{attribute}", params=params)


# ---------------------------------------------------------------------------
# milestones / goalies
# ---------------------------------------------------------------------------


def nhl_stats_rest_milestones_goalies(lang: str = "en", **filters) -> Dict:
    """Retrieve milestone data for goalies.

    Wraps ``GET /{lang}/milestones/goalies``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``sort``, ``start``, ``limit``,
            ``include``, ``exclude``, ``factCayenneExp``, ``dir``.

    Returns:
        Dict: Goalie milestone records.
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/milestones/goalies", params=params)


# ---------------------------------------------------------------------------
# milestones / skaters
# ---------------------------------------------------------------------------


def nhl_stats_rest_milestones_skaters(lang: str = "en", **filters) -> Dict:
    """Retrieve milestone data for skaters.

    Wraps ``GET /{lang}/milestones/skaters``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``sort``, ``start``, ``limit``,
            ``include``, ``exclude``, ``factCayenneExp``, ``dir``.

    Returns:
        Dict: Skater milestone records.
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/milestones/skaters", params=params)


# ---------------------------------------------------------------------------
# players
# ---------------------------------------------------------------------------


def nhl_stats_rest_players(lang: str = "en", **filters) -> Dict:
    """Retrieve the NHL player registry.

    Wraps ``GET /{lang}/players``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``sort``, ``start``, ``limit``,
            ``include``, ``exclude``, ``factCayenneExp``, ``dir``.

    Returns:
        Dict: Player records with ``data`` array and ``total`` count.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_players
        players = nhl_stats_rest_players(
            cayenneExp="active=1",
            sort="lastName",
            limit=100,
        )
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/players", params=params)


# ---------------------------------------------------------------------------
# season
# ---------------------------------------------------------------------------


def nhl_stats_rest_season(lang: str = "en", **kwargs) -> Dict:
    """Retrieve the list of all NHL seasons.

    Wraps ``GET /{lang}/season``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Season records with start/end dates and season ID codes.
    """
    return _fetch_rest(f"/{lang}/season", **kwargs)


# ---------------------------------------------------------------------------
# shiftcharts
# ---------------------------------------------------------------------------


def nhl_stats_rest_shiftcharts(lang: str = "en", **filters) -> Dict:
    """Retrieve shift-chart data.

    Wraps ``GET /{lang}/shiftcharts``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  The
            primary filter is ``cayenneExp`` — at minimum supply a
            ``gameId`` constraint, e.g.
            ``cayenneExp="gameId=2024020001"``.  Other common keys:
            ``sort``, ``start``, ``limit``, ``include``, ``exclude``,
            ``factCayenneExp``, ``dir``.

    Returns:
        Dict: Shift records (player, team, period, start/end times).

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_shiftcharts
        shifts = nhl_stats_rest_shiftcharts(cayenneExp="gameId=2024020001")
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/shiftcharts", params=params)


# ---------------------------------------------------------------------------
# skater / report
# ---------------------------------------------------------------------------


def nhl_stats_rest_skater_report(report: str, lang: str = "en", **filters) -> Dict:
    """Retrieve a skater statistical report.

    Wraps ``GET /{lang}/skater/{report}``.

    Args:
        report: Report name.  Common values include ``"summary"``,
            ``"advanced"``, ``"powerplay"``, ``"penaltykill"``,
            ``"realtime"``, ``"timeonice"``, ``"faceoffpercentages"``,
            ``"faceoffwins"``, ``"goals"``, ``"penalties"``,
            ``"penaltyShots"``, ``"points"``, ``"bios"``,
            ``"shootout"``, ``"hits"``, ``"blockedShots"``.
            Check :func:`nhl_stats_rest_config` for the full enumeration.
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``factCayenneExp``, ``include``,
            ``exclude``, ``sort``, ``dir``, ``start``, ``limit``,
            ``isAggregate``, ``isGame``.

    Returns:
        Dict: Skater report records object with ``data`` array and ``total``.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_skater_report
        summ = nhl_stats_rest_skater_report(
            "summary",
            cayenneExp="seasonId=20242025 and gameTypeId=2",
            sort="points",
            limit=50,
        )
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/skater/{report}", params=params)


# ---------------------------------------------------------------------------
# team
# ---------------------------------------------------------------------------


def nhl_stats_rest_team(lang: str = "en", **filters) -> Dict:
    """Retrieve the list of all NHL teams.

    Wraps ``GET /{lang}/team``.

    Args:
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``sort``, ``start``, ``limit``,
            ``include``, ``exclude``, ``factCayenneExp``, ``dir``.

    Returns:
        Dict: Team records with IDs, abbreviations, and full names.
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/team", params=params)


# ---------------------------------------------------------------------------
# team / id
# ---------------------------------------------------------------------------


def nhl_stats_rest_team_by_id(team_id: int, lang: str = "en", **kwargs) -> Dict:
    """Retrieve a single team by its numeric ID.

    Wraps ``GET /{lang}/team/id/{id}``.

    Args:
        team_id: NHL team integer ID (e.g. ``10`` for Toronto Maple Leafs).
        lang: Locale code.  Defaults to ``"en"``.

    Returns:
        Dict: Team record for the requested ID.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_team_by_id
        team = nhl_stats_rest_team_by_id(10)
    """
    return _fetch_rest(f"/{lang}/team/id/{team_id}", **kwargs)


# ---------------------------------------------------------------------------
# team / report
# ---------------------------------------------------------------------------


def nhl_stats_rest_team_report(report: str, lang: str = "en", **filters) -> Dict:
    """Retrieve a team statistical report.

    Wraps ``GET /{lang}/team/{report}``.

    Args:
        report: Report name.  Common values include ``"summary"``,
            ``"advanced"``, ``"powerplay"``, ``"penaltykill"``,
            ``"realtime"``, ``"timeonice"``, ``"penaltiesAgainst"``,
            ``"scoringFirst"``, ``"leadingTrailing"``.
            Check :func:`nhl_stats_rest_config` for the full enumeration.
        lang: Locale code.  Defaults to ``"en"``.
        **filters: Optional query parameters forwarded to the API.  Common
            keys: ``cayenneExp``, ``factCayenneExp``, ``include``,
            ``exclude``, ``sort``, ``dir``, ``start``, ``limit``,
            ``isAggregate``, ``isGame``.

    Returns:
        Dict: Team report records object with ``data`` array and ``total``.

    Example::

        from sportsdataverse.nhl import nhl_stats_rest_team_report
        pp = nhl_stats_rest_team_report(
            "powerplay",
            cayenneExp="seasonId=20242025 and gameTypeId=2",
            sort="powerPlayPct",
            limit=32,
        )
    """
    params = _build_params(**filters)
    return _fetch_rest(f"/{lang}/team/{report}", params=params)
