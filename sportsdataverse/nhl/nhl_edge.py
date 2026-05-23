"""sportsdataverse.nhl.nhl_edge — wrappers for NHL EDGE Statcast endpoints.

NHL EDGE is the league's Statcast-equivalent tracking system, exposing puck
and player positional data, shot speed, skating distance/speed, shot-location
heat maps, and zone-time metrics.  Endpoints live under two path families:

* ``/v1/edge/*``  — primary player / team EDGE stats
* ``/v1/cat/edge/*`` — categorized (composite) player views

Endpoint catalog sourced from the OpenAPI spec at
``fastRhockey/data-raw/nhl_api_web_openapi.yaml``.

Conventions
-----------

* **Season strings** — 8-digit, e.g. ``"20242025"`` for the 2024-25 season.
  Pass an 8-digit string or the 4-digit end-year as an int (``2025`` →
  ``"20242025"``).  Pass ``None`` to hit the ``/now`` variant (current season).
* **Game type** — ``1`` = preseason, ``2`` = regular season, ``3`` = playoffs.
* **Positions / strength / sortBy** — string slugs used as-is in the URL path
  (e.g. ``"all"``, ``"5v5"``, ``"maxSpeed"``).
* All functions return ``Dict`` (the raw JSON payload).
"""

from __future__ import annotations

from typing import Dict, Optional, Union

from sportsdataverse.dl_utils import download

_API_WEB_BASE = "https://api-web.nhle.com"


# ---------------------------------------------------------------------------
# Internal helpers (module-local copies — no circular import)
# ---------------------------------------------------------------------------


def _format_nhl_season(season: Union[int, str, None]) -> Optional[str]:
    """Normalize a season identifier to the 8-digit ``YYYYYYYY`` form.

    Accepts:
      * ``None`` — returned unchanged (callers use the ``/now`` variants).
      * An 8-digit string (``"20242025"``) — returned unchanged.
      * A 4-digit int or string representing the **end year** (``2025`` or
        ``"2025"``) — returned as ``"20242025"``.
    """
    if season is None:
        return None
    s = str(season)
    if len(s) == 8 and s.isdigit():
        return s
    if len(s) == 4 and s.isdigit():
        end_year = int(s)
        return f"{end_year - 1}{end_year}"
    raise ValueError(
        f"Unrecognized NHL season identifier {season!r}; "
        "expected 8-digit string (e.g. '20242025') or 4-digit end year (2025).",
    )


def _fetch(path: str, **kwargs) -> Dict:
    """Internal ``download() → .json()`` helper.  Returns ``{}`` on failure."""
    url = f"{_API_WEB_BASE}{path}"
    resp = download(url=url, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Skater EDGE functions
# ---------------------------------------------------------------------------


def nhl_edge_skater_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE detail stats for a single skater.

    Wraps ``GET /v1/edge/skater-detail/{playerId}/now`` or
    ``/v1/edge/skater-detail/{playerId}/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            current season (``/now``).
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater detail payload.

    Example::

        from sportsdataverse.nhl import nhl_edge_skater_detail
        nhl_edge_skater_detail(8480801)
    """
    if season is None:
        return _fetch(f"/v1/edge/skater-detail/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/edge/skater-detail/{player_id}/{s}/{game_type}", **kwargs)


def nhl_edge_skater_comparison(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE comparison data for a single skater.

    Wraps ``GET /v1/edge/skater-comparison/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater comparison payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/skater-comparison/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-comparison/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_skater_shot_location_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE shot-location detail for a single skater.

    Wraps ``GET /v1/edge/skater-shot-location-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater shot-location detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-shot-location-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-shot-location-detail/{player_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_shot_location_top_10(
    position: str,
    category: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 skaters for a shot-location category.

    Wraps ``GET /v1/edge/skater-shot-location-top-10/{position}/{category}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        position (str): Position slug, e.g. ``"all"``, ``"C"``, ``"L"``,
            ``"R"``, ``"D"``.
        category (str): Shot-location category slug (e.g. ``"shotAttempts"``).
        sort_by (str): Sort metric slug (e.g. ``"goals"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 skater shot-location payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-shot-location-top-10/{position}/{category}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_shot_speed_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE shot-speed detail for a single skater.

    Wraps ``GET /v1/edge/skater-shot-speed-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater shot-speed detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-shot-speed-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-shot-speed-detail/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_skater_shot_speed_top_10(
    positions: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 skaters by shot speed.

    Wraps ``GET /v1/edge/skater-shot-speed-top-10/{positions}/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        sort_by (str): Sort metric slug (e.g. ``"maxSpeed"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 skater shot-speed payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-shot-speed-top-10/{positions}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_skating_distance_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE skating-distance detail for a single skater.

    Wraps ``GET /v1/edge/skater-skating-distance-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater skating-distance detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-skating-distance-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-skating-distance-detail/{player_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_skating_speed_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE skating-speed detail for a single skater.

    Wraps ``GET /v1/edge/skater-skating-speed-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater skating-speed detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-skating-speed-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-skating-speed-detail/{player_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_speed_top_10(
    positions: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 skaters by skating speed.

    Wraps ``GET /v1/edge/skater-speed-top-10/{positions}/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        sort_by (str): Sort metric slug (e.g. ``"maxSpeed"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 skater speed payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-speed-top-10/{positions}/{sort_by}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-speed-top-10/{positions}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_distance_top_10(
    positions: str,
    strength: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 skaters by skating distance.

    Wraps ``GET /v1/edge/skater-distance-top-10/{positions}/{strength}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        strength (str): Strength state slug (e.g. ``"all"``, ``"5v5"``).
        sort_by (str): Sort metric slug (e.g. ``"totalDistance"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 skater distance payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-distance-top-10/{positions}/{strength}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_zone_time(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE zone-time detail for a single skater.

    Wraps ``GET /v1/edge/skater-zone-time/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater zone-time payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/skater-zone-time/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-zone-time/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_skater_zone_time_top_10(
    positions: str,
    strength: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 skaters by zone time.

    Wraps ``GET /v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        strength (str): Strength state slug (e.g. ``"all"``, ``"5v5"``).
        sort_by (str): Sort metric slug (e.g. ``"offZoneTime"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 skater zone-time payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/skater-zone-time-top-10/{positions}/{strength}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_skater_landing(
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE skater landing page (summary across all skaters).

    Wraps ``GET /v1/edge/skater-landing/now`` or
    ``/v1/edge/skater-landing/{season}/{gameType}``.

    Args:
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE skater landing payload.
    """
    if season is None:
        return _fetch("/v1/edge/skater-landing/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/edge/skater-landing/{s}/{game_type}", **kwargs)


# ---------------------------------------------------------------------------
# Goalie EDGE functions
# ---------------------------------------------------------------------------


def nhl_edge_goalie_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE detail stats for a single goalie.

    Wraps ``GET /v1/edge/goalie-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie detail payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/goalie-detail/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-detail/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_goalie_5v5_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE 5-on-5 detail stats for a single goalie.

    Wraps ``GET /v1/edge/goalie-5v5-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie 5v5 detail payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/goalie-5v5-detail/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-5v5-detail/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_goalie_5v5_top_10(
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 goalies by 5-on-5 metrics.

    Wraps ``GET /v1/edge/goalie-5v5-top-10/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        sort_by (str): Sort metric slug (e.g. ``"savePctg"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 goalie 5v5 payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/goalie-5v5-top-10/{sort_by}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-5v5-top-10/{sort_by}/{s}/{game_type}", **kwargs
    )


def nhl_edge_goalie_comparison(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE comparison data for a single goalie.

    Wraps ``GET /v1/edge/goalie-comparison/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie comparison payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/goalie-comparison/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-comparison/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_goalie_save_percentage_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE save-percentage detail for a single goalie.

    Wraps ``GET /v1/edge/goalie-save-percentage-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie save-percentage detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/goalie-save-percentage-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-save-percentage-detail/{player_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_goalie_edge_save_pctg_top_10(
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 goalies by save-percentage.

    Wraps ``GET /v1/edge/goalie-edge-save-pctg-top-10/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        sort_by (str): Sort metric slug (e.g. ``"savePctgAboveExpected"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 goalie save-percentage payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-edge-save-pctg-top-10/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_goalie_shot_location_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE shot-location detail for a single goalie.

    Wraps ``GET /v1/edge/goalie-shot-location-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie shot-location detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/goalie-shot-location-detail/{player_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-shot-location-detail/{player_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_goalie_shot_location_top_10(
    category: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 goalies for a shot-location category.

    Wraps ``GET /v1/edge/goalie-shot-location-top-10/{category}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        category (str): Shot-location category slug (e.g. ``"shotAttempts"``).
        sort_by (str): Sort metric slug (e.g. ``"savePctg"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 goalie shot-location payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/goalie-shot-location-top-10/{category}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_goalie_landing(
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE goalie landing page (summary across all goalies).

    Wraps ``GET /v1/edge/goalie-landing/now`` or
    ``/v1/edge/goalie-landing/{season}/{gameType}``.

    Args:
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE goalie landing payload.
    """
    if season is None:
        return _fetch("/v1/edge/goalie-landing/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/edge/goalie-landing/{s}/{game_type}", **kwargs)


# ---------------------------------------------------------------------------
# Team EDGE functions
# ---------------------------------------------------------------------------


def nhl_edge_team_detail(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE detail stats for a single team.

    Wraps ``GET /v1/edge/team-detail/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team detail payload.
    """
    if season is None:
        return _fetch(f"/v1/edge/team-detail/{team_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/edge/team-detail/{team_id}/{s}/{game_type}", **kwargs)


def nhl_edge_team_landing(
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE team landing page (summary across all teams).

    Wraps ``GET /v1/edge/team-landing/now`` or
    ``/v1/edge/team-landing/{season}/{gameType}``.

    Args:
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team landing payload.
    """
    if season is None:
        return _fetch("/v1/edge/team-landing/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/edge/team-landing/{s}/{game_type}", **kwargs)


def nhl_edge_team_shot_location_detail(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE shot-location detail for a single team.

    Wraps ``GET /v1/edge/team-shot-location-detail/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team shot-location detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-shot-location-detail/{team_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-shot-location-detail/{team_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_team_shot_location_top_10(
    position: str,
    category: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 teams for a shot-location category.

    Wraps ``GET /v1/edge/team-shot-location-top-10/{position}/{category}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        position (str): Position context slug (e.g. ``"all"``).
        category (str): Shot-location category slug (e.g. ``"shotAttempts"``).
        sort_by (str): Sort metric slug (e.g. ``"goals"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 team shot-location payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-shot-location-top-10/{position}/{category}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_team_shot_speed_detail(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE shot-speed detail for a single team.

    Wraps ``GET /v1/edge/team-shot-speed-detail/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team shot-speed detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-shot-speed-detail/{team_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-shot-speed-detail/{team_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_team_skating_distance_detail(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE skating-distance detail for a single team.

    Wraps ``GET /v1/edge/team-skating-distance-detail/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team skating-distance detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-skating-distance-detail/{team_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-skating-distance-detail/{team_id}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_team_skating_distance_top_10(
    positions: str,
    strength: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 teams by skating distance.

    Wraps ``GET /v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sortBy}/now``
    or ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        strength (str): Strength state slug (e.g. ``"all"``, ``"5v5"``).
        sort_by (str): Sort metric slug (e.g. ``"totalDistance"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 team skating-distance payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-skating-distance-top-10/{positions}/{strength}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_team_skating_speed_detail(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE skating-speed detail for a single team.

    Wraps ``GET /v1/edge/team-skating-speed-detail/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team skating-speed detail payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-skating-speed-detail/{team_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-skating-speed-detail/{team_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_team_skating_speed_top_10(
    positions: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 teams by skating speed.

    Wraps ``GET /v1/edge/team-skating-speed-top-10/{positions}/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        positions (str): Position filter slug (e.g. ``"all"``, ``"F"``,
            ``"D"``).
        sort_by (str): Sort metric slug (e.g. ``"maxSpeed"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 team skating-speed payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-skating-speed-top-10/{positions}/{sort_by}/now",
            **kwargs,
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-skating-speed-top-10/{positions}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


def nhl_edge_team_zone_time_details(
    team_id: Union[int, str],
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull EDGE zone-time details for a single team.

    Wraps ``GET /v1/edge/team-zone-time-details/{teamId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        team_id: NHL team id (integer) or 3-letter abbreviation string.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE team zone-time details payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-zone-time-details/{team_id}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-zone-time-details/{team_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_team_zone_time_top_10(
    strength: str,
    sort_by: str,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull the EDGE top-10 teams by zone time.

    Wraps ``GET /v1/edge/team-zone-time-top-10/{strength}/{sortBy}/now`` or
    ``/{season}/{gameType}``.

    Args:
        strength (str): Strength state slug (e.g. ``"all"``, ``"5v5"``).
        sort_by (str): Sort metric slug (e.g. ``"offZoneTime"``).
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: EDGE top-10 team zone-time payload.
    """
    if season is None:
        return _fetch(
            f"/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/now", **kwargs
        )
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/edge/team-zone-time-top-10/{strength}/{sort_by}/{s}/{game_type}",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Cat (categorized) EDGE functions
# ---------------------------------------------------------------------------


def nhl_edge_cat_skater_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull categorized (cat) EDGE detail stats for a single skater.

    Wraps ``GET /v1/cat/edge/skater-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.  The ``/cat/edge/`` family returns a composite
    view that groups metrics into named categories, useful for radar/spider
    chart visualizations on NHL.com.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: Cat EDGE skater detail payload.

    Example::

        from sportsdataverse.nhl import nhl_edge_cat_skater_detail
        nhl_edge_cat_skater_detail(8480801)
    """
    if season is None:
        return _fetch(f"/v1/cat/edge/skater-detail/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/cat/edge/skater-detail/{player_id}/{s}/{game_type}", **kwargs
    )


def nhl_edge_cat_goalie_detail(
    player_id: int,
    season: Union[int, str, None] = None,
    game_type: int = 2,
    **kwargs,
) -> Dict:
    """Pull categorized (cat) EDGE detail stats for a single goalie.

    Wraps ``GET /v1/cat/edge/goalie-detail/{playerId}/now`` or
    ``/{season}/{gameType}``.

    Args:
        player_id (int): NHL player id.
        season: 8-digit season string or 4-digit end-year int.  ``None`` →
            ``/now``.
        game_type (int): 1=pre, 2=reg, 3=playoffs.  Default 2.

    Returns:
        Dict: Cat EDGE goalie detail payload.
    """
    if season is None:
        return _fetch(f"/v1/cat/edge/goalie-detail/{player_id}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(
        f"/v1/cat/edge/goalie-detail/{player_id}/{s}/{game_type}", **kwargs
    )
