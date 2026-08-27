"""Shared ESPN core-v2 athlete *season* stats scraper (all sports/leagues).

Single source of truth behind every ``espn_<league>_player_stats`` wrapper.
Pulls a player's **season** statistics from ESPN's
``sports.core.api.espn.com`` (core-v2) graph and returns **one wide,
rectangular row** combining

1. athlete identity / biographical metadata,
2. the season stat line, pivoted wide as ``{category}_{stat}`` columns
   (e.g. ``offensive_points``, ``passing_passing_yards``,
   ``batting_home_runs``), and
3. the player's team identity (``team_*`` columns).

The ``splits.categories[].stats[]`` shape and the athlete/team ``$ref``
graph are identical across ESPN sports, so a single parser serves
basketball, football, baseball, and hockey -- only the ``{sport}`` segment
of the URL changes.

Endpoint family (core-v2)::

    .../sports/{sport}/leagues/{league}/seasons/{season}/types/{type}/athletes/{id}/statistics/{totals}
    .../sports/{sport}/leagues/{league}/seasons/{season}/athletes/{id}      # athlete + team $ref
    .../sports/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}    # dereferenced team

This is intentionally a *different* endpoint from the web-common-v3
``/athletes/{id}/stats`` surface (the ``espn_<league>_player_stats_v3``
wrappers), which returns the richer multi-category payload.
``player_stats`` = season line (core-v2); ``player_stats_v3`` =
comprehensive (web-v3). The split matches wehoop/hoopR/cfbfastR.

The statistics call is authoritative: a 404 there means no season line for
that athlete/season and raises. The athlete + team dereferences are
best-effort -- a hiccup leaves the corresponding ``*_`` columns null rather
than failing the whole call.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore

_CORE_V2_HOST: str = "https://sports.core.api.espn.com/v2/sports"

# Athlete-metadata fields lifted from the core-v2 ``/athletes/{id}`` node,
# mapped to their snake_case output column. ``id``/``uid``/``guid``/``type``
# are namespaced ``athlete_*`` to disambiguate from the team block.
_ATHLETE_FIELD_MAP: dict[str, str] = {
    "id": "athlete_id",
    "uid": "athlete_uid",
    "guid": "athlete_guid",
    "type": "athlete_type",
    "firstName": "first_name",
    "lastName": "last_name",
    "fullName": "full_name",
    "displayName": "display_name",
    "shortName": "short_name",
    "weight": "weight",
    "displayWeight": "display_weight",
    "height": "height",
    "displayHeight": "display_height",
    "age": "age",
    "dateOfBirth": "date_of_birth",
    "jersey": "jersey",
    "slug": "slug",
    "active": "active",
}

# Team-metadata fields lifted from the dereferenced ``/teams/{id}`` node,
# mapped to their ``team_``-prefixed output column.
_TEAM_FIELD_MAP: dict[str, str] = {
    "id": "team_id",
    "uid": "team_uid",
    "guid": "team_guid",
    "slug": "team_slug",
    "location": "team_location",
    "name": "team_name",
    "abbreviation": "team_abbreviation",
    "displayName": "team_display_name",
    "shortDisplayName": "team_short_display_name",
    "color": "team_color",
    "alternateColor": "team_alternate_color",
    "isActive": "team_is_active",
}

# Columns coerced to integer when present and parseable.
_INT_COLUMNS: tuple[str, ...] = (
    "athlete_id",
    "team_id",
    "position_id",
    "status_id",
    "season",
)


def _espn_player_stats(
    sport: str,
    league: str,
    athlete_id: int,
    season: int,
    *,
    season_type: str = "regular",
    total: bool = False,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Shared core-v2 season-stats implementation for every ``espn_<league>_player_stats``.

    Builds the core-v2 statistics URL for the supplied ``sport`` + ``league``
    slugs, fetches the season stat line, dereferences the athlete + team
    nodes, and binds everything into a single wide row.

    Args:
        sport: ESPN sport slug (``"basketball"``, ``"football"``,
            ``"baseball"``, ``"hockey"``).
        league: ESPN league slug (``"nba"``, ``"wnba"``,
            ``"mens-college-basketball"``, ``"womens-college-basketball"``,
            ``"nfl"``, ``"college-football"``, ``"mlb"``, ``"nhl"``).
        athlete_id: ESPN athlete identifier.
        season: Season year.
        season_type: ``"regular"`` (type 2) or ``"postseason"`` (type 3).
        total: When True, requests the ``/statistics/0`` totals variant;
            ESPN currently returns the same payload either way (forward-compat).
        raw: If True, returns the parsed core-v2 *statistics* JSON dict.
        return_as_pandas: If True, returns a pandas DataFrame; else polars.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        A single-row wide DataFrame (polars by default). When ``raw=True``
        returns the raw statistics JSON ``dict``.

    Raises:
        ValueError: ``season_type`` is not ``"regular"``/``"postseason"``.
        sportsdataverse.errors.NoDataError: ESPN returned 404 for the
            statistics node (no season line for that athlete/season).
    """
    st = season_type.strip().lower()
    if st not in ("regular", "postseason"):
        raise ValueError(f"season_type must be 'regular' or 'postseason', got {season_type!r}")
    s_type = 3 if st == "postseason" else 2
    totals = "0" if total else ""

    base = f"{_CORE_V2_HOST}/{sport}/leagues/{league}/seasons"
    stats_url = f"{base}/{season}/types/{s_type}/athletes/{athlete_id}/statistics/{totals}"
    athlete_url = f"{base}/{season}/athletes/{athlete_id}"

    # Statistics node is authoritative: a 404 here raises (NoDataError).
    stats_payload: dict[str, Any] = download(stats_url, **kwargs).json()

    if raw:
        return stats_payload

    row: dict[str, Any] = {
        "season": season,
        "season_type": st,
        "total": total,
    }

    # Athlete + team metadata are best-effort: failures degrade to nulls.
    athlete_payload = _safe_json(athlete_url, **kwargs)
    row.update(_extract_athlete_meta(athlete_payload))

    team_ref = ((athlete_payload or {}).get("team") or {}).get("$ref")
    team_payload = _safe_json(team_ref, **kwargs) if team_ref else None
    team_meta = _extract_team_meta(team_payload)

    # Wide stat columns sit between athlete identity and team identity so the
    # frame reads athlete -> stats -> team.
    row.update(_wide_stats(stats_payload))
    row.update(team_meta)

    # Ensure athlete_id is always populated even if the athlete deref failed.
    if row.get("athlete_id") is None:
        row["athlete_id"] = athlete_id

    for col in _INT_COLUMNS:
        if col in row:
            row[col] = _coerce_int(row[col])

    frame = pl.DataFrame({k: [v] for k, v in row.items()})
    if return_as_pandas:
        return frame.to_pandas()
    return frame


def _safe_json(url: str | None, **kwargs: Any) -> dict[str, Any] | None:
    """Best-effort GET + ``.json()``; returns ``None`` on any failure."""
    if not url:
        return None
    try:
        return download(url, **kwargs).json()
    except Exception:
        return None


def _extract_athlete_meta(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Lift curated athlete-identity fields into snake_case output columns."""
    out: dict[str, Any] = {col: None for col in _ATHLETE_FIELD_MAP.values()}
    out.update(
        {
            "position_id": None,
            "position_name": None,
            "position_display_name": None,
            "position_abbreviation": None,
            "college_name": None,
            "status_id": None,
            "status_name": None,
        },
    )
    if not isinstance(payload, dict):
        return out

    for src, dest in _ATHLETE_FIELD_MAP.items():
        if src in payload and not _is_ref_only(payload[src]):
            out[dest] = payload[src]

    position = payload.get("position")
    if isinstance(position, dict):
        out["position_id"] = position.get("id")
        out["position_name"] = position.get("name")
        out["position_display_name"] = position.get("displayName")
        out["position_abbreviation"] = position.get("abbreviation")

    college = payload.get("college")
    if isinstance(college, dict):
        out["college_name"] = college.get("name")

    status = payload.get("status")
    if isinstance(status, dict):
        out["status_id"] = status.get("id")
        out["status_name"] = status.get("name") or status.get("type")

    return out


def _extract_team_meta(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Lift curated team-identity fields into ``team_``-prefixed columns."""
    out: dict[str, Any] = {col: None for col in _TEAM_FIELD_MAP.values()}
    out["team_logo_href"] = None
    if not isinstance(payload, dict):
        return out

    for src, dest in _TEAM_FIELD_MAP.items():
        if src in payload and not _is_ref_only(payload[src]):
            out[dest] = payload[src]

    logos = payload.get("logos")
    if isinstance(logos, list) and logos and isinstance(logos[0], dict):
        out["team_logo_href"] = logos[0].get("href")

    return out


def _wide_stats(stats_payload: dict[str, Any]) -> dict[str, Any]:
    """Pivot ``splits.categories[].stats[]`` into ``{category}_{stat}`` columns.

    Each stat's numeric ``value`` becomes one column named
    ``underscore(f"{category_name}_{stat_name}")``. Composite / non-numeric
    values fall back to their string ``displayValue``.
    """
    out: dict[str, Any] = {}
    splits = stats_payload.get("splits")
    if not isinstance(splits, dict):
        return out
    categories = splits.get("categories")
    if not isinstance(categories, list):
        return out

    for cat in categories:
        if not isinstance(cat, dict):
            continue
        cat_name = cat.get("name") or cat.get("displayName") or "category"
        stats = cat.get("stats")
        if not isinstance(stats, list):
            continue
        for stat in stats:
            if not isinstance(stat, dict):
                continue
            stat_name = stat.get("name") or stat.get("abbreviation")
            if not stat_name:
                continue
            col = underscore(f"{cat_name}_{stat_name}")
            value = stat.get("value")
            num = _coerce_float(value)
            out[col] = num if num is not None else stat.get("displayValue")
    return out


def _is_ref_only(v: Any) -> bool:
    """True if ``v`` is a bare ``{"$ref": ...}`` link with no inline payload."""
    return isinstance(v, dict) and set(v.keys()) == {"$ref"}


def _coerce_float(v: Any) -> float | None:
    """Coerce ``v`` to ``float``; ``None`` if not convertible (e.g. "7.8-15.7")."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _coerce_int(v: Any) -> int | None:
    """Coerce ``v`` to ``int``; ``None`` if not convertible."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return None
    return None
