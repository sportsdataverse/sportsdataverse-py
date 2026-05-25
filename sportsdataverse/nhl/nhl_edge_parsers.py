"""sportsdataverse.nhl.nhl_edge_parsers — polars parsers for NHL EDGE payloads.

**Documentation**:

* NHL EDGE parser deep-dive: https://py.sportsdataverse.org/docs/nhl/edge-parsers
* NHL EDGE endpoint reference: https://py.sportsdataverse.org/docs/nhl/edge
* Parsers overview: https://py.sportsdataverse.org/docs/parsers/

NHL EDGE returns position-tracking / shot-speed / zone-time data via
``api-web.nhle.com/v1/edge/*``. The OpenAPI spec
(``fastRhockey/data-raw/nhl_api_web_openapi.yaml``) declares every response
as ``type: object`` with no inner schema, so this module's parsers are
**defensive by design** — they walk through a sequence of likely top-level
keys, fall back to ``pandas.json_normalize`` on whatever shape comes back,
and return a zero-row polars frame rather than raising when the payload is
empty.

Parser families
---------------

The 35 EDGE endpoints in :mod:`sportsdataverse.nhl.nhl_edge` cluster into
four primary shape families plus a sub-frame family for unrolling nested
lists inside detail payloads:

* **Leaderboards** (``*_top_10``) — list of player/team rows with shared
  schema. Parser: :func:`parse_edge_top10`. *(Note: all ``*_top_10``
  URL paths return 404 as of 2026-05-23 — the parser is kept for
  forward-compat if NHL restores the surface.)*
* **Detail pages** (``*_detail``, ``*_5v5_detail``, ``*_comparison``) —
  multi-section single-entity payload. Parser: :func:`parse_edge_detail`.
* **Shot-location** (``*_shot_location_*``) — strike-zone–style heat map
  with one cell per zone (17-cell grid + 4-12 row aggregate).
  Parser: :func:`parse_edge_shot_location`.
* **Zone-time** (``*_zone_time_*``) — possession share by zone (offensive,
  defensive, neutral; with strength-state splits where available).
  Parser: :func:`parse_edge_zone_time`.
* **Sub-frame parsers** for the nested lists that ``parse_edge_detail``
  deliberately stringifies (to keep the output one row per call):

  - :func:`parse_edge_sog_details` — 17-cell SOG / save grid from
    skater-detail, team-detail, goalie-detail, ``*-shot-location-detail``.
  - :func:`parse_edge_sog_summary` — 4-row location-code aggregate from
    the same endpoints.
  - :func:`parse_edge_hardest_shots` — 10-row hardest-shots list from
    ``skater-shot-speed-detail``.

Endpoints not in those families pass through as raw ``Dict``; call
:func:`parse_edge_payload` for a best-effort flatten.

Usage
-----

    from sportsdataverse.nhl import nhl_edge_skater_shot_speed_top_10
    from sportsdataverse.nhl.nhl_edge_parsers import parse_edge_top10

    raw = nhl_edge_skater_shot_speed_top_10("all", "maxSpeed")
    df = parse_edge_top10(raw)
    print(df.shape)
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


# ---------------------------------------------------------------------------
# Module-local helpers (mirror _common_espn_parsers conventions)
# ---------------------------------------------------------------------------


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Snake-case all column names in a pandas frame."""
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    """Return ``df`` as pandas (when ``return_as_pandas``) or polars."""
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        # Polars rejects mixed-typed object columns sometimes; coerce to str
        # for any leftover object dtype.
        df2 = df.copy()
        for col in df2.select_dtypes(include="object").columns:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _empty_frame(return_as_pandas: bool = False):
    """Zero-row, zero-column frame in the requested flavour."""
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


# ---------------------------------------------------------------------------
# Family 1 — leaderboards (`*_top_10`)
# ---------------------------------------------------------------------------


# Common top-level keys observed on EDGE leaderboard payloads. Tried in
# order until one matches a non-empty list.
_TOP10_LIST_KEYS = (
    "top10",
    "leaderboard",
    "leaders",
    "players",
    "skaters",
    "goalies",
    "teams",
    "data",
    "items",
)


def parse_edge_top10(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an EDGE leaderboard (``*_top_10``) response into a tidy frame.

    Tries a sequence of likely top-level keys (``"top10"``, ``"leaderboard"``,
    ``"players"``, ``"skaters"``, ``"goalies"``, ``"teams"``, ``"data"``,
    ``"items"``) — the first that resolves to a non-empty list is the row
    source.  Flattens with :func:`pandas.json_normalize`, snake-cases the
    columns, and converts to polars.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*_top_10`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per ranked entity. Returns
        a zero-row frame when ``payload`` is empty or no candidate key
        resolves to a non-empty list.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows: Optional[list] = None
    for key in _TOP10_LIST_KEYS:
        candidate = payload.get(key)
        if isinstance(candidate, list) and candidate:
            rows = candidate
            break
    if rows is None:
        # Last-ditch: if the payload itself is wrapped one level deep, look
        # for the first list-valued attribute that contains dicts.
        for val in payload.values():
            if isinstance(val, list) and val and isinstance(val[0], dict):
                rows = val
                break
    if not rows:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(rows, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# Family 2 — single-entity detail pages (`*_detail`, `*_comparison`)
# ---------------------------------------------------------------------------


def parse_edge_detail(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an EDGE detail / comparison payload into a single-row frame.

    Flattens the entire payload one level deep via
    :func:`pandas.json_normalize`. List-valued attributes (e.g. season-by-
    season splits, shot-location grids) are kept as their string
    representation so the result remains one row per detail call.  Use
    :func:`parse_edge_shot_location` / :func:`parse_edge_zone_time` when
    you need the nested structures unrolled into long-form rows.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*_detail`` /
            ``nhl_edge_*_comparison`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row summarising the detail
        payload, columns auto-flattened. Returns a zero-row frame when
        ``payload`` is empty.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(payload, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    # Stringify any leftover list columns so polars can consume the frame.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# Family 3 — shot-location heat maps
# ---------------------------------------------------------------------------


# Keys that host the per-zone heat-map grid in live EDGE payloads.
#
# Empirically verified 2026-05-23 against ``api-web.nhle.com/v1/edge/*``:
#
# * ``shotLocationDetails`` — 17-cell granular grid (``area`` key per row).
#   Appears in team-shot-location-detail, goalie-shot-location-detail,
#   goalie-detail.
# * ``sogDetails`` — 17-cell granular grid for skaters / teams.
#   Appears in skater-detail, team-detail.
# * ``shotLocationTotals`` — 4-12 row aggregate by location code.
#   Appears in team-shot-location-detail, goalie-shot-location-detail.
# * ``shotLocationSummary`` — 4-row aggregate (``locationCode`` key per row).
#   Appears in goalie-detail.
# * ``sogSummary`` — 4-row aggregate for skater-detail, team-detail.
#
# Keys are tried in priority order: most granular (17-cell) first, then
# the 4-12 row aggregates as a fallback when only the summary is shipped.
_SHOT_LOCATION_KEYS = (
    "shotLocationDetails",
    "sogDetails",
    "shotLocationTotals",
    "shotLocationSummary",
    "sogSummary",
)


def parse_edge_shot_location(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Parse an EDGE shot-location heat map into long-form rows.

    Picks the most granular zone list available in the payload, in the
    priority order ``shotLocationDetails`` → ``sogDetails`` →
    ``shotLocationTotals`` → ``shotLocationSummary`` → ``sogSummary``.
    Each zone becomes one row in the output frame.

    Skater / team detail payloads carry **both** a granular 17-cell grid
    (``sogDetails`` / ``shotLocationDetails``) and a 4-row aggregate
    (``sogSummary`` / ``shotLocationSummary``). When both are present,
    only the granular grid is returned — call :func:`parse_edge_sog_summary`
    for the aggregate view.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*_shot_location_*``
            wrapper, or from any ``*_detail`` wrapper that ships a
            shot-location grid inline.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per zone cell. Returns
        a zero-row frame when no recognized zone list is found.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)

    # Direct shape: one of _SHOT_LOCATION_KEYS resolves to the zone list.
    for key in _SHOT_LOCATION_KEYS:
        zones = payload.get(key)
        if isinstance(zones, list) and zones:
            try:
                df = pd.json_normalize(zones, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                return _empty_frame(return_as_pandas)

    # Nested shape: dict-of-sections, each containing one of _SHOT_LOCATION_KEYS.
    parts = []
    for section, contents in payload.items():
        if not isinstance(contents, dict):
            continue
        for key in _SHOT_LOCATION_KEYS:
            zones = contents.get(key)
            if isinstance(zones, list) and zones:
                try:
                    sub = pd.json_normalize(zones, sep="_")
                    sub.insert(0, "section", section)
                    parts.append(sub)
                except Exception:
                    continue
                break
    if not parts:
        return _empty_frame(return_as_pandas)
    df = pd.concat(parts, ignore_index=True, sort=False)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# Family 4 — zone-time / possession by zone
# ---------------------------------------------------------------------------


# Zone-time payloads contain one of:
#   - ``zoneTimeDetails`` list[N] keyed by ``strengthCode`` (live shape on
#     ``/edge/skater-zone-time/{id}/{season}/{gameType}``; 4 rows per skater).
#   - ``zoneTimeDetails`` dict (live shape on ``/edge/skater-detail`` and
#     ``/edge/team-detail``; flat percentages, no strength split).
#   - ``zoneStarts`` dict (live shape on ``/edge/skater-zone-time``;
#     flat offensive/neutral/defensive start percentages).
#
# Keys are tried in priority order: list-valued zoneTimeDetails first (multi
# strength splits — richer), then dict-valued zoneTimeDetails (flatten to one
# row), then zoneStarts.
_ZONE_TIME_KEYS = (
    "zoneTimeDetails",
    "zoneTime",
    "zoneTimes",
    "zoneStarts",
    "zones",
    "byZone",
    "byStrength",
    "data",
)


def parse_edge_zone_time(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Parse an EDGE zone-time payload into long-form rows.

    Each zone (offensive / defensive / neutral) or strength-state row
    becomes one row in the output frame. Falls back to flattening the
    entire payload as a single row when no recognized zone list is found.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*_zone_time_*`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas), zero-row if the payload is empty.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)

    for key in _ZONE_TIME_KEYS:
        candidate = payload.get(key)
        if isinstance(candidate, list) and candidate:
            # Multi-row case: strength-state splits (e.g. zoneTimeDetails on
            # skater-zone-time returns 4 rows keyed by ``strengthCode``).
            try:
                df = pd.json_normalize(candidate, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                continue
        if isinstance(candidate, dict) and candidate:
            # Single-row case: flat metric dict (e.g. zoneTimeDetails on
            # skater-detail is a flat dict of offensiveZonePctg,
            # offensiveZonePercentile, offensiveZoneLeagueAvg, etc.;
            # zoneStarts on skater-zone-time is the same shape).
            try:
                df = pd.json_normalize(candidate, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                continue

    # No zone-shaped key found — fall back to a single-row flatten.
    return parse_edge_detail(payload, return_as_pandas=return_as_pandas)


# ---------------------------------------------------------------------------
# Family 5 — sub-frame parsers for detail-page nested lists
# ---------------------------------------------------------------------------
#
# Detail endpoints (``*-detail``) ship rich nested lists *alongside* the
# single-row entity summary. ``parse_edge_detail`` deliberately stringifies
# those lists to keep the output one row per call; these dedicated parsers
# unroll them into long-form frames.


def parse_edge_sog_details(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the 17-cell shots-on-goal heat map from a detail payload.

    Looks for ``sogDetails`` (skater-detail, team-detail) or
    ``shotLocationDetails`` (goalie-detail, *-shot-location-detail).
    Returns one row per zone cell with the ``area`` column plus shot /
    goal / save metrics depending on the entity type.

    Args:
        payload: Raw JSON dict from any ``*_detail`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas), zero-row if the payload lacks both
        ``sogDetails`` and ``shotLocationDetails``.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    for key in ("sogDetails", "shotLocationDetails"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            try:
                df = pd.json_normalize(rows, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                return _empty_frame(return_as_pandas)
    return _empty_frame(return_as_pandas)


def parse_edge_sog_summary(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the 4-row shots-on-goal location aggregate from a detail payload.

    Looks for ``sogSummary`` (skater-detail, team-detail),
    ``shotLocationSummary`` (goalie-detail), or ``shotLocationTotals``
    (team-shot-location-detail, goalie-shot-location-detail). Returns
    one row per location code with shot / goal / save metrics.

    Args:
        payload: Raw JSON dict from any ``*_detail`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas), zero-row if no aggregate is found.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    for key in ("sogSummary", "shotLocationSummary", "shotLocationTotals"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            try:
                df = pd.json_normalize(rows, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                return _empty_frame(return_as_pandas)
    return _empty_frame(return_as_pandas)


def parse_edge_hardest_shots(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the hardest-shots list from ``skater-shot-speed-detail``.

    The endpoint ships ``hardestShots: list[10]`` with per-shot metadata
    (``gameDate``, ``shotSpeed``, ``timeInPeriod``, etc.). This parser
    returns those 10 rows as a tidy frame.

    Args:
        payload: Raw JSON dict from ``nhl_edge_skater_shot_speed_detail``.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per hardest shot; zero
        rows when ``hardestShots`` is missing or empty.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    shots = payload.get("hardestShots")
    if not isinstance(shots, list) or not shots:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(shots, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# Generic fallback
# ---------------------------------------------------------------------------


def parse_edge_payload(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Generic best-effort flatten for any EDGE payload shape.

    Picks the largest list of dicts inside the payload (most likely to be
    the "interesting" row source) and flattens it; falls back to flattening
    the payload itself as a single row.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas). Zero-row when payload is empty.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)

    # Find the largest list-of-dicts to use as rows.
    best_key = None
    best_len = 0
    for key, val in payload.items():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            if len(val) > best_len:
                best_len = len(val)
                best_key = key

    if best_key is not None:
        try:
            df = pd.json_normalize(payload[best_key], sep="_")
            df = _snake_columns(df)
            return _to_output(df, return_as_pandas)
        except Exception:
            pass

    # Single-row fallback.
    return parse_edge_detail(payload, return_as_pandas=return_as_pandas)


# ---------------------------------------------------------------------------
# Endpoint → parser registry
# ---------------------------------------------------------------------------


# Maps an NHL EDGE wrapper name to the parser family it belongs to.
# ``parser_for_edge(fn_name)`` returns the registered parser or
# :func:`parse_edge_payload` (the generic fallback).
#
# Live status (verified 2026-05-23 against ``api-web.nhle.com/v1/edge/*``):
#
# * **Detail / shot-location / zone-time endpoints** all return 200 with
#   stable schemas — registered with their tightened family parser.
# * **``*_top_10`` endpoints all 404** — the URL pattern in the OpenAPI
#   spec at ``fastRhockey/data-raw/nhl_api_web_openapi.yaml`` is dead.
#   Their parser still defaults to :func:`parse_edge_top10` so if NHL
#   restores the surface in the future the registry stays correct.
EDGE_ENDPOINT_PARSERS = {
    # ---- Leaderboards (paths confirmed 404 live; kept for forward-compat) ----
    "nhl_edge_skater_shot_location_top_10":       parse_edge_top10,
    "nhl_edge_skater_shot_speed_top_10":          parse_edge_top10,
    "nhl_edge_skater_speed_top_10":               parse_edge_top10,
    "nhl_edge_skater_distance_top_10":            parse_edge_top10,
    "nhl_edge_skater_zone_time_top_10":           parse_edge_top10,
    "nhl_edge_goalie_5v5_top_10":                 parse_edge_top10,
    "nhl_edge_goalie_edge_save_pctg_top_10":      parse_edge_top10,
    "nhl_edge_goalie_shot_location_top_10":       parse_edge_top10,
    "nhl_edge_team_shot_location_top_10":         parse_edge_top10,
    "nhl_edge_team_skating_distance_top_10":      parse_edge_top10,
    "nhl_edge_team_skating_speed_top_10":         parse_edge_top10,
    "nhl_edge_team_zone_time_top_10":             parse_edge_top10,

    # ---- Single-entity detail / comparison (all confirmed live) ----
    "nhl_edge_skater_detail":                     parse_edge_detail,
    "nhl_edge_skater_comparison":                 parse_edge_detail,
    "nhl_edge_skater_shot_speed_detail":          parse_edge_detail,
    "nhl_edge_skater_skating_distance_detail":    parse_edge_detail,
    "nhl_edge_skater_skating_speed_detail":       parse_edge_detail,
    "nhl_edge_skater_landing":                    parse_edge_detail,
    "nhl_edge_goalie_detail":                     parse_edge_detail,
    "nhl_edge_goalie_5v5_detail":                 parse_edge_detail,
    "nhl_edge_goalie_comparison":                 parse_edge_detail,
    "nhl_edge_goalie_save_percentage_detail":     parse_edge_detail,
    "nhl_edge_goalie_landing":                    parse_edge_detail,
    "nhl_edge_team_detail":                       parse_edge_detail,
    "nhl_edge_team_landing":                      parse_edge_detail,
    "nhl_edge_team_shot_speed_detail":            parse_edge_detail,
    "nhl_edge_cat_skater_detail":                 parse_edge_detail,
    "nhl_edge_cat_goalie_detail":                 parse_edge_detail,

    # ---- Shot-location heat maps (17-cell grids) ----
    "nhl_edge_skater_shot_location_detail":       parse_edge_shot_location,
    "nhl_edge_goalie_shot_location_detail":       parse_edge_shot_location,
    "nhl_edge_team_shot_location_detail":         parse_edge_shot_location,

    # ---- Zone-time breakdowns ----
    "nhl_edge_skater_zone_time":                  parse_edge_zone_time,
    "nhl_edge_team_zone_time_details":            parse_edge_zone_time,
}


# Sub-frame parser hints: which detail-payload wrappers ship rich nested
# lists that the long-form sub-frame parsers (parse_edge_sog_details /
# parse_edge_sog_summary / parse_edge_hardest_shots) can unroll.
#
# Use these as a quick reference for "what else can I pull out of the
# raw payload" — they're not part of parser_for_edge() (which is one
# wrapper → one parser).
EDGE_SUBFRAME_PARSERS = {
    # Both skater-detail and team-detail ship sogDetails (17 rows) and
    # sogSummary (4 rows).
    "nhl_edge_skater_detail":           (parse_edge_sog_details, parse_edge_sog_summary),
    "nhl_edge_team_detail":             (parse_edge_sog_details, parse_edge_sog_summary),
    # goalie-detail ships shotLocationDetails (17 rows) and shotLocationSummary (4 rows).
    "nhl_edge_goalie_detail":           (parse_edge_sog_details, parse_edge_sog_summary),
    # shot-location-detail endpoints ship shotLocationDetails (17 rows) and
    # shotLocationTotals (4-12 rows).
    "nhl_edge_skater_shot_location_detail": (parse_edge_sog_details, parse_edge_sog_summary),
    "nhl_edge_team_shot_location_detail":   (parse_edge_sog_details, parse_edge_sog_summary),
    "nhl_edge_goalie_shot_location_detail": (parse_edge_sog_details, parse_edge_sog_summary),
    # shot-speed-detail ships hardestShots (10 rows).
    "nhl_edge_skater_shot_speed_detail": (parse_edge_hardest_shots,),
}


def parser_for_edge(fn_name: str):
    """Return the registered EDGE parser for a wrapper name.

    Falls back to :func:`parse_edge_payload` (generic best-effort flatten)
    when no specific parser is registered, so the caller always gets a
    DataFrame-returning function rather than ``None``.

    Args:
        fn_name: The ``__name__`` of any ``nhl_edge_*`` wrapper.

    Returns:
        Parser callable: one of :func:`parse_edge_top10`,
        :func:`parse_edge_detail`, :func:`parse_edge_shot_location`,
        :func:`parse_edge_zone_time`, or :func:`parse_edge_payload`.
    """
    return EDGE_ENDPOINT_PARSERS.get(fn_name, parse_edge_payload)
