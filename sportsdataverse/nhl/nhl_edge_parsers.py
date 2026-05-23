"""sportsdataverse.nhl.nhl_edge_parsers — polars parsers for NHL EDGE payloads.

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
four shape families that share a parser:

* **Leaderboards** (``*_top_10``) — list of player/team rows with shared
  schema. Parser: :func:`parse_edge_top10`.
* **Detail pages** (``*_detail``, ``*_5v5_detail``, ``*_comparison``) —
  multi-section single-entity payload. Parser: :func:`parse_edge_detail`.
* **Shot-location** (``*_shot_location_*``) — strike-zone–style heat map
  with one cell per zone. Parser: :func:`parse_edge_shot_location`.
* **Zone-time** (``*_zone_time_*``) — possession share by zone (offensive,
  defensive, neutral, plus split blocks). Parser:
  :func:`parse_edge_zone_time`.

Endpoints not in those four families pass through as raw ``Dict``; call
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


# Keys that commonly host the per-zone grid.
_SHOT_LOCATION_KEYS = (
    "shotLocation",
    "shotLocations",
    "zones",
    "buckets",
    "grid",
)


def parse_edge_shot_location(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Parse an EDGE shot-location heat map into long-form rows.

    Walks the payload for a list of zone records (each typically with
    coordinates, shot/goal totals, and a percentage). Each zone becomes
    one row in the output frame; if the payload has multiple top-level
    locations (e.g. per-position breakdown), each location's zones are
    unrolled with a leading ``section`` column identifying the parent.

    Args:
        payload: Raw JSON dict from any ``nhl_edge_*_shot_location_*``
            wrapper.
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


# Zone-time payloads typically have one record per zone (offensive,
# defensive, neutral) plus optional strength splits.
_ZONE_TIME_KEYS = (
    "zoneTime",
    "zoneTimes",
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
            try:
                df = pd.json_normalize(candidate, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)
            except Exception:
                continue
        if isinstance(candidate, dict) and candidate:
            # dict-of-zone keyed by zone name
            rows = []
            for zone, payload_inner in candidate.items():
                row = {"zone": zone}
                if isinstance(payload_inner, dict):
                    row.update(payload_inner)
                else:
                    row["value"] = payload_inner
                rows.append(row)
            if rows:
                df = pd.json_normalize(rows, sep="_")
                df = _snake_columns(df)
                return _to_output(df, return_as_pandas)

    # No zone-shaped key found — fall back to a single-row flatten.
    return parse_edge_detail(payload, return_as_pandas=return_as_pandas)


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
EDGE_ENDPOINT_PARSERS = {
    # ---- Leaderboards ----
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

    # ---- Single-entity detail / comparison ----
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

    # ---- Shot-location heat maps ----
    "nhl_edge_skater_shot_location_detail":       parse_edge_shot_location,
    "nhl_edge_goalie_shot_location_detail":       parse_edge_shot_location,
    "nhl_edge_team_shot_location_detail":         parse_edge_shot_location,

    # ---- Zone-time breakdowns ----
    "nhl_edge_skater_zone_time":                  parse_edge_zone_time,
    "nhl_edge_team_zone_time_details":            parse_edge_zone_time,
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
