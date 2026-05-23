"""sportsdataverse._common_espn_parsers — polars DataFrame parsers for ESPN endpoints.

Each parser accepts the raw ``Dict`` returned by the corresponding
``_common_espn`` helper (or any per-league wrapper that delegates to it)
and returns a tidy ``polars.DataFrame``.

All parsers:

* Return ``polars.DataFrame`` by default; ``pandas.DataFrame`` via
  ``return_as_pandas=True``.
* Return an *empty* frame (zero rows) rather than raising when the payload
  is empty or malformed — callers must guard the ``height == 0`` case.
* Snake-case output column names via
  :func:`sportsdataverse.dl_utils.underscore`.
* Use :func:`pandas.json_normalize` for nested structures, then convert to
  polars at the end — consistent with the rest of the package.

Endpoint → parser mapping
--------------------------

.. list-table::
   :header-rows: 1

   * - Endpoint family
     - Parser
     - Raw payload key
   * - ``_site_v2_scoreboard``
     - :func:`parse_scoreboard`
     - ``events[]``
   * - ``_site_v2_teams``
     - :func:`parse_teams`
     - ``sports[0].leagues[0].teams[]``
   * - ``_site_v2_alt_standings``
     - :func:`parse_standings`
     - ``children[].standings.entries[]``
   * - ``_site_v2_groups``
     - :func:`parse_groups`
     - ``sports[0].leagues[0].groups[]`` / ``groups[]``
   * - ``_espn_athlete_overview``
     - :func:`parse_athlete_overview`
     - ``statistics.splits[].stats[]``
   * - ``_espn_athlete_stats``
     - :func:`parse_athlete_stats`
     - ``categories[].labels``
   * - ``_espn_athlete_gamelog``
     - :func:`parse_athlete_gamelog`
     - ``seasonTypes[].categories[].events[]``
   * - ``_espn_athlete_splits``
     - :func:`parse_athlete_splits`
     - ``categories[].splits[]``
   * - ``_espn_statistics_byathlete``
     - :func:`parse_leaders`
     - ``categories[].leaders[]``
   * - ``_core_v2_season_coaches``
     - :func:`parse_coaches`
     - ``items[]``
   * - ``_core_v2_season_draft``
     - :func:`parse_draft`
     - ``rounds[].picks[]``
   * - ``_core_v2_event_competitor_roster``
     - :func:`parse_event_competitor_roster`
     - ``entries[].athlete``
   * - ``_core_v2_event_competitor_statistics``
     - :func:`parse_event_competitor_statistics`
     - ``splits[].categories[].stats[]``
   * - ``_core_v2_event_competitor_linescores``
     - :func:`parse_event_competitor_linescores`
     - ``items[]``
   * - ``_core_v2_event_plays``
     - :func:`parse_event_plays`
     - ``items[]``
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case via :func:`underscore`."""
    df.columns = [underscore(c) for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool) -> pl.DataFrame:
    """Convert pandas DataFrame to polars (or return pandas if requested).

    Handles the ``object`` dtype that polars cannot infer to a scalar type
    by casting those columns to ``str``.
    """
    if return_as_pandas:
        return df
    # Replace any remaining pure-Python objects (dicts/lists) with their
    # string representations so polars can ingest them without errors.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(lambda v: str(v) if isinstance(v, (dict, list)) else v)
    try:
        return pl.from_pandas(df)
    except Exception:
        # Last-resort: stringify everything that is still object-typed
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str)
        return pl.from_pandas(df)


def _empty_frame(return_as_pandas: bool = False):
    """Return an appropriately-typed empty frame."""
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


# ===========================================================================
# 1. Scoreboard
# ===========================================================================


def _scoreboard_event_parsing(event: dict) -> dict:
    """Flatten one ESPN scoreboard event into a single row keyed by game_id.

    Mirrors the prototype in :mod:`sportsdataverse.mlb.mlb_schedule` with
    additional ``home_color``/``away_color`` and ``notes`` fields.
    """
    comp = (event.get("competitions") or [{}])[0]
    competitors = comp.get("competitors") or []
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") == "away"), {})
    status = (event.get("status") or {}).get("type") or {}
    venue = comp.get("venue") or {}
    notes = comp.get("notes") or []
    note_text = notes[0].get("headline", "") if notes else ""

    def _team(side: dict) -> dict:
        team = side.get("team") or {}
        return {
            "id": team.get("id"),
            "name": team.get("name"),
            "abbreviation": team.get("abbreviation"),
            "display_name": team.get("displayName"),
            "location": team.get("location"),
            "color": team.get("color"),
            "alternate_color": team.get("alternateColor"),
            "logo": (team.get("logos") or [{}])[0].get("href")
            if team.get("logos")
            else team.get("logo"),
            "score": side.get("score"),
            "winner": side.get("winner"),
            "home_away": side.get("homeAway"),
            "rank": side.get("curatedRank", {}).get("current"),
        }

    h, a = _team(home), _team(away)
    return {
        "game_id": event.get("id"),
        "uid": event.get("uid"),
        "date": event.get("date"),
        "name": event.get("name"),
        "short_name": event.get("shortName"),
        "season_year": (event.get("season") or {}).get("year"),
        "season_type": (event.get("season") or {}).get("type"),
        "season_slug": (event.get("season") or {}).get("slug"),
        "status_type_id": status.get("id"),
        "status_type_name": status.get("name"),
        "status_type_state": status.get("state"),
        "status_type_completed": status.get("completed"),
        "status_type_description": status.get("description"),
        "status_type_detail": status.get("detail"),
        "status_type_short_detail": status.get("shortDetail"),
        "status_clock": (event.get("status") or {}).get("clock"),
        "status_display_clock": (event.get("status") or {}).get("displayClock"),
        "status_period": (event.get("status") or {}).get("period"),
        "neutral_site": comp.get("neutralSite"),
        "conference_competition": comp.get("conferenceCompetition"),
        "attendance": comp.get("attendance"),
        "venue_id": venue.get("id"),
        "venue_full_name": venue.get("fullName"),
        "venue_city": (venue.get("address") or {}).get("city"),
        "venue_state": (venue.get("address") or {}).get("state"),
        "venue_indoor": venue.get("indoor"),
        "broadcast": ", ".join(
            b.get("names", [""])[0] if b.get("names") else b.get("market", "")
            for b in (comp.get("broadcasts") or [])
        ),
        "note": note_text,
        "home_id": h["id"],
        "home_name": h["name"],
        "home_abbreviation": h["abbreviation"],
        "home_display_name": h["display_name"],
        "home_location": h["location"],
        "home_color": h["color"],
        "home_alternate_color": h["alternate_color"],
        "home_logo": h["logo"],
        "home_score": h["score"],
        "home_winner": h["winner"],
        "home_rank": h["rank"],
        "away_id": a["id"],
        "away_name": a["name"],
        "away_abbreviation": a["abbreviation"],
        "away_display_name": a["display_name"],
        "away_location": a["location"],
        "away_color": a["color"],
        "away_alternate_color": a["alternate_color"],
        "away_logo": a["logo"],
        "away_score": a["score"],
        "away_winner": a["winner"],
        "away_rank": a["rank"],
    }


def parse_scoreboard(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a scoreboard response into a tidy polars frame.

    Input: raw payload from ``_site_v2_scoreboard()`` (or any per-league
    ``espn_{league}_scoreboard()`` wrapper).
    Output: one row per event with columns covering game_id, date,
    season_year, season_type, status_*, venue_*, home_*, away_*.

    Args:
        payload: Raw JSON dict from the scoreboard endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty or missing
        the ``events`` key.

    Example:
        Parse a pre-fetched payload::

            from sportsdataverse._common_espn_parsers import parse_scoreboard
            from sportsdataverse._common_espn import _site_v2_scoreboard

            raw = _site_v2_scoreboard("basketball", "nba", dates=20240101)
            df = parse_scoreboard(raw)
            print(df.shape)
    """
    if not payload:
        return _empty_frame(return_as_pandas)
    events = payload.get("events") or []
    if not events:
        return _empty_frame(return_as_pandas)
    rows = [_scoreboard_event_parsing(ev) for ev in events]
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 2. Teams (site v2)
# ===========================================================================


def parse_teams(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a site-v2 teams response into a tidy polars frame.

    Input: raw payload from ``_site_v2_teams()`` (``sports[0].leagues[0].teams[]``).
    Output: one row per team, columns from the ``team`` sub-object flattened
    via ``pd.json_normalize``.

    Args:
        payload: Raw JSON dict from the teams endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Example:
        Parse a pre-fetched payload::

            from sportsdataverse._common_espn_parsers import parse_teams
            raw = _site_v2_teams("basketball", "nba")
            df = parse_teams(raw)
            print(df.select(["team_id", "team_abbreviation"]).head())
    """
    if not payload:
        return _empty_frame(return_as_pandas)
    try:
        sports = payload.get("sports") or []
        if sports:
            leagues = (sports[0] or {}).get("leagues") or []
            teams_raw = (leagues[0] or {}).get("teams") or [] if leagues else []
        else:
            # Alternate shape: top-level "items" (core v2) or "teams"
            teams_raw = payload.get("items") or payload.get("teams") or []

        if not teams_raw:
            return _empty_frame(return_as_pandas)

        # Strip heavy sub-objects that polars can't handle and we don't need
        _drop = {"record", "links", "nextEvent", "standingSummary"}
        cleaned = []
        for entry in teams_raw:
            t = dict(entry.get("team") or entry)
            for k in _drop:
                t.pop(k, None)
            cleaned.append({"team": t})

        df = pd.json_normalize(cleaned, sep="_")
        df = _snake_columns(df)
        return _to_output(df, return_as_pandas)
    except Exception:
        return _empty_frame(return_as_pandas)


# ===========================================================================
# 3. Standings (alt v2)
# ===========================================================================


def _extract_standing_entries(
    children: List[dict],
    parent_name: str = "",
    parent_abbreviation: str = "",
) -> List[dict]:
    """Recursively flatten standings entries from a ``children[]`` list.

    Each entry in ``children`` may have its own ``children`` (e.g. conference
    → division). Recursion stops when ``standings.entries`` is present.
    """
    rows: List[dict] = []
    for child in children:
        group_name = child.get("name") or parent_name
        group_abbr = child.get("abbreviation") or parent_abbreviation
        entries = (child.get("standings") or {}).get("entries") or []
        if entries:
            for entry in entries:
                team = entry.get("team") or {}
                stats_list = entry.get("stats") or []
                row: dict = {
                    "group_name": group_name,
                    "group_abbreviation": group_abbr,
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "team_abbreviation": team.get("abbreviation"),
                    "team_display_name": team.get("displayName"),
                    "team_location": team.get("location"),
                    "team_logo": team.get("logo"),
                }
                for stat in stats_list:
                    col = underscore(stat.get("name") or stat.get("abbreviation") or "")
                    row[col] = stat.get("value")
                rows.append(row)
        # Recurse into nested children
        sub = child.get("children") or []
        if sub:
            rows.extend(_extract_standing_entries(sub, group_name, group_abbr))
    return rows


def parse_standings(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a standings (alt v2) response into a tidy polars frame.

    Input: raw payload from ``_site_v2_alt_standings()``.
    Output: one row per team entry, with per-team ``stats[]`` values pivoted
    into individual columns. Carries ``group_name`` (conference/division).

    Args:
        payload: Raw JSON dict from the standings endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Example:
        Parse a pre-fetched payload::

            from sportsdataverse._common_espn_parsers import parse_standings
            raw = _site_v2_alt_standings("basketball", "nba", season=2024)
            df = parse_standings(raw)
            df.select(["team_abbreviation", "wins", "losses"]).head(10)
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    children = payload.get("children") or []
    # Some shapes wrap under "standings" at top level
    if not children:
        entries = (payload.get("standings") or {}).get("entries") or []
        if entries:
            children = [payload]  # treat the top-level as a single group

    if not children:
        return _empty_frame(return_as_pandas)

    rows = _extract_standing_entries(children)
    if not rows:
        return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 4. Groups (conferences/divisions)
# ===========================================================================


def _flatten_groups(groups: List[dict], parent_id: str = "", depth: int = 0) -> List[dict]:
    """Recursively flatten a groups list."""
    rows: List[dict] = []
    for g in groups:
        children = g.get("children") or []
        row = {
            "group_id": g.get("id") or g.get("groupId"),
            "name": g.get("name"),
            "abbreviation": g.get("abbreviation") or g.get("abbrev"),
            "short_name": g.get("shortName"),
            "is_conference": g.get("isConference", depth == 0),
            "parent_group_id": parent_id or None,
            "depth": depth,
            "children_count": len(children),
        }
        rows.append(row)
        if children:
            rows.extend(_flatten_groups(children, parent_id=row["group_id"] or "", depth=depth + 1))
    return rows


def parse_groups(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a groups response into a tidy polars frame.

    Input: raw payload from ``_site_v2_groups()``.
    Output: one row per group (conference or division), with columns
    group_id, name, abbreviation, is_conference, children_count.

    Args:
        payload: Raw JSON dict from the groups endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    # Shape varies: sports[0].leagues[0].groups[] or groups[]
    try:
        sports = payload.get("sports") or []
        if sports:
            leagues = (sports[0] or {}).get("leagues") or []
            groups = (leagues[0] or {}).get("groups") or [] if leagues else []
        else:
            groups = payload.get("groups") or []
    except Exception:
        groups = []

    if not groups:
        return _empty_frame(return_as_pandas)

    rows = _flatten_groups(groups)
    if not rows:
        return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 5. Athlete overview
# ===========================================================================


def parse_athlete_overview(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an athlete overview response into a tidy polars frame.

    Input: raw payload from ``_espn_athlete_overview()``.
    Output: one row per stats split (season / career / etc.) with columns
    matching the stat labels. Bio fields (name, position, etc.) are carried
    on every row.

    Args:
        payload: Raw JSON dict from the athlete overview endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Note:
        The overview shape varies by sport.  When ``statistics.splits``
        is absent the function falls back to ``pd.json_normalize(payload)``.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    athlete = payload.get("athlete") or {}
    bio = {
        "athlete_id": athlete.get("id"),
        "athlete_display_name": athlete.get("displayName"),
        "athlete_short_name": athlete.get("shortName"),
        "athlete_position": (athlete.get("position") or {}).get("abbreviation"),
        "athlete_jersey": athlete.get("jersey"),
        "athlete_team_id": (athlete.get("team") or {}).get("id"),
        "athlete_team_abbreviation": (athlete.get("team") or {}).get("abbreviation"),
    }

    statistics = payload.get("statistics") or {}
    splits = statistics.get("splits") or []

    rows: List[dict] = []
    for split in splits:
        labels: List[str] = statistics.get("labels") or split.get("labels") or []
        names: List[str] = statistics.get("names") or split.get("names") or labels
        stats: List = split.get("stats") or []
        row = dict(bio)
        row["split_name"] = split.get("name") or split.get("displayName")
        row["split_category"] = split.get("category")
        for i, val in enumerate(stats):
            col = underscore(names[i]) if i < len(names) else f"stat_{i}"
            row[col] = val
        rows.append(row)

    if not rows:
        # Fallback: flatten whatever is in the payload
        try:
            df = pd.json_normalize(payload, sep="_")
            df = _snake_columns(df)
            return _to_output(df, return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 6. Athlete stats
# ===========================================================================


def parse_athlete_stats(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an athlete stats response into a tidy polars frame.

    Input: raw payload from ``_espn_athlete_stats()``.
    Output: one row per (category × split) combination. The parallel
    ``labels``/``names``/``displayNames`` arrays become column names; each
    ``splits[].stats[]`` array becomes the values.

    Args:
        payload: Raw JSON dict from the athlete stats endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    categories = payload.get("categories") or []
    if not categories:
        # Try direct labels/splits at top level (some sport variants)
        labels = payload.get("labels") or []
        splits = payload.get("splits") or []
        if labels and splits:
            categories = [{"labels": labels, "splits": splits, "name": "default"}]

    if not categories:
        try:
            df = pd.json_normalize(payload, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    rows: List[dict] = []
    for cat in categories:
        cat_name = cat.get("name") or cat.get("displayName") or ""
        labels: List[str] = cat.get("labels") or cat.get("names") or []
        names: List[str] = cat.get("names") or labels
        splits = cat.get("splits") or []
        for split in splits:
            stats: List = split.get("stats") or []
            row = {
                "category": cat_name,
                "split_name": split.get("name") or split.get("displayName"),
                "split_category": split.get("category"),
                "split_value": split.get("value"),
            }
            for i, val in enumerate(stats):
                col = underscore(names[i]) if i < len(names) else f"stat_{i}"
                row[col] = val
            rows.append(row)

    if not rows:
        return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 7. Athlete gamelog
# ===========================================================================


def parse_athlete_gamelog(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an athlete gamelog response into a tidy polars frame.

    Input: raw payload from ``_espn_athlete_gamelog()``.
    Output: one row per game from ``seasonTypes[].categories[].events[]``.
    Carries season_type_id, season_type_name, category + per-event game-stats.

    Args:
        payload: Raw JSON dict from the athlete gamelog endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    season_types = payload.get("seasonTypes") or []
    if not season_types:
        # Fallback: top-level events
        events = payload.get("events") or []
        if events:
            season_types = [{"id": None, "name": None, "categories": [{"name": None, "events": events}]}]

    rows: List[dict] = []
    for st in season_types:
        st_id = st.get("id")
        st_name = st.get("name") or st.get("displayName")
        categories = st.get("categories") or []
        for cat in categories:
            cat_name = cat.get("name") or cat.get("displayName")
            labels: List[str] = cat.get("labels") or cat.get("names") or []
            names: List[str] = cat.get("names") or labels
            events = cat.get("events") or []
            for ev in events:
                event_ref = ev.get("eventId") or ev.get("id") or ev.get("event", {}).get("id")
                opp = ev.get("opponent") or {}
                row = {
                    "season_type_id": st_id,
                    "season_type_name": st_name,
                    "category": cat_name,
                    "event_id": event_ref,
                    "event_date": ev.get("date"),
                    "home_away": ev.get("homeAway"),
                    "score": ev.get("score"),
                    "opponent_id": opp.get("id"),
                    "opponent_abbreviation": opp.get("abbreviation"),
                    "opponent_display_name": opp.get("displayName"),
                    "game_result": ev.get("gameResult"),
                    "game_processed": ev.get("gameProcessed"),
                }
                stats: List = ev.get("stats") or []
                for i, val in enumerate(stats):
                    col = underscore(names[i]) if i < len(names) else f"stat_{i}"
                    row[col] = val
                rows.append(row)

    if not rows:
        return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 8. Athlete splits
# ===========================================================================


def parse_athlete_splits(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an athlete splits response into a tidy polars frame.

    Input: raw payload from ``_espn_athlete_splits()``.
    Output: one row per split with columns matching the category labels.

    Args:
        payload: Raw JSON dict from the athlete splits endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    categories = payload.get("categories") or []
    rows: List[dict] = []

    for cat in categories:
        cat_name = cat.get("name") or cat.get("displayName") or ""
        labels: List[str] = cat.get("labels") or cat.get("names") or []
        names: List[str] = cat.get("names") or labels
        splits = cat.get("splits") or []
        for split in splits:
            stats: List = split.get("stats") or []
            row = {
                "category": cat_name,
                "split_name": split.get("name") or split.get("displayName"),
                "split_abbreviation": split.get("abbreviation"),
                "split_category": split.get("category"),
                "split_value": split.get("value"),
                "split_description": split.get("description"),
            }
            for i, val in enumerate(stats):
                col = underscore(names[i]) if i < len(names) else f"stat_{i}"
                row[col] = val
            rows.append(row)

    if not rows:
        # Safe fallback
        try:
            df = pd.json_normalize(payload, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 9. Leaders (statistics/byathlete)
# ===========================================================================


def parse_leaders(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a statistics-by-athlete leaderboard response into a tidy polars frame.

    Input: raw payload from ``_espn_statistics_byathlete()``.
    Output: one row per athlete in ``categories[].leaders[]`` with
    athlete_id + per-stat values. Stat column names are drawn from
    ``glossary[].abbreviation`` or ``categories[].labels``.

    Args:
        payload: Raw JSON dict from the statistics/byathlete endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    # Build a glossary mapping abbreviation → description from payload.glossary
    glossary = {
        g.get("abbreviation"): g.get("displayName") or g.get("description") or g.get("abbreviation")
        for g in (payload.get("glossary") or [])
    }

    categories = payload.get("categories") or []
    rows: List[dict] = []

    for cat in categories:
        cat_name = cat.get("name") or cat.get("displayName") or ""
        labels: List[str] = cat.get("labels") or cat.get("names") or []
        names: List[str] = cat.get("names") or labels
        leaders = cat.get("leaders") or []
        for leader in leaders:
            athlete = leader.get("athlete") or {}
            team = leader.get("team") or {}
            row = {
                "category": cat_name,
                "rank": leader.get("rank"),
                "athlete_id": athlete.get("id"),
                "athlete_display_name": athlete.get("displayName"),
                "athlete_short_name": athlete.get("shortName"),
                "athlete_jersey": athlete.get("jersey"),
                "athlete_position": (athlete.get("position") or {}).get("abbreviation"),
                "team_id": team.get("id"),
                "team_abbreviation": team.get("abbreviation"),
                "team_display_name": team.get("displayName"),
            }
            stats: List = leader.get("stats") or []
            for i, val in enumerate(stats):
                if i < len(names):
                    col = underscore(names[i])
                elif labels and i < len(labels):
                    col = underscore(labels[i])
                else:
                    col = f"stat_{i}"
                row[col] = val
            rows.append(row)

    if not rows:
        try:
            df = pd.json_normalize(payload, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 10. Coaches (season-scoped)
# ===========================================================================


def parse_coaches(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a season coaches response into a tidy polars frame.

    Input: raw payload from ``_core_v2_season_coaches()``.
    Output: one row per coach from ``items[]`` (or ``coaches[]``).

    Args:
        payload: Raw JSON dict from the season coaches endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Note:
        The core-v2 coaches endpoint uses ``$ref``-heavy pagination. This
        parser handles the *inline* shape (``items[].id`` present). When
        items contain only ``$ref`` links, the frame will have a single
        ``ref`` column — follow the refs to get rich coach objects.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    items = payload.get("items") or payload.get("coaches") or []
    if not items:
        return _empty_frame(return_as_pandas)

    rows: List[dict] = []
    for item in items:
        # Flatten only scalar fields; drop deep nesting that varies per coach
        row: dict = {}
        for k, v in item.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                row[k] = v
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, (str, int, float, bool)) or v2 is None:
                        row[f"{k}_{k2}"] = v2
        rows.append(row)

    if not rows:
        try:
            df = pd.json_normalize(items, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 11. Draft
# ===========================================================================


def parse_draft(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a draft response into a tidy polars frame.

    Input: raw payload from ``_core_v2_season_draft()`` or
    ``_site_v2_draft()``.
    Output: one row per pick from ``rounds[].picks[]``.  Falls back to
    ``items[]`` or ``picks[]`` when the rounds structure is absent.

    Args:
        payload: Raw JSON dict from the draft endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Note:
        Shape is uncertain across sports — the rounds/picks structure is
        confirmed for NFL/NBA. Other sports may use ``items[]`` or a flat
        ``picks[]`` directly at the top level.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    rounds = payload.get("rounds") or []
    all_picks: List[dict] = []

    if rounds:
        for rnd in rounds:
            round_num = rnd.get("number") or rnd.get("round")
            picks = rnd.get("picks") or rnd.get("items") or []
            for pick in picks:
                p = dict(pick)
                p.setdefault("round_number", round_num)
                all_picks.append(p)
    else:
        # Fallback shapes
        all_picks = payload.get("picks") or payload.get("items") or []

    if not all_picks:
        return _empty_frame(return_as_pandas)

    try:
        df = pd.json_normalize(all_picks, sep="_")
        df = _snake_columns(df)
        return _to_output(df, return_as_pandas)
    except Exception:
        return _empty_frame(return_as_pandas)


# ===========================================================================
# 12. Event competitor roster
# ===========================================================================


def parse_event_competitor_roster(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an event competitor roster response into a tidy polars frame.

    Input: raw payload from ``_core_v2_event_competitor_roster()``.
    Output: one row per athlete from ``entries[].athlete`` (flattened).

    Args:
        payload: Raw JSON dict from the event competitor roster endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    entries = payload.get("entries") or payload.get("items") or []
    if not entries:
        return _empty_frame(return_as_pandas)

    rows: List[dict] = []
    for entry in entries:
        athlete = entry.get("athlete") or entry
        row: dict = {}
        for k, v in athlete.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                row[k] = v
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, (str, int, float, bool)) or v2 is None:
                        row[f"{k}_{k2}"] = v2
        # Also carry entry-level metadata (status, active, etc.)
        for k in ("active", "starter", "didNotPlay", "ejected", "playingTime"):
            if k in entry:
                row.setdefault(k, entry[k])
        rows.append(row)

    if not rows:
        try:
            df = pd.json_normalize(entries, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 13. Event competitor statistics
# ===========================================================================


def parse_event_competitor_statistics(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an event competitor statistics response into a tidy polars frame.

    Input: raw payload from ``_core_v2_event_competitor_statistics()``.
    Output: one row per stat from ``splits[].categories[].stats[]``.
    Each row carries split_name, category_name, stat_name, stat_abbreviation,
    stat_value, stat_display_value.

    Args:
        payload: Raw JSON dict from the event competitor statistics endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    splits = payload.get("splits") or []
    if not splits:
        # Some endpoints expose categories directly
        cats = payload.get("categories") or []
        if cats:
            splits = [{"name": None, "categories": cats}]

    rows: List[dict] = []
    for split in splits:
        split_name = split.get("name") or split.get("displayName")
        categories = split.get("categories") or []
        for cat in categories:
            cat_name = cat.get("name") or cat.get("displayName")
            stats = cat.get("stats") or []
            for stat in stats:
                rows.append(
                    {
                        "split_name": split_name,
                        "category_name": cat_name,
                        "stat_name": stat.get("name"),
                        "stat_abbreviation": stat.get("abbreviation"),
                        "stat_value": stat.get("value"),
                        "stat_display_value": stat.get("displayValue"),
                        "stat_description": stat.get("description"),
                    }
                )

    if not rows:
        try:
            df = pd.json_normalize(payload, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 14. Event competitor linescores
# ===========================================================================


def parse_event_competitor_linescores(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an event competitor linescores response into a tidy polars frame.

    Input: raw payload from ``_core_v2_event_competitor_linescores()``.
    Output: one row per period from ``items[]``.

    Args:
        payload: Raw JSON dict from the event competitor linescores endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    items = payload.get("items") or payload.get("linescores") or []
    if not items:
        return _empty_frame(return_as_pandas)

    rows: List[dict] = []
    for i, item in enumerate(items):
        row = {"period": i + 1}
        for k, v in item.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                row[k] = v
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, (str, int, float, bool)) or v2 is None:
                        row[f"{k}_{k2}"] = v2
        rows.append(row)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# 15. Event plays
# ===========================================================================


def parse_event_plays(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an event plays response into a tidy polars frame.

    Input: raw payload from ``_core_v2_event_plays()``.
    Output: one row per play from ``items[]``.

    Args:
        payload: Raw JSON dict from the event plays endpoint.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: Tidy frame; zero rows if payload is empty.

    Note:
        The ``participants[]``, ``probability``, and ``start``/``end``
        sub-objects are flattened one level deep. Deeply nested structures
        (``athletesInvolved[]``) are kept as string representations to avoid
        column-count explosion.
    """
    if not payload:
        return _empty_frame(return_as_pandas)

    items = payload.get("items") or payload.get("plays") or []
    if not items:
        return _empty_frame(return_as_pandas)

    # Drop keys that are too deeply nested or too large for a flat frame
    _skip = {"participants", "athletesInvolved", "drive"}

    rows: List[dict] = []
    for play in items:
        row: dict = {}
        for k, v in play.items():
            if k in _skip:
                continue
            if isinstance(v, (str, int, float, bool)) or v is None:
                row[k] = v
            elif isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, (str, int, float, bool)) or v2 is None:
                        row[f"{k}_{k2}"] = v2
                    elif isinstance(v2, dict):
                        for k3, v3 in v2.items():
                            if isinstance(v3, (str, int, float, bool)) or v3 is None:
                                row[f"{k}_{k2}_{k3}"] = v3
            elif isinstance(v, list):
                row[k] = str(v)
        rows.append(row)

    if not rows:
        try:
            df = pd.json_normalize(items, sep="_")
            return _to_output(_snake_columns(df), return_as_pandas)
        except Exception:
            return _empty_frame(return_as_pandas)

    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# Endpoint -> parser registry
# ===========================================================================
#
# Maps the *short name* used in sportsdataverse._common_espn's wrapper
# tables (_UNIVERSAL_WRAPPERS, _NCAA_WRAPPERS, _FOOTBALL_WRAPPERS,
# _MLB_WRAPPERS) to the parser that turns its raw payload into a tidy
# polars DataFrame.
#
# Keys here MUST match the first element of the tuples in those tables.
# Helpers without a registered parser pass through as raw Dict.
#
# This registry is the dispatch table for the ``return_parsed=True`` kwarg
# wired into every bound wrapper by ``make_league_module()`` -- see
# ``sportsdataverse._common_espn._bind``.

ENDPOINT_PARSERS = {
    # Site v2
    "scoreboard": parse_scoreboard,
    "teams_site": parse_teams,
    # Site v2 alt + Core v2
    "standings": parse_standings,
    "standings_core": parse_standings,
    # Groups / conferences
    "conferences": parse_groups,
    # Web v3 athlete deep dives
    "athlete_overview": parse_athlete_overview,
    "athlete_stats": parse_athlete_stats,
    "athlete_gamelog": parse_athlete_gamelog,
    "athlete_splits": parse_athlete_splits,
    "leaders": parse_leaders,
    # Core v2 catalog
    "teams_core": parse_teams,
    "coaches": parse_coaches,
    "season_coaches": parse_coaches,
    "season_draft": parse_draft,
    # Event-competitor surface
    "event_competitor_roster": parse_event_competitor_roster,
    "event_competitor_statistics": parse_event_competitor_statistics,
    "event_competitor_linescores": parse_event_competitor_linescores,
    "event_plays": parse_event_plays,
}


def parser_for(short_name):
    """Return the registered parser for an endpoint short name, or None.

    Args:
        short_name: First element of the tuple in any ``_*_WRAPPERS`` table
            (e.g. ``"scoreboard"``, ``"teams_site"``, ``"athlete_overview"``).

    Returns:
        The parser callable, or ``None`` if the endpoint has no registered
        parser yet.
    """
    return ENDPOINT_PARSERS.get(short_name)
