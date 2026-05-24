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
# Generic Core v2 paginated items
# ===========================================================================


# Keys that hold the row list in ESPN paginated / list payloads. Tried
# in priority order — the first one that resolves to a non-empty list
# is the row source.
_LIST_PAYLOAD_KEYS = (
    "items",     # Core v2 paginated default
    "entries",   # Core v2 athlete statisticslog
    "events",    # site v2 schedules, scoreboards as fallback
    "athletes",  # team rosters as fallback
)


def parse_items(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse an ESPN paginated / list response into a tidy frame.

    Walks ``payload`` for the first non-empty list under one of the
    well-known row keys (``items`` → ``entries`` → ``events`` →
    ``athletes``), flattens with :func:`pandas.json_normalize`, and
    snake-cases the columns.

    Many Core v2 ``items`` are just ``{"$ref": "<url>"}`` pointers — this
    parser does NOT auto-resolve them; it returns one row per item, so
    ``$ref``-only items yield a frame with a single ``_ref`` column.

    Args:
        payload: Raw JSON dict from any Core v2 paginated list endpoint
            (``venues``, ``franchises``, ``events``, ``awards``,
            ``athletes_index``, ``athlete_statisticslog``, …).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per item. Zero rows
        when no list key resolves.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    rows = None
    for key in _LIST_PAYLOAD_KEYS:
        candidate = payload.get(key)
        if isinstance(candidate, list) and candidate:
            rows = candidate
            break
    if rows is None:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(rows, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# Team-scoped Site v2 payloads
# ===========================================================================


def parse_team_schedule(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a Site v2 ``team/{id}/schedule`` response into a tidy frame.

    Input: raw payload with shape::

        {"events": [...], "season": {...}, "team": {...},
         "requestedSeason": {...}, "byeWeek": ...}

    One row per event with columns covering ``event_id``, ``date``,
    ``name``, ``short_name``, ``season_*``, ``competitions_*``.

    Args:
        payload: Raw JSON dict from ``espn_{league}_team_schedule()``.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per scheduled event.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    events = payload.get("events")
    if not isinstance(events, list) or not events:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(events, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_team_roster(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a Site v2 ``team/{id}/roster`` response into a tidy frame.

    Input: raw payload with shape::

        {"athletes": [...], "coach": [...], "team": {...}, "season": {...}}

    One row per athlete with columns flattened from the athlete sub-
    object (``id``, ``firstName``, ``lastName``, ``position``,
    ``experience``, ``jersey``, ``height``, ``weight``, …).  Coaches are
    available via ``payload["coach"]``; this parser intentionally only
    surfaces the athlete rows.

    Args:
        payload: Raw JSON dict from ``espn_{league}_team_roster()``.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per athlete on the
        roster.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    athletes = payload.get("athletes")
    if not isinstance(athletes, list) or not athletes:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(athletes, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# News / injuries (Site v2 league-wide + team / athlete scoped)
# ===========================================================================


def parse_news(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a Site v2 ``news`` response into a tidy frame.

    Input: raw payload from any of ``espn_{league}_news()``,
    ``espn_{league}_team_news()``, or ``espn_{league}_athlete_news()`` —
    shape::

        {"header": "<title>", "link": {...}, "articles": [...]}

    One row per article with columns flattened from the article object
    (``id``, ``headline``, ``description``, ``published``, ``type``,
    ``byline``, ``links_*``, …).

    Args:
        payload: Raw JSON dict from any news wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per article.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    articles = payload.get("articles")
    if not isinstance(articles, list) or not articles:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(articles, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_injuries(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a Site v2 ``injuries`` response into a tidy frame.

    Input: raw payload with shape::

        {"injuries": [{"id": "<team-id>",
                       "displayName": "<team-name>",
                       "injuries": [<per-player injury>...]}],
         "season": {...}}

    The **outer** ``injuries`` list is per-team; each team carries a
    nested ``injuries`` sub-list of per-player records. This parser
    returns the **outer** rows (one per team) with the nested list left
    as a list-typed column — call ``df.explode("injuries")`` on the
    pandas frame to drill in.

    Args:
        payload: Raw JSON dict from any injuries wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per team that has
        injuries reported.
    """
    if not payload or not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    teams = payload.get("injuries")
    if not isinstance(teams, list) or not teams:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(teams, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    # Keep the per-player nested list column as a string so polars can ingest.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ===========================================================================
# Site v2 summary — multi-section dispatcher + per-section parsers
# ===========================================================================
#
# The Site v2 ``summary`` endpoint
# (``espn_{league}_summary(event_id=...)``) returns a single huge payload
# with ~19 top-level sections: ``boxscore``, ``gameInfo``, ``leaders``,
# ``injuries``, ``plays``, ``winprobability``, ``news``, ``seasonseries``,
# ``againstTheSpread``, ``header``, ``standings``, ``article``, etc.
#
# Rather than collapse that into one parser, each section gets its own
# targeted ``parse_summary_*`` parser; ``parse_summary`` is the
# dispatcher that returns a dict of sub-frames keyed by section name.
#
# The boxscore-player parser does the most work — ESPN ships the stats
# block as parallel arrays (``keys`` / ``labels`` / ``stats``) which need
# to be zipped per athlete to produce a tidy long-form frame.


def parse_summary_boxscore_player(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract per-athlete boxscore stats from a Site v2 summary payload.

    Walks ``payload["boxscore"]["players"]`` (one entry per team), zips
    each ``statistics`` block's ``keys`` / ``athletes[].stats`` parallel
    arrays into one row per athlete with stat columns named by the
    block's ``keys`` (e.g. ``min``, ``fg``, ``3pt``, ``ft``, ``reb``,
    ``ast``, ``stl``, ``blk``, ``to``, ``pf``, ``pm``, ``pts``).

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()``
            wrapper (or :func:`_site_v2_summary` core call).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per (team × athlete)
        plus columns for team identifiers, athlete identifiers, and the
        flat stat values. Zero rows when the payload lacks
        ``boxscore.players``.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    bs = payload.get("boxscore") or {}
    teams = bs.get("players") or []
    if not isinstance(teams, list) or not teams:
        return _empty_frame(return_as_pandas)

    rows = []
    for entry in teams:
        team = (entry or {}).get("team") or {}
        team_row_base = {
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
            "team_display_name": team.get("displayName"),
            "team_location": team.get("location"),
        }
        for stat_block in entry.get("statistics") or []:
            keys = stat_block.get("keys") or stat_block.get("names") or []
            for athlete_row in stat_block.get("athletes") or []:
                ath = athlete_row.get("athlete") or {}
                row = dict(team_row_base)
                row.update({
                    "athlete_id": ath.get("id"),
                    "athlete_display_name": ath.get("displayName"),
                    "athlete_short_name": ath.get("shortName"),
                    "athlete_jersey": ath.get("jersey"),
                    "athlete_position": (ath.get("position") or {}).get("abbreviation"),
                    "starter": athlete_row.get("starter"),
                    "active": athlete_row.get("active"),
                    "did_not_play": athlete_row.get("didNotPlay"),
                    "ejected": athlete_row.get("ejected"),
                    "reason": athlete_row.get("reason"),
                })
                stats = athlete_row.get("stats") or []
                for k, v in zip(keys, stats):
                    row[k] = v
                rows.append(row)

    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_boxscore_team(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract per-team boxscore stats from a Site v2 summary payload.

    Walks ``payload["boxscore"]["teams"]`` (one entry per team) and
    unrolls each team's ``statistics`` array into one row per
    (team × stat) with ``team_*`` identifiers plus ``stat_name``,
    ``stat_label``, ``stat_display_value``.

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per (team × stat).
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    bs = payload.get("boxscore") or {}
    teams = bs.get("teams") or []
    if not isinstance(teams, list) or not teams:
        return _empty_frame(return_as_pandas)

    rows = []
    for entry in teams:
        team = (entry or {}).get("team") or {}
        team_row_base = {
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
            "team_display_name": team.get("displayName"),
            "home_away": entry.get("homeAway"),
            "display_order": entry.get("displayOrder"),
        }
        for stat in entry.get("statistics") or []:
            row = dict(team_row_base)
            row.update({
                "stat_name": stat.get("name"),
                "stat_label": stat.get("label"),
                "stat_display_value": stat.get("displayValue"),
                "stat_value": stat.get("value"),
            })
            rows.append(row)

    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_plays(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the play-by-play list from a Site v2 summary payload.

    Site v2 ``summary`` ships a complete PBP at ``payload["plays"]``
    (typically 350-500 rows per game). One row per play with id,
    sequenceNumber, type, text, awayScore, homeScore, period, clock,
    scoringPlay, scoreValue, team identifiers.

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per play.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    plays = payload.get("plays")
    if not isinstance(plays, list) or not plays:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(plays, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    # Stringify the participants list so polars accepts the frame.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_winprobability(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract win-probability over time from a Site v2 summary payload.

    Site v2 ``summary`` ships per-play win probabilities at
    ``payload["winprobability"]`` (parallel array to ``plays``, same
    length, joined by ``play_id``).

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per probability tick
        carrying ``home_win_percentage``, ``tie_percentage``, ``play_id``.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    wp = payload.get("winprobability")
    if not isinstance(wp, list) or not wp:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(wp, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_leaders(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract per-game stat leaders from a Site v2 summary payload.

    Site v2 ``summary`` ships per-team-per-category leaders at
    ``payload["leaders"]``. Shape: ``[{team, leaders: [{name, displayName,
    leaders: [{athlete, displayValue, value, statistics, mainStat,
    summary}, ...]}, ...]}, ...]``. This parser walks the 3-level
    nesting to produce one row per (team × category × leader).

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per (team × category ×
        leader-rank).
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    teams = payload.get("leaders")
    if not isinstance(teams, list) or not teams:
        return _empty_frame(return_as_pandas)

    rows = []
    for team_entry in teams:
        team = (team_entry or {}).get("team") or {}
        team_row_base = {
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
        }
        for category in team_entry.get("leaders") or []:
            cat_name = category.get("name")
            cat_display = category.get("displayName")
            for leader in category.get("leaders") or []:
                ath = leader.get("athlete") or {}
                row = dict(team_row_base)
                row.update({
                    "category_name": cat_name,
                    "category_display_name": cat_display,
                    "athlete_id": ath.get("id"),
                    "athlete_display_name": ath.get("displayName"),
                    "athlete_position": (ath.get("position") or {}).get("abbreviation"),
                    "value": leader.get("value"),
                    "display_value": leader.get("displayValue"),
                    "main_stat": leader.get("mainStat"),
                    "summary": leader.get("summary"),
                })
                rows.append(row)

    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def _single_row(payload_dict: Dict, return_as_pandas: bool):
    """Flatten a dict to a single-row frame; zero-row frame on empty."""
    if not isinstance(payload_dict, dict) or not payload_dict:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(payload_dict, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    # Stringify any list-valued cells so polars can ingest.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def _row_per_item(items, return_as_pandas: bool):
    """Flatten a list-of-dicts to one row per item; zero-row on empty."""
    if not isinstance(items, list) or not items:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(items, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_game_info(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the venue + attendance from a summary payload's ``gameInfo``.

    ``gameInfo`` carries ``{venue: {...}, attendance: int, officials: [...]}``.
    This parser returns a single-row frame with the venue / attendance
    flattened; call :func:`parse_summary_officials` for the officials list.
    """
    info = (payload or {}).get("gameInfo") or {}
    if not info:
        return _empty_frame(return_as_pandas)
    flat = {"attendance": info.get("attendance")}
    venue = info.get("venue") or {}
    for k, v in venue.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            flat[f"venue_{k}"] = v
        elif isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, (str, int, float, bool)) or v2 is None:
                    flat[f"venue_{k}_{k2}"] = v2
    return _single_row(flat, return_as_pandas)


def parse_summary_officials(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the per-game officials list from ``gameInfo.officials``.

    One row per official with ``full_name``, ``display_name``,
    ``position``, ``order``.
    """
    officials = ((payload or {}).get("gameInfo") or {}).get("officials")
    return _row_per_item(officials, return_as_pandas)


def parse_summary_header(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the single-row game header from ``payload["header"]``.

    The ``header`` carries event id / uid / season / league plus a
    ``competitions[0]`` sub-dict with the game state. The competitions
    list is one element per game and is flattened as
    ``competitions_0_*`` columns.
    """
    return _single_row(payload.get("header") if isinstance(payload, dict) else None,
                       return_as_pandas)


def parse_summary_season_series(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract head-to-head season series context from ``payload["seasonseries"]``.

    Each entry summarises a head-to-head series between the two teams
    with ``type``, ``title``, ``description``, ``summary``, ``completed``,
    ``totalCompetitions``, ``seriesLabel``, ``seriesScore``. One row per
    series entry.
    """
    series = (payload or {}).get("seasonseries")
    return _row_per_item(series, return_as_pandas)


def parse_summary_against_the_spread(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract per-team ATS records from ``payload["againstTheSpread"]``.

    Shape: ``[{team: {...}, records: [{...}, ...]}, ...]``. Walks both
    levels to produce one row per (team × record) with ``team_id`` /
    ``team_abbreviation`` plus the flattened record fields.
    """
    teams = (payload or {}).get("againstTheSpread")
    if not isinstance(teams, list) or not teams:
        return _empty_frame(return_as_pandas)
    rows = []
    for entry in teams:
        team = (entry or {}).get("team") or {}
        team_base = {
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
            "team_display_name": team.get("displayName"),
        }
        for rec in entry.get("records") or []:
            row = dict(team_base)
            for k, v in (rec or {}).items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    row[k] = v
                elif isinstance(v, dict):
                    for k2, v2 in v.items():
                        if isinstance(v2, (str, int, float, bool)) or v2 is None:
                            row[f"{k}_{k2}"] = v2
            rows.append(row)
    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_standings(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract a standings snapshot from ``payload["standings"]``.

    The ``standings`` section ships
    ``{header, groups: [{standings: {entries: [...]}, ...}, ...]}``.
    Each group's ``standings.entries[]`` is one row per team; this
    parser stacks all groups, adding a ``group_header`` /
    ``conference_header`` / ``division_header`` column to identify
    which conference/division each row belongs to.
    """
    st = (payload or {}).get("standings") or {}
    groups = st.get("groups") or []
    if not isinstance(groups, list) or not groups:
        return _empty_frame(return_as_pandas)
    rows = []
    for grp in groups:
        if not isinstance(grp, dict):
            continue
        grp_base = {
            "group_header": grp.get("header"),
            "conference_header": grp.get("conferenceHeader"),
            "division_header": grp.get("divisionHeader"),
        }
        for entry in ((grp.get("standings") or {}).get("entries") or []):
            row = dict(grp_base)
            # Standings entries put team identifiers at the top level
            # and the team's *display location* in the ``team`` string
            # (e.g. "Boston").  The team-dict shape lives on the
            # leaders / boxscore endpoints, not here.
            team_field = entry.get("team")
            row["team_id"] = entry.get("id")
            row["team_uid"] = entry.get("uid")
            row["team_location"] = team_field if isinstance(team_field, str) else None
            if isinstance(team_field, dict):
                row["team_abbreviation"] = team_field.get("abbreviation")
                row["team_display_name"] = team_field.get("displayName")
            for stat in entry.get("stats") or []:
                key = stat.get("name") or stat.get("type")
                if key:
                    row[key] = stat.get("displayValue", stat.get("value"))
            rows.append(row)
    if not rows:
        return _empty_frame(return_as_pandas)
    df = pd.DataFrame(rows)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


def parse_summary_broadcasts(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract TV broadcast info from ``payload["broadcasts"]``.

    Site v2 ``summary`` ships ``broadcasts: [...]`` (often empty for
    past games). Each entry typically has ``type, market, media, lang,
    region``. One row per broadcast; zero-row frame when the list is
    absent or empty.
    """
    return _row_per_item((payload or {}).get("broadcasts"), return_as_pandas)


def parse_summary_format(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the game format from ``payload["format"]``.

    ``format`` carries ``{regulation: {periods, displayName, slug, clock},
    overtime: {displayName, slug, clock}}``. Flattens to a single-row
    frame with ``regulation_*`` and ``overtime_*`` columns.
    """
    return _single_row(payload.get("format") if isinstance(payload, dict) else None,
                       return_as_pandas)


def parse_summary_pickcenter(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract pre-game odds / picks from ``payload["pickcenter"]``.

    ``pickcenter`` ships one entry per book / provider with line, odds,
    spread, over/under, and team-specific moneylines. Empty for many
    past games; this parser returns a zero-row frame when the list is
    absent.
    """
    return _row_per_item((payload or {}).get("pickcenter"), return_as_pandas)


def parse_summary_odds(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract odds entries from ``payload["odds"]``.

    Similar shape to ``pickcenter`` but typically a flatter list of
    provider × market entries.  Empty for many past games.
    """
    return _row_per_item((payload or {}).get("odds"), return_as_pandas)


def parse_summary_article(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the recap article metadata from ``payload["article"]``.

    The ``article`` section is a single rich dict (~27 fields) with
    ``id, headline, description, byline, published, links, images``.
    Flattens to one row.
    """
    return _single_row(payload.get("article") if isinstance(payload, dict) else None,
                       return_as_pandas)


def parse_summary_injuries(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract per-team injuries from ``payload["injuries"]``.

    Same shape as the standalone ``espn_{league}_injuries()`` payload —
    each row is one team with a nested ``injuries`` list (stringified
    for polars). Call ``parse_injuries(payload)`` on the standalone
    endpoint or on ``{"injuries": payload["injuries"]}`` for the same
    output; this parser is a convenience that handles the unwrap.
    """
    return _row_per_item((payload or {}).get("injuries"), return_as_pandas)


def parse_summary_news(
    payload: Dict, return_as_pandas: bool = False
) -> pl.DataFrame:
    """Extract the embedded news feed from ``payload["news"]``.

    ``summary.news`` has the same shape as the standalone
    ``espn_{league}_news()`` payload (``{header, link, articles}``).
    Returns one row per article from ``news.articles``.
    """
    news = (payload or {}).get("news") or {}
    return _row_per_item(news.get("articles"), return_as_pandas)


def parse_summary(payload: Dict, section: str = None,
                  return_as_pandas: bool = False):
    """Dispatcher: parse one section of a Site v2 summary payload.

    With ``section=None`` (default), returns a ``dict`` of every parsable
    sub-frame keyed by section name (17 sections currently). Sections
    that are empty in the payload are still present with zero-row frames
    so downstream code can rely on the key set.

    With ``section="<name>"``, returns just that one frame.

    Args:
        payload: Raw JSON dict from any ``espn_{league}_summary()`` wrapper.
        section: Optional section name; see :data:`SUMMARY_SECTION_PARSERS`
            for the full list.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        Dict[str, pl.DataFrame] (or pandas) when ``section`` is None;
        a single DataFrame when a section name is provided.

    Raises:
        ValueError: If ``section`` is not a recognised name.
    """
    if section is not None:
        if section not in SUMMARY_SECTION_PARSERS:
            raise ValueError(
                f"Unknown summary section {section!r}. "
                f"Choose one of {sorted(SUMMARY_SECTION_PARSERS)} or pass "
                f"section=None for the full dict.",
            )
        return SUMMARY_SECTION_PARSERS[section](payload, return_as_pandas=return_as_pandas)
    return {
        name: fn(payload, return_as_pandas=return_as_pandas)
        for name, fn in SUMMARY_SECTION_PARSERS.items()
    }


# Map summary section name -> parser. Used by parse_summary() and exposed
# so callers can introspect the section list.
SUMMARY_SECTION_PARSERS = {
    "boxscore_player":      parse_summary_boxscore_player,
    "boxscore_team":        parse_summary_boxscore_team,
    "plays":                parse_summary_plays,
    "winprobability":       parse_summary_winprobability,
    "leaders":              parse_summary_leaders,
    "game_info":            parse_summary_game_info,
    "officials":            parse_summary_officials,
    "header":               parse_summary_header,
    "season_series":        parse_summary_season_series,
    "against_the_spread":   parse_summary_against_the_spread,
    "standings":            parse_summary_standings,
    "broadcasts":           parse_summary_broadcasts,
    "format":               parse_summary_format,
    "pickcenter":           parse_summary_pickcenter,
    "odds":                 parse_summary_odds,
    "article":              parse_summary_article,
    "injuries":             parse_summary_injuries,
    "news":                 parse_summary_news,
}


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
    # Site v2 (rich nested)
    "scoreboard": parse_scoreboard,
    "teams_site": parse_teams,
    # summary is the dispatcher — returns dict of sub-frames by default
    "summary": parse_summary,
    # Site v2 alt + Core v2 standings
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
    # Core v2 catalog (one-shot)
    "teams_core": parse_teams,
    "coaches": parse_coaches,
    "season_coaches": parse_coaches,
    "season_draft": parse_draft,
    # Event-competitor surface
    "event_competitor_roster": parse_event_competitor_roster,
    "event_competitor_statistics": parse_event_competitor_statistics,
    "event_competitor_linescores": parse_event_competitor_linescores,
    "event_plays": parse_event_plays,
    # Team-scoped Site v2
    "team_schedule": parse_team_schedule,
    "team_roster": parse_team_roster,
    # News (league-wide + team + athlete scoped)
    "news": parse_news,
    "team_news": parse_news,
    "athlete_news": parse_news,
    # Injuries (league-wide + team + athlete scoped)
    "injuries": parse_injuries,
    "team_injuries": parse_injuries,
    "athlete_injuries": parse_injuries,
    # Core v2 paginated list endpoints — parse_items returns a frame of
    # raw items (often $ref-only on Core v2). The generic shape covers
    # ~30 short names; only the well-known ones are registered here.
    "venues": parse_items,
    "franchises": parse_items,
    "events": parse_items,
    "athletes_index": parse_items,
    "seasons": parse_items,
    "season_types": parse_items,
    "season_groups": parse_items,
    "season_group_teams": parse_items,
    "season_teams": parse_items,
    "season_athletes": parse_items,
    "season_weeks": parse_items,
    "season_week_events": parse_items,
    "season_awards": parse_items,
    "season_recruits": parse_items,
    "season_futures": parse_items,
    "season_freeagents": parse_items,
    "season_draft_round_picks": parse_items,
    "awards": parse_items,
    "tournaments": parse_items,
    "positions": parse_items,
    "transactions": parse_items,
    "team_transactions": parse_items,
    "team_record": parse_items,
    "team_history": parse_items,
    "athlete_career_stats": parse_items,
    "athlete_statisticslog": parse_items,
    "athlete_eventlog": parse_items,
    "athlete_contracts": parse_items,
    "athlete_awards": parse_items,
    "athlete_seasons": parse_items,
    "athlete_records": parse_items,
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
