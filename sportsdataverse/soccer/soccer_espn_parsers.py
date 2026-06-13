"""ESPN soccer parsers — tidy polars frames for the soccer endpoint family.

Contract (shared with the universal parsers): return a polars.DataFrame by default,
pandas when return_as_pandas=True; empty/malformed payloads return a zero-row frame
(never raise); columns are snake_cased.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def _competitor(comp: dict, home_away: str) -> dict:
    for c in comp.get("competitors", []) or []:
        if c.get("homeAway") == home_away:
            return c
    return {}


def parse_soccer_standings(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN soccer standings payload into a tidy flat DataFrame.

    Each row is one team in one group/conference. The ``group`` column holds
    the child (group/conference) name so multi-group leagues (MLS, World Cup
    groups) can be filtered directly.

    Args:
        payload: Raw dict from an ESPN ``standings`` endpoint response.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.soccer import espn_epl_standings
            df = espn_epl_standings(return_parsed=True)
            print(df.shape)
    """
    children = (payload or {}).get("children") if isinstance(payload, dict) else None
    rows: list[dict] = []
    for child in children or []:
        group = child.get("name")
        standings_block = child.get("standings", {})
        entries = standings_block.get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            note = entry.get("note") or {}
            row: dict = {
                "group": group,
                "team": team.get("displayName"),
                "team_id": team.get("id"),
                "team_abbreviation": team.get("abbreviation"),
                "note": note.get("description") if isinstance(note, dict) else None,
            }
            for stat in entry.get("stats") or []:
                col = underscore(stat["name"])
                val = stat.get("value")
                if val is None:
                    val = stat.get("displayValue")
                row[col] = val
            rows.append(row)
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(rows), return_as_pandas)


def parse_soccer_scoreboard(payload: Any, *, return_as_pandas: bool = False):
    events = (payload or {}).get("events") if isinstance(payload, dict) else None
    rows = []
    for ev in events or []:
        comp = (ev.get("competitions") or [{}])[0]
        home, away = _competitor(comp, "home"), _competitor(comp, "away")
        rows.append(
            {
                "event_id": ev.get("id"),
                "date": ev.get("date"),
                "name": ev.get("name"),
                "short_name": ev.get("shortName"),
                "home_team": (home.get("team") or {}).get("displayName"),
                "home_team_id": (home.get("team") or {}).get("id"),
                "home_score": home.get("score"),
                "away_team": (away.get("team") or {}).get("displayName"),
                "away_team_id": (away.get("team") or {}).get("id"),
                "away_score": away.get("score"),
                "status": ((ev.get("status") or {}).get("type") or {}).get("name"),
                "venue": (comp.get("venue") or {}).get("fullName"),
            }
        )
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(rows), return_as_pandas)


# ---------------------------------------------------------------------------
# Soccer summary dispatcher
# ---------------------------------------------------------------------------


def _stringify_lists(rows: list[dict]) -> list[dict]:
    """Stringify any list-valued cells so polars accepts the frame."""
    out = []
    for row in rows:
        out.append({k: str(v) if isinstance(v, list) else v for k, v in row.items()})
    return out


def _build_header(payload: dict) -> pl.DataFrame:
    try:
        hd = payload.get("header") or {}
        if not hd:
            return pl.DataFrame()
        row: dict = {
            "id": hd.get("id"),
            "uid": hd.get("uid"),
            "time_valid": hd.get("timeValid"),
        }
        season = hd.get("season") or {}
        row["season_year"] = season.get("year")
        row["season_type"] = season.get("type")
        row["season_slug"] = season.get("slug")
        league = hd.get("league") or {}
        row["league_id"] = league.get("id")
        row["league_name"] = league.get("name")
        row["league_abbreviation"] = league.get("abbreviation")
        comps = hd.get("competitions") or []
        if comps:
            comp = comps[0]
            row["competition_id"] = comp.get("id")
            row["competition_date"] = comp.get("date")
            row["neutral_site"] = comp.get("neutralSite")
            status = comp.get("status") or {}
            status_type = status.get("type") or {}
            row["status_name"] = status_type.get("name")
            row["status_description"] = status_type.get("description")
            row["is_final"] = comp.get("isFinal")
        return pl.DataFrame([row])
    except Exception:
        return pl.DataFrame()


def _build_lineups(payload: dict) -> pl.DataFrame:
    try:
        rosters = payload.get("rosters") or []
        rows = []
        for entry in rosters:
            team = entry.get("team") or {}
            team_id = team.get("id")
            home_away = entry.get("homeAway")
            for player in entry.get("roster") or []:
                athlete = player.get("athlete") or {}
                pos = player.get("position") or {}
                rows.append(
                    {
                        "team_id": team_id,
                        "home_away": home_away,
                        "athlete": athlete.get("displayName"),
                        "athlete_id": athlete.get("id"),
                        "position": pos.get("abbreviation"),
                        "starter": player.get("starter"),
                        "jersey": player.get("jersey"),
                        "formation_place": player.get("formationPlace"),
                        "subbed_in": player.get("subbedIn"),
                        "subbed_out": player.get("subbedOut"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_key_events(payload: dict) -> pl.DataFrame:
    try:
        events = payload.get("keyEvents") or []
        rows = []
        for ev in events:
            type_block = ev.get("type") or {}
            clock_block = ev.get("clock") or {}
            team_block = ev.get("team") or {}
            participants = ev.get("participants") or []
            primary_athlete_id = None
            primary_athlete_name = None
            if participants:
                primary_ath = participants[0].get("athlete") or {}
                primary_athlete_id = primary_ath.get("id")
                primary_athlete_name = primary_ath.get("displayName")
            rows.append(
                {
                    "id": ev.get("id"),
                    "type": type_block.get("text"),
                    "type_id": type_block.get("id"),
                    "type_slug": type_block.get("type"),
                    "text": ev.get("text"),
                    "short_text": ev.get("shortText"),
                    "clock": clock_block.get("displayValue"),
                    "clock_value": clock_block.get("value"),
                    "period": (ev.get("period") or {}).get("number"),
                    "team_id": team_block.get("id"),
                    "team_name": team_block.get("displayName"),
                    "scoring_play": ev.get("scoringPlay"),
                    "athlete_id": primary_athlete_id,
                    "athlete_name": primary_athlete_name,
                    "wallclock": ev.get("wallclock"),
                }
            )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_team_stats(payload: dict) -> pl.DataFrame:
    try:
        bx = payload.get("boxscore") or {}
        teams = bx.get("teams") or []
        rows = []
        for t in teams:
            team = t.get("team") or {}
            row: dict = {
                "team_id": team.get("id"),
                "team_name": team.get("displayName"),
                "team_abbreviation": team.get("abbreviation"),
                "home_away": t.get("homeAway"),
            }
            for stat in t.get("statistics") or []:
                col = underscore(stat["name"])
                val = stat.get("displayValue")
                if val is None:
                    val = stat.get("value")
                row[col] = val
            rows.append(row)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


_SOCCER_SUMMARY_BUILDERS: Dict[str, Any] = {
    "header": _build_header,
    "lineups": _build_lineups,
    "key_events": _build_key_events,
    "team_stats": _build_team_stats,
}


def parse_soccer_summary(
    payload: Any,
    section: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[Dict[str, pl.DataFrame], pl.DataFrame, "pd.DataFrame"]:
    """Parse an ESPN soccer summary payload into tidy polars frames.

    With ``section=None`` (default), returns a dict of all four section
    DataFrames keyed by section name. With ``section="<name>"``, returns
    just that one frame. Unknown section names return a zero-row frame
    (never raise).

    Sections: ``header``, ``lineups``, ``key_events``, ``team_stats``.

    Args:
        payload: Raw dict from an ESPN ``summary`` endpoint response.
        section: Optional section name.
        return_as_pandas: When True, return :class:`pandas.DataFrame` instead
            of polars (only applies when a single section is requested).

    Returns:
        ``Dict[str, pl.DataFrame]`` when ``section`` is None;
        a single DataFrame when a section name is provided.

    Example:
        Quick start::

            from sportsdataverse.soccer import espn_epl_summary
            raw = espn_epl_summary(event=740966)
            from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary
            frames = parse_soccer_summary(raw)
            print(frames["key_events"].shape)

        Single section::

            ke = parse_soccer_summary(raw, section="key_events")
            print(ke.head())
    """
    p = payload if isinstance(payload, dict) else {}
    if section is not None:
        builder = _SOCCER_SUMMARY_BUILDERS.get(section)
        if builder is None:
            return _out(pl.DataFrame(), return_as_pandas)
        return _out(builder(p), return_as_pandas)
    return {name: fn(p) for name, fn in _SOCCER_SUMMARY_BUILDERS.items()}
