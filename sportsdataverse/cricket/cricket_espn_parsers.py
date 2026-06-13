"""ESPN cricket parsers — tidy polars frames for the cricket endpoint family.

Contract (shared with the universal parsers): return a polars.DataFrame by default,
pandas when return_as_pandas=True; empty/malformed payloads return a zero-row frame
(never raise); columns are snake_cased.

Cricket-specific notes:
  * ``matchcards`` in the summary payload carries three distinct scorecard shapes
    (Batting, Bowling, Partnerships) emitted by ``parse_cricket_summary`` as three
    separate frames keyed ``matchcards_batting``, ``matchcards_bowling``,
    ``matchcards_partnerships``.
  * Standings use a ``children`` hierarchy (not ``groups``), flattened with a
    ``group`` column identical to the soccer convention.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _out(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    return df.to_pandas() if return_as_pandas else df


def _stringify_lists(rows: list[dict]) -> list[dict]:
    return [{k: str(v) if isinstance(v, list) else v for k, v in row.items()} for row in rows]


def _competitor(comp: dict, home_away: str) -> dict:
    for c in comp.get("competitors", []) or []:
        if c.get("homeAway") == home_away:
            return c
    return {}


# ---------------------------------------------------------------------------
# Scoreboard
# ---------------------------------------------------------------------------


def parse_cricket_scoreboard(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN cricket scoreboard payload into a tidy flat DataFrame.

    Each row is one match. Score strings retain the cricket format
    (e.g. ``"161/5 (18/20 ov, target 156)"``).

    Args:
        payload: Raw dict from an ESPN cricket ``scoreboard`` endpoint.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.cricket import espn_cricket_scoreboard
            from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_scoreboard
            df = parse_cricket_scoreboard(espn_cricket_scoreboard(league="8048", return_parsed=False))
            print(df.shape)
    """
    events = (payload or {}).get("events") if isinstance(payload, dict) else None
    rows = []
    for ev in events or []:
        comp = (ev.get("competitions") or [{}])[0]
        home = _competitor(comp, "home")
        away = _competitor(comp, "away")
        status_type = (ev.get("status") or {}).get("type") or {}
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
                "status": status_type.get("name"),
                "status_detail": status_type.get("detail"),
                "venue": (comp.get("venue") or {}).get("fullName"),
                "neutral_site": comp.get("neutralSite"),
            }
        )
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(_stringify_lists(rows)), return_as_pandas)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def parse_cricket_standings(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN cricket standings payload into a tidy flat DataFrame.

    Each row is one team in one group/division. The ``group`` column holds
    the child conference/group name so multi-group tournaments (World Cup
    group stages) can be filtered directly.

    Args:
        payload: Raw dict from an ESPN cricket ``standings`` endpoint.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.cricket import espn_cricket_standings
            from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_standings
            df = parse_cricket_standings(espn_cricket_standings(league="8048", return_parsed=False))
            print(df.columns)
    """
    children = (payload or {}).get("children") if isinstance(payload, dict) else None
    rows: list[dict] = []
    for child in children or []:
        group = child.get("name")
        standings_block = child.get("standings") or {}
        entries = standings_block.get("entries") or []
        for entry in entries:
            team = entry.get("team") or {}
            row: dict = {
                "group": group,
                "team": team.get("displayName"),
                "team_id": team.get("id"),
                "team_abbreviation": team.get("abbreviation"),
            }
            for stat in entry.get("stats") or []:
                col = underscore(stat.get("name", ""))
                val = stat.get("value")
                if val is None:
                    val = stat.get("displayValue")
                row[col] = val
            rows.append(row)
    if not rows:
        return _out(pl.DataFrame(), return_as_pandas)
    return _out(pl.DataFrame(_stringify_lists(rows)), return_as_pandas)


# ---------------------------------------------------------------------------
# Summary sub-builders
# ---------------------------------------------------------------------------


def _build_cricket_header(payload: dict) -> pl.DataFrame:
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


def _build_cricket_matchcards_batting(payload: dict) -> pl.DataFrame:
    try:
        rows = []
        for mc in payload.get("matchcards") or []:
            if mc.get("headline") != "Batting":
                continue
            team_name = mc.get("teamName")
            innings_number = mc.get("inningsNumber")
            total = mc.get("total")
            runs = mc.get("runs")
            extras = mc.get("extras")
            for player in mc.get("playerDetails") or []:
                rows.append(
                    {
                        "innings_number": innings_number,
                        "team_name": team_name,
                        "total": total,
                        "runs_total": runs,
                        "extras": extras,
                        "player_id": player.get("playerID"),
                        "player_name": player.get("playerName"),
                        "dismissal": player.get("dismissal"),
                        "runs": player.get("runs"),
                        "balls_faced": player.get("ballsFaced"),
                        "fours": player.get("fours"),
                        "sixes": player.get("sixes"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_cricket_matchcards_bowling(payload: dict) -> pl.DataFrame:
    try:
        rows = []
        for mc in payload.get("matchcards") or []:
            if mc.get("headline") != "Bowling":
                continue
            team_name = mc.get("teamName")
            innings_number = mc.get("inningsNumber")
            for player in mc.get("playerDetails") or []:
                rows.append(
                    {
                        "innings_number": innings_number,
                        "team_name": team_name,
                        "player_id": player.get("playerID"),
                        "player_name": player.get("playerName"),
                        "overs": player.get("overs"),
                        "maidens": player.get("maidens"),
                        "conceded": player.get("conceded"),
                        "wickets": player.get("wickets"),
                        "economy_rate": player.get("economyRate"),
                        "nbw": player.get("nbw"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_cricket_matchcards_partnerships(payload: dict) -> pl.DataFrame:
    try:
        rows = []
        for mc in payload.get("matchcards") or []:
            if mc.get("headline") not in ("Partnerships", "Fall of Wickets"):
                continue
            team_name = mc.get("teamName")
            innings_number = mc.get("inningsNumber")
            for entry in mc.get("playerDetails") or []:
                rows.append(
                    {
                        "innings_number": innings_number,
                        "team_name": team_name,
                        "partnership_runs": entry.get("partnershipRuns"),
                        "partnership_overs": entry.get("partnershipOvers"),
                        "wicket_name": entry.get("partnershipWicketName"),
                        "fow_type": entry.get("fowType"),
                        "player1_name": entry.get("player1Name"),
                        "player1_runs": entry.get("player1Runs"),
                        "player2_name": entry.get("player2Name"),
                        "player2_runs": entry.get("player2Runs"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_cricket_rosters(payload: dict) -> pl.DataFrame:
    try:
        rows = []
        for entry in payload.get("rosters") or []:
            team = entry.get("team") or {}
            team_id = team.get("id")
            home_away = entry.get("homeAway")
            winner = entry.get("winner")
            for player in entry.get("roster") or []:
                athlete = player.get("athlete") or {}
                pos = player.get("position") or {}
                rows.append(
                    {
                        "team_id": team_id,
                        "home_away": home_away,
                        "winner": winner,
                        "athlete_id": athlete.get("id"),
                        "athlete": athlete.get("displayName"),
                        "jersey": player.get("jersey"),
                        "starter": player.get("starter"),
                        "position": pos.get("abbreviation"),
                        "captain": player.get("captain"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_cricket_game_info(payload: dict) -> pl.DataFrame:
    try:
        gi = payload.get("gameInfo")
        if not gi or not isinstance(gi, dict):
            return pl.DataFrame()
        venue = gi.get("venue") or {}
        address = venue.get("address") or {}
        officials = gi.get("officials") or []
        row: dict = {
            "venue_id": venue.get("id"),
            "venue_full_name": venue.get("fullName"),
            "venue_short_name": venue.get("shortName"),
            "venue_city": address.get("city"),
            "venue_country": address.get("country"),
            "attendance": gi.get("attendance"),
            "officials": str(officials) if officials else None,
        }
        return pl.DataFrame([row])
    except Exception:
        return pl.DataFrame()


def _build_cricket_leaders(payload: dict) -> pl.DataFrame:
    try:
        team_leaders = payload.get("leaders") or []
        rows = []
        for team_entry in team_leaders:
            team_block = team_entry.get("team") or {}
            team_id = team_block.get("id")
            team_name = team_block.get("displayName")
            for category in team_entry.get("leaders") or []:
                cat_name = category.get("displayName")
                cat_slug = category.get("name")
                for leader in category.get("leaders") or []:
                    athlete = leader.get("athlete") or {}
                    rows.append(
                        {
                            "team_id": team_id,
                            "team_name": team_name,
                            "category": cat_name,
                            "category_slug": cat_slug,
                            "athlete_id": athlete.get("id"),
                            "athlete": athlete.get("displayName"),
                            "value": leader.get("displayValue"),
                            "summary": leader.get("summary"),
                        }
                    )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_cricket_standings_summary(payload: dict) -> pl.DataFrame:
    """Flatten the ``standings`` block embedded in a summary payload.

    Uses the same ``children`` hierarchy as the standalone standings endpoint.
    """
    try:
        st = payload.get("standings") or {}
        children = st.get("children") or []
        rows = []
        for child in children:
            group = child.get("name")
            entries = (child.get("standings") or {}).get("entries") or []
            for entry in entries:
                team = entry.get("team") or {}
                row: dict = {
                    "group": group,
                    "team": team.get("displayName") if isinstance(team, dict) else team,
                    "team_id": team.get("id") if isinstance(team, dict) else None,
                }
                for stat in entry.get("stats") or []:
                    col = underscore(stat.get("name", ""))
                    val = stat.get("value")
                    if val is None:
                        val = stat.get("displayValue")
                    row[col] = val
                rows.append(row)
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


_CRICKET_SUMMARY_BUILDERS: Dict[str, Any] = {
    "header": _build_cricket_header,
    "matchcards_batting": _build_cricket_matchcards_batting,
    "matchcards_bowling": _build_cricket_matchcards_bowling,
    "matchcards_partnerships": _build_cricket_matchcards_partnerships,
    "rosters": _build_cricket_rosters,
    "game_info": _build_cricket_game_info,
    "leaders": _build_cricket_leaders,
    "standings": _build_cricket_standings_summary,
}


def parse_cricket_summary(
    payload: Any,
    section: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[Dict[str, pl.DataFrame], pl.DataFrame, "pd.DataFrame"]:
    """Parse an ESPN cricket summary payload into tidy polars frames.

    With ``section=None`` (default) returns a dict of all section DataFrames.
    With ``section="<name>"`` returns just that one frame. Unknown section
    names return a zero-row frame (never raise).

    Sections: ``header``, ``matchcards_batting``, ``matchcards_bowling``,
    ``matchcards_partnerships``, ``rosters``, ``game_info``, ``leaders``,
    ``standings``.

    Args:
        payload: Raw dict from an ESPN cricket ``summary`` endpoint.
        section: Optional section name to return a single DataFrame.
        return_as_pandas: When True, return :class:`pandas.DataFrame` instead
            of polars (only applies to single-section requests).

    Returns:
        ``Dict[str, pl.DataFrame]`` when ``section`` is None;
        a single DataFrame when a section name is provided.

    Example:
        Quick start::

            from sportsdataverse.cricket import espn_cricket_summary
            from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary
            raw = espn_cricket_summary(league="8048", event_id=1535465, return_parsed=False)
            frames = parse_cricket_summary(raw)
            print(frames["matchcards_batting"].shape)

        Single section::

            batting = parse_cricket_summary(raw, section="matchcards_batting")
            print(batting.head())
    """
    p = payload if isinstance(payload, dict) else {}
    if section is not None:
        builder = _CRICKET_SUMMARY_BUILDERS.get(section)
        if builder is None:
            return _out(pl.DataFrame(), return_as_pandas)
        return _out(builder(p), return_as_pandas)
    return {name: fn(p) for name, fn in _CRICKET_SUMMARY_BUILDERS.items()}
