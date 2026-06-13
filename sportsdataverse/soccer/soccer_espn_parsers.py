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


def _build_commentary(payload: dict) -> pl.DataFrame:
    try:
        items = payload.get("commentary") or []
        rows = []
        for item in items:
            time_block = item.get("time") or {}
            rows.append(
                {
                    "sequence": item.get("sequence"),
                    "time_display": time_block.get("displayValue"),
                    "time_value": time_block.get("value"),
                    "text": item.get("text"),
                }
            )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_leaders(payload: dict) -> pl.DataFrame:
    """Flatten payload["leaders"] — a 2-element list (one per team).

    Each element: {team: {...}, leaders: [{name, displayName, leaders: [athlete entries]}]}.
    Emits one row per (team, category, athlete) triple.
    """
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
                    main_stat = leader.get("mainStat") or {}
                    rows.append(
                        {
                            "team_id": team_id,
                            "team_name": team_name,
                            "category": cat_name,
                            "category_slug": cat_slug,
                            "athlete_id": athlete.get("id"),
                            "athlete": athlete.get("displayName"),
                            "athlete_position": (athlete.get("position") or {}).get("abbreviation"),
                            "value": leader.get("displayValue"),
                            "main_stat_label": main_stat.get("label"),
                            "main_stat_value": main_stat.get("value"),
                            "summary": leader.get("summary"),
                        }
                    )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_standings_summary(payload: dict) -> pl.DataFrame:
    """Flatten the ``standings`` block embedded in a summary payload.

    The summary standings shape differs from the standalone endpoint:
    it has ``{fullViewLink, header, groups}`` where each group has
    ``{standings: {entries: [...]}, header, href}``.
    Each entry: ``{team (str), link, id, uid, stats [...], logo}``.
    """
    try:
        st = payload.get("standings") or {}
        groups = st.get("groups") or []
        rows = []
        for group in groups:
            group_header = group.get("header")
            entries = (group.get("standings") or {}).get("entries") or []
            for entry in entries:
                row: dict = {
                    "group": group_header,
                    "team": entry.get("team"),
                    "team_id": entry.get("id"),
                    "team_uid": entry.get("uid"),
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


def _build_head_to_head(payload: dict) -> pl.DataFrame:
    """Flatten payload["headToHeadGames"] — list of {team, events[]} per team perspective."""
    try:
        h2h = payload.get("headToHeadGames") or []
        rows = []
        seen: set = set()
        for team_entry in h2h:
            team_block = team_entry.get("team") or {}
            team_id = team_block.get("id")
            for ev in team_entry.get("events") or []:
                ev_id = ev.get("id")
                # deduplicate — same event appears once per team perspective
                if ev_id in seen:
                    continue
                seen.add(ev_id)
                rows.append(
                    {
                        "event_id": ev_id,
                        "game_date": ev.get("gameDate"),
                        "at_vs": ev.get("atVs"),
                        "score": ev.get("score"),
                        "home_team_id": ev.get("homeTeamId"),
                        "away_team_id": ev.get("awayTeamId"),
                        "home_team_score": ev.get("homeTeamScore"),
                        "away_team_score": ev.get("awayTeamScore"),
                        "home_aggregate_score": ev.get("homeAggregateScore"),
                        "away_aggregate_score": ev.get("awayAggregateScore"),
                        "home_shootout_score": ev.get("homeShootoutScore"),
                        "away_shootout_score": ev.get("awayShootoutScore"),
                        "game_result": ev.get("gameResult"),
                        "match_note": ev.get("matchNote"),
                        "competition_name": ev.get("competitionName"),
                        "round_name": ev.get("roundName"),
                        "league_name": ev.get("leagueName"),
                        "league_abbreviation": ev.get("leagueAbbreviation"),
                        "opponent": ev.get("opponent"),
                        "perspective_team_id": team_id,
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_last_five(payload: dict) -> pl.DataFrame:
    """Flatten payload["lastFiveGames"] — list of {team, events[]} per team."""
    try:
        l5 = payload.get("lastFiveGames") or []
        rows = []
        for team_entry in l5:
            team_block = team_entry.get("team") or {}
            team_id = team_block.get("id")
            team_name = team_block.get("displayName")
            display_order = team_entry.get("displayOrder")
            for ev in team_entry.get("events") or []:
                rows.append(
                    {
                        "team_id": team_id,
                        "team_name": team_name,
                        "display_order": display_order,
                        "event_id": ev.get("id"),
                        "game_date": ev.get("gameDate"),
                        "at_vs": ev.get("atVs"),
                        "score": ev.get("score"),
                        "home_team_id": ev.get("homeTeamId"),
                        "away_team_id": ev.get("awayTeamId"),
                        "home_team_score": ev.get("homeTeamScore"),
                        "away_team_score": ev.get("awayTeamScore"),
                        "game_result": ev.get("gameResult"),
                        "competition_name": ev.get("competitionName"),
                        "league_name": ev.get("leagueName"),
                        "league_abbreviation": ev.get("leagueAbbreviation"),
                        "opponent": ev.get("opponent"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def _build_game_info(payload: dict) -> pl.DataFrame:
    """Flatten payload["gameInfo"] into a single-row frame.

    Shape: {venue: {id, fullName, shortName, address, images}, attendance, officials: [...]}.
    Officials list is stringified (may be multiple referees).
    """
    try:
        gi = payload.get("gameInfo")
        if not gi or not isinstance(gi, dict):
            return pl.DataFrame()
        venue = gi.get("venue") or {}
        address = venue.get("address") or {}
        officials = gi.get("officials") or []
        # stringify officials list
        officials_str = str(officials) if officials else None
        row: dict = {
            "venue_id": venue.get("id"),
            "venue_full_name": venue.get("fullName"),
            "venue_short_name": venue.get("shortName"),
            "venue_city": address.get("city"),
            "venue_country": address.get("country"),
            "attendance": gi.get("attendance"),
            "officials": officials_str,
        }
        return pl.DataFrame([row])
    except Exception:
        return pl.DataFrame()


def _build_shootout(payload: dict) -> pl.DataFrame:
    """Flatten payload["shootout"] — list of {id, team, shots[]}.

    One row per shot across both teams. Present only for knockout games
    that go to penalties; returns zero-row frame when absent.
    """
    try:
        items = payload.get("shootout") or []
        rows = []
        for team_entry in items:
            team_name = team_entry.get("team")
            entry_id = team_entry.get("id")
            for shot in team_entry.get("shots") or []:
                rows.append(
                    {
                        "team_entry_id": entry_id,
                        "team_name": team_name,
                        "shot_id": shot.get("id"),
                        "player_id": shot.get("playerId"),
                        "player": shot.get("player"),
                        "shot_number": shot.get("shotNumber"),
                        "did_score": shot.get("didScore"),
                    }
                )
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(_stringify_lists(rows))
    except Exception:
        return pl.DataFrame()


def parse_soccer_teams(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN soccer teams payload into a tidy flat DataFrame.

    Each row is one team in the league. The ``team_id``, ``display_name``,
    and ``abbreviation`` columns are always present; additional metadata
    columns (``location``, ``name``, ``short_display_name``, ``is_active``)
    are included when available.

    Args:
        payload: Raw dict from an ESPN ``teams`` endpoint response.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.soccer import espn_epl_teams_site
            df = espn_epl_teams_site(return_parsed=True)
            print(df.shape)
    """
    try:
        sports = (payload or {}).get("sports") if isinstance(payload, dict) else None
        if not sports:
            return _out(pl.DataFrame(), return_as_pandas)
        leagues = (sports[0] or {}).get("leagues") or []
        if not leagues:
            return _out(pl.DataFrame(), return_as_pandas)
        teams_list = (leagues[0] or {}).get("teams") or []
        rows = []
        for item in teams_list:
            team = item.get("team") or {}
            rows.append(
                {
                    "team_id": team.get("id"),
                    "display_name": team.get("displayName"),
                    "abbreviation": team.get("abbreviation"),
                    "location": team.get("location"),
                    "name": team.get("name"),
                    "short_display_name": team.get("shortDisplayName"),
                    "nickname": team.get("nickname"),
                    "slug": team.get("slug"),
                    "uid": team.get("uid"),
                    "color": team.get("color"),
                    "alternate_color": team.get("alternateColor"),
                    "is_active": team.get("isActive"),
                    "is_all_star": team.get("isAllStar"),
                }
            )
        if not rows:
            return _out(pl.DataFrame(), return_as_pandas)
        return _out(pl.DataFrame(_stringify_lists(rows)), return_as_pandas)
    except Exception:
        return _out(pl.DataFrame(), return_as_pandas)


def parse_soccer_team_roster(payload: Any, *, return_as_pandas: bool = False):
    """Parse an ESPN soccer team roster payload into a tidy flat DataFrame.

    Each row is one athlete on the roster. The ``athlete_id`` and
    ``display_name`` columns are always present; position, jersey, age,
    and birth metadata are included when available.

    Args:
        payload: Raw dict from an ESPN ``team_roster`` endpoint response.
        return_as_pandas: When True, return a :class:`pandas.DataFrame` instead.

    Returns:
        pl.DataFrame or pd.DataFrame — zero rows when payload is empty/malformed.

    Example:
        Quick start::

            from sportsdataverse.soccer import espn_epl_team_roster
            df = espn_epl_team_roster(team_id=364, return_parsed=True)
            print(df.shape)
    """
    try:
        athletes = (payload or {}).get("athletes") if isinstance(payload, dict) else None
        if not athletes or not isinstance(athletes, list):
            return _out(pl.DataFrame(), return_as_pandas)
        rows = []
        for a in athletes:
            pos = a.get("position") or {}
            birth_place = a.get("birthPlace") or {}
            status = a.get("status") or {}
            rows.append(
                {
                    "athlete_id": a.get("id"),
                    "uid": a.get("uid"),
                    "first_name": a.get("firstName"),
                    "last_name": a.get("lastName"),
                    "display_name": a.get("displayName"),
                    "short_name": a.get("shortName"),
                    "jersey": a.get("jersey"),
                    "age": a.get("age"),
                    "date_of_birth": a.get("dateOfBirth"),
                    "height": a.get("height"),
                    "display_height": a.get("displayHeight"),
                    "weight": a.get("weight"),
                    "display_weight": a.get("displayWeight"),
                    "position": pos.get("abbreviation"),
                    "position_name": pos.get("name"),
                    "birth_city": birth_place.get("city"),
                    "birth_country": birth_place.get("country"),
                    "citizenship": a.get("citizenship"),
                    "gender": a.get("gender"),
                    "slug": a.get("slug"),
                    "status": status.get("name") if isinstance(status, dict) else status,
                }
            )
        if not rows:
            return _out(pl.DataFrame(), return_as_pandas)
        return _out(pl.DataFrame(_stringify_lists(rows)), return_as_pandas)
    except Exception:
        return _out(pl.DataFrame(), return_as_pandas)


_SOCCER_SUMMARY_BUILDERS: Dict[str, Any] = {
    "header": _build_header,
    "lineups": _build_lineups,
    "key_events": _build_key_events,
    "team_stats": _build_team_stats,
    "commentary": _build_commentary,
    "leaders": _build_leaders,
    "standings": _build_standings_summary,
    "head_to_head": _build_head_to_head,
    "last_five": _build_last_five,
    "game_info": _build_game_info,
    "shootout": _build_shootout,
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

    Sections: ``header``, ``lineups``, ``key_events``, ``team_stats``,
    ``commentary``, ``leaders``, ``standings``, ``head_to_head``,
    ``last_five``, ``game_info``, ``shootout``.

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
