"""Parsers for the generated ``vtb_league`` wrappers (VTB United League schedule API, ``api.vtb-league.com/v2``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_cased
columns (``dl_utils.underscore``), and a zero-row frame (never an exception) for an empty or malformed body. The zero-row
frame carries the parser's documented returns schema (``vtb_dtypes.DTYPES``, generated with the schemas), so two empty
frames, or an empty one and a populated one, stack.

**Input.** ``vtb_league_runtime._get`` returns the ``{"meta": ..., "data": [...]}`` envelope with the request stamped under
``_request`` (``league_slug``, ``season``); the parsers also accept the bare envelope or the bare ``data`` list.

**Row rule.** One row per ``data`` element. Nested ``{"en": ..., "ru": ...}`` pairs become ``<field>_en`` / ``<field>_ru``;
the two competitors of a match become ``home_*`` / ``away_*`` columns; image blocks are dropped.

**Season.** The v2 API keys seasons by the **END year** (``season=2026`` is 2025-26), which is also the sdv key.

**Id key spaces.** Every id column is ``Int64``.

* ``match_id`` is the Infobasket ``GameID`` (932510 is the 2025 Supercup game on both backends), ``home_team_id`` /
  ``away_team_id`` the Infobasket ``TeamID`` and ``venue_id`` the Infobasket ``ArenaId``.
* ``league_id`` and ``phase_id`` are the v2 API's own keys (``phase_id`` is a stage of one league-season, not an Infobasket
  ``CompID``).

**Times.** ``match_time`` is the local kick-off with its offset, ``match_time_msk`` the same instant in Moscow time and
``match_time_utc`` in UTC (ISO 8601 strings as the API sends them).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence

from sportsdataverse.basketball.vtb import vtb_frames as F
from sportsdataverse.basketball.vtb.vtb_frames import Frame
from sportsdataverse.errors import _data_only

__all__ = ["parse_vtb_league_leagues", "parse_vtb_league_matches", "parse_vtb_league_seasons"]


def _data(raw: Any) -> List[Mapping[str, Any]]:
    """The ``data`` rows of the envelope (or of a bare ``{"items": [...]}`` / list)."""
    if isinstance(raw, list):
        return F.rows(raw)
    body = F.obj(raw)
    return F.rows(body.get("data")) or F.rows(body.get("items"))


def _pair(value: Any, name: str) -> Dict[str, Any]:
    """``{"en": a, "ru": b}`` -> ``{name_en: a, name_ru: b}``."""
    d = F.obj(value)
    return {f"{name}_en": d.get("en"), f"{name}_ru": d.get("ru")}


@_data_only
def parse_vtb_league_leagues(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/v2/leagues`` -> one row per league served by the API (``vtb``, ``vtbyouth``, ``wbc``).

    Args:
        raw: the runtime's envelope (``data`` list), the bare envelope or the bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per league: ``league_id`` (v2 key), ``league`` (the slug used in the other routes) and the league name and
        abbreviation in English and Russian. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_league_leagues
            parse_vtb_league_leagues({"data": [{"leagueId": 1, "league": "vtb", "leagueName": {"en": "VTB United League"}}]})
    """
    data = [
        {
            "league_id": r.get("leagueId"),
            "league": r.get("league"),
            **_pair(r.get("leagueName"), "league_name"),
            **_pair(r.get("leagueAbbrev"), "league_abbrev"),
        }
        for r in _data(raw)
    ]
    return F.finish(F.frame(data), columns, return_as_pandas, "parse_vtb_league_leagues")


@_data_only
def parse_vtb_league_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/v2/leagues/{league}/seasons`` -> one row per season the API serves for the league (new seasons only).

    Args:
        raw: the runtime's envelope (``data`` list), the bare envelope or the bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per season: ``season`` (END year, 2026 = 2025-26), ``league``, ``is_current``, ``season_name`` and
        ``season_abbrev``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_league_seasons
            parse_vtb_league_seasons({"data": [{"season": 2026, "league": "vtb", "isCurrent": False, "seasonName": "Season 2025/2026"}]})
    """
    data = [
        {
            "season": r.get("season"),
            "league": r.get("league"),
            "is_current": r.get("isCurrent"),
            "season_name": r.get("seasonName"),
            "season_abbrev": r.get("seasonAbbrev"),
        }
        for r in _data(raw)
    ]
    return F.finish(F.frame(data), columns, return_as_pandas, "parse_vtb_league_seasons")


def _side(competitor: Mapping[str, Any], side: str) -> Dict[str, Any]:
    out = {
        f"{side}_team_id": competitor.get("teamId"),
        f"{side}_score": competitor.get("scoreString"),
        **_pair(competitor.get("teamName"), f"{side}_team_name"),
        **_pair(competitor.get("teamCode"), f"{side}_team_code"),
        **_pair(competitor.get("regionName"), f"{side}_region_name"),
    }
    return out


def _match_row(m: Mapping[str, Any]) -> Dict[str, Any]:
    comps = F.rows(m.get("competitors"))
    home = next((c for c in comps if c.get("isHomeCompetitor") is True), {})
    away = next((c for c in comps if c.get("isHomeCompetitor") is False), {})
    venue = F.obj(m.get("venue"))
    custom = F.obj(m.get("customValues"))
    quarters = [q for q in (m.get("pointsByQuarters") or []) if isinstance(q, list) and len(q) == 2]
    row: Dict[str, Any] = {
        "match_id": m.get("matchId"),
        "league": m.get("league"),
        "season": m.get("season"),
        "competition": m.get("competition"),
        "phase_id": m.get("phaseId"),
        **_pair(m.get("phaseName"), "phase_name"),
        "match_number": m.get("matchNumber"),
        "match_type": m.get("matchType"),
        "match_status": m.get("matchStatus"),
        "match_time": m.get("matchTime"),
        "match_time_msk": m.get("matchTimeMSK"),
        "match_time_utc": m.get("matchTimeUTC"),
        "is_time_set": m.get("isTimeSet"),
        "time_actual": m.get("timeActual"),
        "time_end_actual": m.get("timeEndActual"),
        "duration": m.get("duration"),
        "periods": m.get("periods"),
        "extra_periods_used": m.get("extraPeriodsUsed"),
        "attendance": m.get("attendance"),
        "hosting_team_id": m.get("hostingTeamId"),
        "venue_id": m.get("venueId"),
        **_pair(venue.get("venueName"), "venue_name"),
        **_pair(venue.get("regionName"), "venue_region_name"),
        **_side(home, "home"),
        **_side(away, "away"),
        **_pair(custom.get("subtitle"), "subtitle"),
        **_pair(custom.get("necessaryLabel"), "necessary_label"),
        "points_by_quarters": json.dumps(m.get("pointsByQuarters")) if quarters else None,
    }
    for i in range(4):
        row[f"home_q{i + 1}"] = quarters[i][0] if i < len(quarters) else None
        row[f"away_q{i + 1}"] = quarters[i][1] if i < len(quarters) else None
    extra = quarters[4:]
    row["home_ot"] = sum(q[0] for q in extra if isinstance(q[0], int)) if extra else None
    row["away_ot"] = sum(q[1] for q in extra if isinstance(q[1], int)) if extra else None
    return row


@_data_only
def parse_vtb_league_matches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/v2/leagues/{league}/seasons/{season}/matches`` -> one row per match of a league-season.

    Args:
        raw: the runtime's envelope (``data`` list), the bare envelope or the bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per match: ids (``match_id``, ``phase_id``, ``hosting_team_id``, ``venue_id``), the competition, stage and
        match number, status and times, the venue, the home and away team (id, score as an integer, name, code, region in
        English and Russian), the quarter points (``home_q1`` ... ``away_q4``, overtime points summed into ``home_ot`` /
        ``away_ot``, and the raw ``points_by_quarters`` as a JSON string) and the site's subtitle and label. Cancelled
        games stay in the list with score 0. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_league_matches
            parse_vtb_league_matches({"data": [{"matchId": 932510, "league": "vtb", "season": 2026, "competitors": []}]})
    """
    return F.finish(F.frame(_match_row(m) for m in _data(raw)), columns, return_as_pandas, "parse_vtb_league_matches")
