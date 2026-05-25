"""sportsdataverse._common_espn — cross-league ESPN endpoint helpers.

**Documentation**:

* Architecture (factory + return_parsed shim): https://py.sportsdataverse.org/docs/architecture/espn-cross-league
* Reusable patterns (_bind, make_league_module, ...): https://py.sportsdataverse.org/docs/architecture/building-blocks
* Parsers + ENDPOINT_PARSERS registry: https://py.sportsdataverse.org/docs/parsers/

ESPN's Site v2 / Web v3 / Core v2 endpoint shapes are **identical across every
sport** with only the ``sport``/``league`` slug differing. This module provides
one implementation per endpoint family, parameterized on those two slugs, and
each per-league wrapper module exposes thin sport-specific functions on top
(``espn_nba_scoreboard`` → ``_site_v2_scoreboard('basketball', 'nba', ...)``).

Per-league wrapper modules:
    * :mod:`sportsdataverse.nba.nba_espn_ext` (basketball/nba)
    * :mod:`sportsdataverse.mbb.mbb_espn_ext` (basketball/mens-college-basketball)
    * :mod:`sportsdataverse.wnba.wnba_espn_ext` (basketball/wnba)
    * :mod:`sportsdataverse.wbb.wbb_espn_ext` (basketball/womens-college-basketball)
    * :mod:`sportsdataverse.cfb.cfb_espn_ext` (football/college-football)
    * :mod:`sportsdataverse.nfl.nfl_espn_ext` (football/nfl)
    * :mod:`sportsdataverse.mlb.mlb_espn_ext` (baseball/mlb)

NHL is handled separately at :mod:`sportsdataverse.nhl.nhl_api_web` because
Web v3 ``gamelog`` 404s for NHL and the canonical NHL source is the modern
``api-web.nhle.com`` host.

R-package parity: this module is the convergence point for porting the
``espn_*`` families from hoopR (NBA, MBB), wehoop (WNBA, WBB), and cfbfastR
(college football). See ``sdv-internal-refs/_notes/espn_port_roadmap.md``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from sportsdataverse.dl_utils import download

# ---------------------------------------------------------------------------
# Hosts
# ---------------------------------------------------------------------------

_SITE_V2 = "https://site.api.espn.com/apis/site/v2/sports"
_SITE_V2_ALT = "https://site.api.espn.com/apis/v2/sports"
_WEB_V3 = "https://site.web.api.espn.com/apis/common/v3/sports"
_CORE_V2 = "https://sports.core.api.espn.com/v2/sports"
_CORE_V3 = "https://sports.core.api.espn.com/v3/sports"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get(url: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """GET ``url`` as JSON. Returns ``{}`` on failure. Strips ``None`` params."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _csv(values: Any) -> Optional[str]:
    """Join an iterable into a comma-separated string; pass scalar / None through."""
    if values is None:
        return None
    if isinstance(values, (list, tuple, set)):
        return ",".join(str(v) for v in values)
    return str(values)


# ===========================================================================
#                                  SITE V2
# ===========================================================================
#
# Base URL pattern: https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/...
# Returns inline JSON, user-friendly shape.

# --- league-wide ---


def _site_v2_scoreboard(
    sport: str,
    league: str,
    dates: Optional[Union[int, str]] = None,
    week: Optional[int] = None,
    season_type: Optional[int] = None,
    groups: Optional[Union[int, str]] = None,
    limit: int = 500,
    **kwargs,
) -> Dict:
    """GET /scoreboard. ``dates``: YYYYMMDD or YYYYMMDD-YYYYMMDD or season year."""
    return _get(
        f"{_SITE_V2}/{sport}/{league}/scoreboard",
        params={
            "dates": dates,
            "week": week,
            "seasontype": season_type,
            "groups": groups,
            "limit": limit,
        },
        **kwargs,
    )


def _site_v2_summary(sport: str, league: str, event_id: Union[int, str], **kwargs) -> Dict:
    """GET /summary?event={id} — comprehensive game summary (boxscore + plays + leaders)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/summary", params={"event": event_id}, **kwargs)


def _site_v2_calendar(sport: str, league: str, **kwargs) -> Dict:
    """GET /calendar — full season calendar."""
    return _get(f"{_SITE_V2}/{sport}/{league}/calendar", **kwargs)


def _site_v2_calendar_offseason(sport: str, league: str, **kwargs) -> Dict:
    """GET /calendar/offseason."""
    return _get(f"{_SITE_V2}/{sport}/{league}/calendar/offseason", **kwargs)


def _site_v2_calendar_regular_season(sport: str, league: str, **kwargs) -> Dict:
    """GET /calendar/regular-season — week-by-week regular season ranges."""
    return _get(f"{_SITE_V2}/{sport}/{league}/calendar/regular-season", **kwargs)


def _site_v2_calendar_postseason(sport: str, league: str, **kwargs) -> Dict:
    """GET /calendar/postseason."""
    return _get(f"{_SITE_V2}/{sport}/{league}/calendar/postseason", **kwargs)


def _site_v2_calendar_ondays(sport: str, league: str, **kwargs) -> Dict:
    """GET /calendar/ondays — dates with games."""
    return _get(f"{_SITE_V2}/{sport}/{league}/calendar/ondays", **kwargs)


def _site_v2_news(sport: str, league: str, limit: int = 50, **kwargs) -> Dict:
    """GET /news — league-wide news."""
    return _get(f"{_SITE_V2}/{sport}/{league}/news", params={"limit": limit}, **kwargs)


def _site_v2_injuries(sport: str, league: str, **kwargs) -> Dict:
    """GET /injuries — league-wide injury report."""
    return _get(f"{_SITE_V2}/{sport}/{league}/injuries", **kwargs)


def _site_v2_transactions(sport: str, league: str, **kwargs) -> Dict:
    """GET /transactions — league-wide transactions."""
    return _get(f"{_SITE_V2}/{sport}/{league}/transactions", **kwargs)


def _site_v2_groups(sport: str, league: str, **kwargs) -> Dict:
    """GET /groups — conferences and divisions."""
    return _get(f"{_SITE_V2}/{sport}/{league}/groups", **kwargs)


def _site_v2_rankings(sport: str, league: str, **kwargs) -> Dict:
    """GET /rankings — poll rankings (NCAA leagues only)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/rankings", **kwargs)


def _site_v2_statistics(sport: str, league: str, **kwargs) -> Dict:
    """GET /statistics — league statistical leaders (site-v2 variant)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/statistics", **kwargs)


def _site_v2_draft(sport: str, league: str, **kwargs) -> Dict:
    """GET /draft — draft board (varies per sport)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/draft", **kwargs)


# --- teams ---


def _site_v2_teams(sport: str, league: str, limit: int = 1000, **kwargs) -> Dict:
    """GET /teams — all teams."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams", params={"limit": limit}, **kwargs)


def _site_v2_team(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id} — single team detail."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}", **kwargs)


def _site_v2_team_roster(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/roster — team roster."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/roster", **kwargs)


def _site_v2_team_schedule(
    sport: str,
    league: str,
    team_id: Union[int, str],
    season: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /teams/{id}/schedule — team schedule for a season."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/schedule", params={"season": season}, **kwargs)


def _site_v2_team_record(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/record — team win/loss record."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/record", **kwargs)


def _site_v2_team_depthcharts(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/depthcharts — depth chart by position."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/depthcharts", **kwargs)


def _site_v2_team_injuries(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/injuries — team injury report."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/injuries", **kwargs)


def _site_v2_team_transactions(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/transactions — recent team transactions."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/transactions", **kwargs)


def _site_v2_team_history(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/history — franchise historical record."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/history", **kwargs)


def _site_v2_team_news(sport: str, league: str, team_id: Union[int, str], limit: int = 50, **kwargs) -> Dict:
    """GET /teams/{id}/news — team-scoped news."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/news", params={"limit": limit}, **kwargs)


def _site_v2_team_leaders(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id}/leaders — team statistical leaders."""
    return _get(f"{_SITE_V2}/{sport}/{league}/teams/{team_id}/leaders", **kwargs)


# --- athletes (site v2 lite) ---


def _site_v2_athlete(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id} — athlete profile (site v2 lite shape)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/athletes/{athlete_id}", **kwargs)


def _site_v2_athlete_gamelog(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/gamelog — site-v2 lite gamelog (Web v3 is richer)."""
    return _get(f"{_SITE_V2}/{sport}/{league}/athletes/{athlete_id}/gamelog", **kwargs)


def _site_v2_athlete_splits(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/splits — site-v2 lite splits."""
    return _get(f"{_SITE_V2}/{sport}/{league}/athletes/{athlete_id}/splits", **kwargs)


def _site_v2_athlete_bio(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/bio — athlete bio."""
    return _get(f"{_SITE_V2}/{sport}/{league}/athletes/{athlete_id}/bio", **kwargs)


def _site_v2_athlete_news(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/news — athlete-scoped news."""
    return _get(f"{_SITE_V2}/{sport}/{league}/athletes/{athlete_id}/news", **kwargs)


# ===========================================================================
#                          SITE V2 ALT — full standings
# ===========================================================================


def _site_v2_alt_standings(
    sport: str,
    league: str,
    season: Optional[Union[int, str]] = None,
    group: Optional[Union[int, str]] = None,
    standings_type: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET /apis/v2/sports/{sport}/{league}/standings — full standings (not the stub)."""
    return _get(
        f"{_SITE_V2_ALT}/{sport}/{league}/standings",
        params={
            "season": season,
            "group": group,
            "type": standings_type,
        },
        **kwargs,
    )


# ===========================================================================
#                                 WEB V3 (athletes)
# ===========================================================================


def _espn_athlete_overview(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET {WEB_V3}/{sport}/{league}/athletes/{id}/overview — rich snapshot."""
    return _get(f"{_WEB_V3}/{sport}/{league}/athletes/{athlete_id}/overview", **kwargs)


def _espn_athlete_stats(
    sport: str,
    league: str,
    athlete_id: Union[int, str],
    season: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET {WEB_V3}/.../athletes/{id}/stats?season={y} — parallel-array stats."""
    return _get(f"{_WEB_V3}/{sport}/{league}/athletes/{athlete_id}/stats", params={"season": season}, **kwargs)


def _espn_athlete_gamelog(
    sport: str,
    league: str,
    athlete_id: Union[int, str],
    season: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET {WEB_V3}/.../athletes/{id}/gamelog?season={y}. **404 for NHL.**"""
    return _get(f"{_WEB_V3}/{sport}/{league}/athletes/{athlete_id}/gamelog", params={"season": season}, **kwargs)


def _espn_athlete_splits(
    sport: str,
    league: str,
    athlete_id: Union[int, str],
    season: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET {WEB_V3}/.../athletes/{id}/splits?season={y} — situational splits."""
    return _get(f"{_WEB_V3}/{sport}/{league}/athletes/{athlete_id}/splits", params={"season": season}, **kwargs)


def _espn_statistics_byathlete(
    sport: str,
    league: str,
    category: str,
    season: Optional[Union[int, str]] = None,
    season_type: Optional[int] = None,
    limit: int = 50,
    page: int = 1,
    sort: Optional[str] = None,
    **kwargs,
) -> Dict:
    """GET {WEB_V3}/.../statistics/byathlete — ranked leaderboard with glossary."""
    return _get(
        f"{_WEB_V3}/{sport}/{league}/statistics/byathlete",
        params={
            "category": category,
            "season": season,
            "seasontype": season_type,
            "limit": limit,
            "page": page,
            "sort": sort,
        },
        **kwargs,
    )


# ===========================================================================
#                                 CORE V2
# ===========================================================================
#
# Base URL: https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/...
# `$ref`-heavy — many endpoints return links you follow.

# --- league + seasons ---


def _core_v2_league_root(sport: str, league: str, **kwargs) -> Dict:
    """GET /leagues/{league} — league root."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}", **kwargs)


def _core_v2_season_pointer(sport: str, league: str, **kwargs) -> Dict:
    """GET /leagues/{league}/season — current-season pointer."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/season", **kwargs)


def _core_v2_seasons(sport: str, league: str, limit: int = 200, **kwargs) -> Dict:
    """GET /leagues/{league}/seasons — paginated season list."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons", params={"limit": limit}, **kwargs)


def _core_v2_season(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y} — single-season root."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}", **kwargs)


def _core_v2_season_types(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/types — season-type list (1=pre, 2=reg, 3=post, 4=off/all-star)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types", **kwargs)


def _core_v2_season_type(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t} — season-type root."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}", **kwargs)


def _core_v2_season_type_groups(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/groups — conferences/divisions within season-type."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups", **kwargs)


def _core_v2_season_type_group(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    group_id: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/groups/{g} — single group within season-type."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}",
        **kwargs,
    )


def _core_v2_season_type_group_teams(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    group_id: Union[int, str],
    limit: int = 500,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/groups/{g}/teams — teams in a group."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/teams",
        params={"limit": limit},
        **kwargs,
    )


def _core_v2_season_type_group_children(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    group_id: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/groups/{g}/children — sub-groups (divisions inside conf)."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/groups/{group_id}/children",
        **kwargs,
    )


def _core_v2_season_type_weeks(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/weeks — weeks within a season-type (NFL/CFB)."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks",
        **kwargs,
    )


def _core_v2_season_type_week(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    week: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/weeks/{w} — single-week root."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}",
        **kwargs,
    )


def _core_v2_season_type_week_events(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    week: Union[int, str],
    limit: int = 500,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/weeks/{w}/events — week-scoped events."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/events",
        params={"limit": limit},
        **kwargs,
    )


def _core_v2_season_type_week_rankings(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    week: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/weeks/{w}/rankings — weekly polls (NCAA/CFB)."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/rankings",
        **kwargs,
    )


def _core_v2_season_type_leaders(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/leaders — per-season-type leaders."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/leaders",
        **kwargs,
    )


def _core_v2_season_type_corrections(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/corrections — stat-correction audit trail."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/corrections",
        **kwargs,
    )


def _core_v2_season_teams(sport: str, league: str, season: Union[int, str], limit: int = 500, **kwargs) -> Dict:
    """GET /seasons/{y}/teams — teams active in a season."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/teams", params={"limit": limit}, **kwargs)


def _core_v2_season_team(sport: str, league: str, season: Union[int, str], team_id: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/teams/{id} — team-in-a-season profile."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/teams/{team_id}", **kwargs)


def _core_v2_season_athletes(
    sport: str,
    league: str,
    season: Union[int, str],
    limit: int = 100,
    page: int = 1,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/athletes — athletes active in a season."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/athletes",
        params={"limit": limit, "page": page},
        **kwargs,
    )


def _core_v2_season_coaches(sport: str, league: str, season: Union[int, str], limit: int = 200, **kwargs) -> Dict:
    """GET /seasons/{y}/coaches — coaches active in a season."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/coaches", params={"limit": limit}, **kwargs)


def _core_v2_season_draft(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/draft — draft board for a year."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/draft", **kwargs)


def _core_v2_season_draft_round_picks(
    sport: str,
    league: str,
    season: Union[int, str],
    round_num: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/draft/rounds/{r}/picks — per-round picks."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/draft/rounds/{round_num}/picks",
        **kwargs,
    )


def _core_v2_season_futures(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/futures — futures odds."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/futures", **kwargs)


def _core_v2_season_freeagents(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/freeagents — UFA/RFA list (where applicable)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/freeagents", **kwargs)


def _core_v2_season_powerindex(
    sport: str,
    league: str,
    season: Union[int, str],
    team_id: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/powerindex[/{teamId}] — BPI/FPI/SP+. Per-team when ``team_id``."""
    suffix = f"/{team_id}" if team_id is not None else ""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/powerindex{suffix}", **kwargs)


def _core_v2_season_powerindex_leaders(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/powerindex/leaders — power-index leaderboard."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/powerindex/leaders", **kwargs)


def _core_v2_season_recruits(sport: str, league: str, season: Union[int, str], limit: int = 100, **kwargs) -> Dict:
    """GET /seasons/{y}/recruits — NCAA recruiting (CFB / MBB / WBB)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/recruits", params={"limit": limit}, **kwargs)


def _core_v2_season_qbr(
    sport: str,
    league: str,
    season: Union[int, str],
    season_type: Union[int, str] = 2,
    group_id: Optional[Union[int, str]] = None,
    split: int = 0,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/groups/{g}/qbr/{split} — Total QBR (NFL/CFB)."""
    if group_id is not None:
        path = f"/seasons/{season}/types/{season_type}/groups/{group_id}/qbr/{split}"
    else:
        path = f"/seasons/{season}/types/{season_type}/qbr/{split}"
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}{path}", **kwargs)


def _core_v2_season_qbr_week(
    sport: str,
    league: str,
    season: Union[int, str],
    week: Union[int, str],
    season_type: Union[int, str] = 2,
    split: int = 0,
    **kwargs,
) -> Dict:
    """GET /seasons/{y}/types/{t}/weeks/{w}/qbr/{split} — per-week QBR."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/types/{season_type}/weeks/{week}/qbr/{split}",
        **kwargs,
    )


# --- athletes (core v2) ---


def _core_v2_athletes_index(
    sport: str,
    league: str,
    active: bool = True,
    limit: int = 100,
    page: int = 1,
    **kwargs,
) -> Dict:
    """GET /athletes?active={bool}&limit={n}&page={p} — paginated athletes index."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/athletes",
        params={
            "active": "true" if active else "false",
            "limit": limit,
            "page": page,
        },
        **kwargs,
    )


def _core_v2_athlete(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id} — enriched athlete profile (core v2)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}", **kwargs)


def _core_v2_athlete_statistics(
    sport: str,
    league: str,
    athlete_id: Union[int, str],
    stat_type: Optional[int] = None,
    **kwargs,
) -> Dict:
    """GET /athletes/{id}/statistics[/{type}]. ``type`` ∈ {0=reg, 1=post, 2=career}."""
    if stat_type is not None:
        path = f"/athletes/{athlete_id}/statistics/{stat_type}"
    else:
        path = f"/athletes/{athlete_id}/statistics"
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}{path}", **kwargs)


def _core_v2_athlete_statisticslog(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/statisticslog — game-by-game log (NHL gamelog replacement)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/statisticslog", **kwargs)


def _core_v2_athlete_eventlog(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/eventlog — event participation log."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/eventlog", **kwargs)


def _core_v2_athlete_contracts(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/contracts — contract info."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/contracts", **kwargs)


def _core_v2_athlete_awards(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/awards — awards won by the athlete."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/awards", **kwargs)


def _core_v2_athlete_seasons(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/seasons — seasons played."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/seasons", **kwargs)


def _core_v2_athlete_records(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/records — career records."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/records", **kwargs)


def _core_v2_athlete_injuries(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/injuries — per-athlete injuries."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/injuries", **kwargs)


def _core_v2_athlete_notes(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/notes — analyst notes."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/notes", **kwargs)


def _core_v2_athlete_vsathlete(
    sport: str,
    league: str,
    athlete_id: Union[int, str],
    opp_id: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /athletes/{id}/vsathlete/{oid} — head-to-head."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/vsathlete/{opp_id}",
        **kwargs,
    )


# --- events (core v2) ---


def _core_v2_events(
    sport: str,
    league: str,
    dates: Optional[Union[int, str]] = None,
    limit: int = 500,
    **kwargs,
) -> Dict:
    """GET /events?dates={d} — paginated events index."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/events", params={"dates": dates, "limit": limit}, **kwargs)


def _core_v2_event(sport: str, league: str, event_id: Union[int, str], **kwargs) -> Dict:
    """GET /events/{id} — event root."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}", **kwargs)


def _core_v2_event_competition(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid} — competition (cid defaults to event_id)."""
    c = cid if cid is not None else event_id
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}", **kwargs)


def _core_v2_event_competitors(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/competitors — both teams' refs."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors",
        **kwargs,
    )


def _core_v2_event_competitor(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/competitors/{tid} — single competitor."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}",
        **kwargs,
    )


def _core_v2_event_competitor_roster(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../competitors/{tid}/roster — competitor roster for one game."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}/roster",
        **kwargs,
    )


def _core_v2_event_competitor_linescores(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../competitors/{tid}/linescores — per-period scores."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}/linescores",
        **kwargs,
    )


def _core_v2_event_competitor_statistics(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../competitors/{tid}/statistics — team game statistics."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}/statistics",
        **kwargs,
    )


def _core_v2_event_competitor_record(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../competitors/{tid}/record — competitor record at game-time."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}/record",
        **kwargs,
    )


def _core_v2_event_competitor_leaders(
    sport: str,
    league: str,
    event_id: Union[int, str],
    team_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../competitors/{tid}/leaders — per-team game leaders."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/competitors/{team_id}/leaders",
        **kwargs,
    )


def _core_v2_event_odds(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/odds — game odds."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/odds",
        **kwargs,
    )


def _core_v2_event_probabilities(
    sport: str,
    league: str,
    event_id: Union[int, str],
    limit: int = 300,
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/probabilities — per-play WP timeline."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/probabilities",
        params={"limit": limit},
        **kwargs,
    )


def _core_v2_event_plays(
    sport: str,
    league: str,
    event_id: Union[int, str],
    limit: int = 1000,
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/plays — raw plays for one game."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/plays",
        params={"limit": limit},
        **kwargs,
    )


def _core_v2_event_play(
    sport: str,
    league: str,
    event_id: Union[int, str],
    play_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/competitions/{cid}/plays/{pid} — single play detail."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/plays/{play_id}",
        **kwargs,
    )


def _core_v2_event_play_personnel(
    sport: str,
    league: str,
    event_id: Union[int, str],
    play_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../plays/{pid}/personnel — personnel on the play."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/plays/{play_id}/personnel",
        **kwargs,
    )


def _core_v2_event_drives(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../drives — drive list (football)."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/drives",
        **kwargs,
    )


def _core_v2_event_drive_plays(
    sport: str,
    league: str,
    event_id: Union[int, str],
    drive_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../drives/{did}/plays — plays in a drive."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/drives/{drive_id}/plays",
        **kwargs,
    )


def _core_v2_event_situation(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../situation — current in-game state."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/situation",
        **kwargs,
    )


def _core_v2_event_status(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../status — current event status."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/status",
        **kwargs,
    )


def _core_v2_event_officials(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../officials — referees/umpires."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/officials",
        **kwargs,
    )


def _core_v2_event_broadcasts(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../broadcasts — TV/streaming broadcasters."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/broadcasts",
        **kwargs,
    )


def _core_v2_event_predictor(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../predictor — ESPN game predictor."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/predictor",
        **kwargs,
    )


def _core_v2_event_powerindex(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../powerindex — power index for the game."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/powerindex",
        **kwargs,
    )


def _core_v2_event_propbets(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../propbets — prop bet markets."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/propbets",
        **kwargs,
    )


def _core_v2_event_leaders(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../leaders — per-game leaders."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/leaders",
        **kwargs,
    )


def _core_v2_event_scoringplays(
    sport: str,
    league: str,
    event_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../scoringplays — scoring summary."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/scoringplays",
        **kwargs,
    )


def _core_v2_event_official_detail(
    sport: str,
    league: str,
    event_id: Union[int, str],
    official_id: Union[int, str],
    cid: Optional[Union[int, str]] = None,
    **kwargs,
) -> Dict:
    """GET /events/{id}/.../officials/{oid} — single official detail."""
    c = cid if cid is not None else event_id
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/events/{event_id}/competitions/{c}/officials/{official_id}",
        **kwargs,
    )


# --- catalog (core v2) ---


def _core_v2_teams(sport: str, league: str, limit: int = 500, **kwargs) -> Dict:
    """GET /teams — paginated teams catalog."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/teams", params={"limit": limit}, **kwargs)


def _core_v2_team(sport: str, league: str, team_id: Union[int, str], **kwargs) -> Dict:
    """GET /teams/{id} — enriched team."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/teams/{team_id}", **kwargs)


def _core_v2_venues(sport: str, league: str, limit: int = 200, **kwargs) -> Dict:
    """GET /venues — stadiums/arenas."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/venues", params={"limit": limit}, **kwargs)


def _core_v2_venue(sport: str, league: str, venue_id: Union[int, str], **kwargs) -> Dict:
    """GET /venues/{id} — single venue detail."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/venues/{venue_id}", **kwargs)


def _core_v2_franchises(sport: str, league: str, limit: int = 200, **kwargs) -> Dict:
    """GET /franchises — franchise list."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/franchises", params={"limit": limit}, **kwargs)


def _core_v2_franchise(sport: str, league: str, franchise_id: Union[int, str], **kwargs) -> Dict:
    """GET /franchises/{id} — single franchise."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/franchises/{franchise_id}", **kwargs)


def _core_v2_coaches(sport: str, league: str, limit: int = 200, **kwargs) -> Dict:
    """GET /coaches — coaches index. **Often 404s — prefer /seasons/{y}/coaches.**"""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/coaches", params={"limit": limit}, **kwargs)


def _core_v2_coach(sport: str, league: str, coach_id: Union[int, str], **kwargs) -> Dict:
    """GET /coaches/{id} — single coach."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/coaches/{coach_id}", **kwargs)


def _core_v2_coach_record(
    sport: str,
    league: str,
    coach_id: Union[int, str],
    record_type: Union[int, str] = 0,
    **kwargs,
) -> Dict:
    """GET /coaches/{id}/record/{type} — coaching record."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/coaches/{coach_id}/record/{record_type}",
        **kwargs,
    )


def _core_v2_coach_season(
    sport: str,
    league: str,
    coach_id: Union[int, str],
    season: Union[int, str],
    **kwargs,
) -> Dict:
    """GET /coaches/{id}/seasons/{y} — coach's per-season record."""
    return _get(
        f"{_CORE_V2}/{sport}/leagues/{league}/coaches/{coach_id}/seasons/{season}",
        **kwargs,
    )


def _core_v2_positions(sport: str, league: str, **kwargs) -> Dict:
    """GET /positions — position definitions."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/positions", **kwargs)


def _core_v2_position(sport: str, league: str, position_id: Union[int, str], **kwargs) -> Dict:
    """GET /positions/{id} — single position."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/positions/{position_id}", **kwargs)


def _core_v2_tournaments(sport: str, league: str, **kwargs) -> Dict:
    """GET /tournaments — tournament list."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/tournaments", **kwargs)


def _core_v2_awards(sport: str, league: str, **kwargs) -> Dict:
    """GET /awards — league award catalog."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/awards", **kwargs)


def _core_v2_award(sport: str, league: str, award_id: Union[int, str], **kwargs) -> Dict:
    """GET /awards/{id} — single award detail."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/awards/{award_id}", **kwargs)


def _core_v2_season_awards(sport: str, league: str, season: Union[int, str], **kwargs) -> Dict:
    """GET /seasons/{y}/awards — awards given in a season."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/seasons/{season}/awards", **kwargs)


def _core_v2_standings(sport: str, league: str, **kwargs) -> Dict:
    """GET /standings — league standings (core v2 form)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/standings", **kwargs)


def _core_v2_leaders(sport: str, league: str, **kwargs) -> Dict:
    """GET /leaders — league-wide statistical leaders (core v2)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/leaders", **kwargs)


def _core_v2_league_notes(sport: str, league: str, **kwargs) -> Dict:
    """GET /notes — league-level editorial notes (sparse; NFL crawler discovery)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/notes", **kwargs)


def _core_v2_talentpicks(sport: str, league: str, **kwargs) -> Dict:
    """GET /talentpicks — ESPN editorial talent picks (sparse; NFL crawler discovery)."""
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/talentpicks", **kwargs)


def _core_v2_athlete_hotzones(sport: str, league: str, athlete_id: Union[int, str], **kwargs) -> Dict:
    """GET /athletes/{id}/hotzones — pitch-zone heat map (**MLB-only**).

    Returns the 9-cell (or 25-cell new-zones) strike-zone grid with hit
    metrics per zone. Two flavors: hitter hot zones (BA/SLG/HR per zone)
    and pitcher hot zones (BAA/whiff% per zone).
    """
    return _get(f"{_CORE_V2}/{sport}/leagues/{league}/athletes/{athlete_id}/hotzones",
                **kwargs)


# ===========================================================================
#                       PER-LEAGUE WRAPPER GENERATION
# ===========================================================================
#
# `make_league_module()` mass-generates per-league wrappers from the
# universal core function table below. Each per-league module under
# sportsdataverse/{league}/{league}_espn_ext.py is a 2-line file that
# calls make_league_module() and the wrappers materialize as proper
# attributes in the league module's namespace (with __name__ / __doc__
# set so help() and IDE introspection still work).

_UNIVERSAL_WRAPPERS = [
    # site v2 league-wide
    ("scoreboard", _site_v2_scoreboard),
    ("summary", _site_v2_summary),
    ("calendar", _site_v2_calendar),
    ("calendar_offseason", _site_v2_calendar_offseason),
    ("calendar_regular_season", _site_v2_calendar_regular_season),
    ("calendar_postseason", _site_v2_calendar_postseason),
    ("calendar_ondays", _site_v2_calendar_ondays),
    ("news", _site_v2_news),
    ("injuries", _site_v2_injuries),
    ("transactions", _site_v2_transactions),
    ("conferences", _site_v2_groups),
    ("statistics_league", _site_v2_statistics),
    ("draft", _site_v2_draft),
    # site v2 teams
    ("teams_site", _site_v2_teams),
    ("team", _site_v2_team),
    ("team_roster", _site_v2_team_roster),
    ("team_schedule", _site_v2_team_schedule),
    ("team_record", _site_v2_team_record),
    ("team_depthcharts", _site_v2_team_depthcharts),
    ("team_injuries", _site_v2_team_injuries),
    ("team_transactions", _site_v2_team_transactions),
    ("team_history", _site_v2_team_history),
    ("team_news", _site_v2_team_news),
    ("team_leaders", _site_v2_team_leaders),
    # site v2 athletes (lite)
    ("athlete_info", _site_v2_athlete),
    ("athlete_bio", _site_v2_athlete_bio),
    ("athlete_news", _site_v2_athlete_news),
    # site v2 alt
    ("standings", _site_v2_alt_standings),
    # web v3
    ("athlete_overview", _espn_athlete_overview),
    ("athlete_stats", _espn_athlete_stats),
    ("athlete_gamelog", _espn_athlete_gamelog),
    ("athlete_splits", _espn_athlete_splits),
    ("leaders", _espn_statistics_byathlete),
    # core v2 league + seasons
    ("league_root", _core_v2_league_root),
    ("season_pointer", _core_v2_season_pointer),
    ("seasons", _core_v2_seasons),
    ("season_info", _core_v2_season),
    ("season_types", _core_v2_season_types),
    ("season_type", _core_v2_season_type),
    ("season_group", _core_v2_season_type_group),
    ("season_groups", _core_v2_season_type_groups),
    ("season_group_teams", _core_v2_season_type_group_teams),
    ("season_group_children", _core_v2_season_type_group_children),
    ("season_type_leaders", _core_v2_season_type_leaders),
    ("season_type_corrections", _core_v2_season_type_corrections),
    ("season_weeks", _core_v2_season_type_weeks),
    ("season_week", _core_v2_season_type_week),
    ("season_week_events", _core_v2_season_type_week_events),
    ("season_teams", _core_v2_season_teams),
    ("season_team", _core_v2_season_team),
    ("season_athletes", _core_v2_season_athletes),
    ("season_coaches", _core_v2_season_coaches),
    ("season_draft", _core_v2_season_draft),
    ("season_draft_round_picks", _core_v2_season_draft_round_picks),
    ("season_futures", _core_v2_season_futures),
    ("season_freeagents", _core_v2_season_freeagents),
    ("season_powerindex", _core_v2_season_powerindex),
    ("season_powerindex_leaders", _core_v2_season_powerindex_leaders),
    ("season_awards", _core_v2_season_awards),
    # core v2 athletes
    ("athletes_index", _core_v2_athletes_index),
    ("athlete_core", _core_v2_athlete),
    ("athlete_career_stats", _core_v2_athlete_statistics),
    ("athlete_statisticslog", _core_v2_athlete_statisticslog),
    ("athlete_eventlog", _core_v2_athlete_eventlog),
    ("athlete_contracts", _core_v2_athlete_contracts),
    ("athlete_awards", _core_v2_athlete_awards),
    ("athlete_seasons", _core_v2_athlete_seasons),
    ("athlete_records", _core_v2_athlete_records),
    ("athlete_injuries", _core_v2_athlete_injuries),
    ("athlete_notes", _core_v2_athlete_notes),
    ("athlete_vs_athlete", _core_v2_athlete_vsathlete),
    # core v2 events
    ("events", _core_v2_events),
    ("event", _core_v2_event),
    ("event_competition", _core_v2_event_competition),
    ("event_competitors", _core_v2_event_competitors),
    ("event_competitor", _core_v2_event_competitor),
    ("event_competitor_roster", _core_v2_event_competitor_roster),
    ("event_competitor_linescores", _core_v2_event_competitor_linescores),
    ("event_competitor_statistics", _core_v2_event_competitor_statistics),
    ("event_competitor_record", _core_v2_event_competitor_record),
    ("event_competitor_leaders", _core_v2_event_competitor_leaders),
    ("event_odds", _core_v2_event_odds),
    ("event_probabilities", _core_v2_event_probabilities),
    ("event_plays", _core_v2_event_plays),
    ("event_play", _core_v2_event_play),
    ("event_play_personnel", _core_v2_event_play_personnel),
    ("event_situation", _core_v2_event_situation),
    ("event_status", _core_v2_event_status),
    ("event_officials", _core_v2_event_officials),
    ("event_broadcasts", _core_v2_event_broadcasts),
    ("event_predictor", _core_v2_event_predictor),
    ("event_powerindex", _core_v2_event_powerindex),
    ("event_propbets", _core_v2_event_propbets),
    ("event_leaders", _core_v2_event_leaders),
    ("event_scoringplays", _core_v2_event_scoringplays),
    ("event_official_detail", _core_v2_event_official_detail),
    # core v2 catalog
    ("teams_core", _core_v2_teams),
    ("team_core", _core_v2_team),
    ("venues", _core_v2_venues),
    ("venue", _core_v2_venue),
    ("franchises", _core_v2_franchises),
    ("franchise", _core_v2_franchise),
    ("coaches", _core_v2_coaches),
    ("coach", _core_v2_coach),
    ("coach_record", _core_v2_coach_record),
    ("coach_season", _core_v2_coach_season),
    ("positions", _core_v2_positions),
    ("position", _core_v2_position),
    ("tournaments", _core_v2_tournaments),
    ("awards", _core_v2_awards),
    ("award", _core_v2_award),
    ("standings_core", _core_v2_standings),
    ("leaders_core", _core_v2_leaders),
    # league-level editorial / picks (sparse — NFL crawler-discovered)
    ("league_notes", _core_v2_league_notes),
    ("talentpicks", _core_v2_talentpicks),
]

# MLB-only wrappers
_MLB_WRAPPERS = [
    ("athlete_hotzones", _core_v2_athlete_hotzones),
]

# NCAA-only wrappers (poll rankings + recruiting)
_NCAA_WRAPPERS = [
    ("rankings", _site_v2_rankings),
    ("season_recruits", _core_v2_season_recruits),
    ("season_week_rankings", _core_v2_season_type_week_rankings),
]

# Football-only wrappers (QBR — NFL + CFB)
_FOOTBALL_WRAPPERS = [
    ("season_qbr", _core_v2_season_qbr),
    ("season_qbr_week", _core_v2_season_qbr_week),
]


def _bind(core_fn, sport: str, league: str, full_name: str, parser=None):
    """Return a callable bound to ``(sport, league)`` with proper
    ``__name__`` / ``__qualname__`` / ``__doc__`` so ``help()`` and IDE
    introspection still work.

    When ``parser`` is ``None``, the returned object is a plain
    ``functools.partial`` that forwards every kwarg straight to ``core_fn``
    and returns the raw ``Dict``.

    When ``parser`` is provided, the returned object is a wrapper closure
    that adds two optional kwargs:

    * ``return_parsed`` (default ``False``) — when ``True``, dispatch the
      raw payload through the registered parser and return a polars
      DataFrame.
    * ``return_as_pandas`` (default ``False``) — forwarded to the parser
      when ``return_parsed=True``; ignored otherwise (the raw-Dict path
      cannot be coerced into pandas).
    """
    from functools import partial

    bound = partial(core_fn, sport, league)
    base_doc = (core_fn.__doc__ or "").rstrip()
    binding_note = (
        f"Bound to ``sport={sport!r}``, ``league={league!r}``. "
        f"Core implementation: :func:`sportsdataverse._common_espn.{core_fn.__name__}`."
    )

    if parser is None:
        bound.__name__ = full_name  # type: ignore[attr-defined]
        bound.__qualname__ = full_name  # type: ignore[attr-defined]
        bound.__doc__ = f"{base_doc}\n\n{binding_note}"  # type: ignore[attr-defined]
        return bound

    parser_name = getattr(parser, "__name__", "parser")

    def wrapper(*args, return_parsed: bool = False,
                return_as_pandas: bool = False, **kwargs):
        result = bound(*args, **kwargs)
        if return_parsed:
            return parser(result, return_as_pandas=return_as_pandas)
        return result

    wrapper.__name__ = full_name
    wrapper.__qualname__ = full_name
    wrapper.__doc__ = (
        f"{base_doc}\n\n{binding_note}\n\n"
        f"Pass ``return_parsed=True`` to dispatch the raw response through "
        f":func:`sportsdataverse._common_espn_parsers.{parser_name}` and "
        f"return a polars DataFrame (or pandas via ``return_as_pandas=True``)."
    )
    return wrapper


def make_league_module(
    sport: str,
    league: str,
    prefix: str,
    namespace: dict,
    include_ncaa: bool = False,
    include_football: bool = False,
    include_mlb: bool = False,
) -> List[str]:
    """Register all common ESPN wrappers in ``namespace``, named
    ``espn_{prefix}_{short_name}``. Universal wrappers always register;
    NCAA / football / MLB extras opt in via flags. Returns the list of
    full wrapper names registered (useful for __all__ population).

    Wrappers whose ``short`` name appears in
    :data:`sportsdataverse._common_espn_parsers.ENDPOINT_PARSERS` are bound
    with a ``return_parsed=True`` shim that dispatches the raw payload
    through the registered parser. All other wrappers return raw ``Dict``.
    """
    # Lazy import to keep the parser module optional at install time.
    try:
        from sportsdataverse._common_espn_parsers import parser_for
    except Exception:  # pragma: no cover — parsers module unavailable
        def parser_for(_short):  # type: ignore[no-redef]
            return None

    wrappers = list(_UNIVERSAL_WRAPPERS)
    if include_ncaa:
        wrappers.extend(_NCAA_WRAPPERS)
    if include_football:
        wrappers.extend(_FOOTBALL_WRAPPERS)
    if include_mlb:
        wrappers.extend(_MLB_WRAPPERS)
    registered = []
    for short, core in wrappers:
        full = f"espn_{prefix}_{short}"
        namespace[full] = _bind(core, sport, league, full, parser=parser_for(short))
        registered.append(full)
    return registered
