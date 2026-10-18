"""Parsers for the generated ``easl`` wrappers (East Asia Super League, via an UNOFFICIAL third-party sync server).

The data comes from ``easl.joshricha.com/api/v1``, an individual's Express server that mirrors Sportradar DataCore
(organisation ``b1e0z``); it is not EASL's or Sportradar's, can change or vanish, and its field names (``steak``,
``facebook_username``) are the proxy author's. A first-party DataCore feed needs credentials.

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns, and a zero-row frame (never an exception) for an empty or malformed body. The zero-row frame
carries the parser's documented returns schema (:data:`SPECS`, the single source of the schemas, the column order and
the descriptions' wire keys), so two empty frames, or an empty one and a populated one, stack.

**Input.** ``easl_runtime._get`` stamps the request under ``_request`` (``fixture_id`` for a game route; ``season``
and ``season_id`` for a season route; ``team_id`` / ``player_id`` for those routes) and wraps a bare list as
``{"items": [...]}``. The parsers also accept the bare body, but then a frame that takes its season from the request
has it only when the body carries it (the ``seasonId`` of a fixture row does; ``season``, the END year, never does).

**Row rule.** Each route has its own shaper. Nested objects flatten to ``<parent>_<child>`` names chosen for the
frame (``home_*`` / ``away_*`` for a fixture's two competitors, read from ``isHome`` because the server lists them in
arbitrary order), the statistics objects flatten to one column per statistic (the wire key snake-cased:
``pointsThreeMade`` -> ``points_three_made``), and wire keys that are null in every observed body are not carried
(they have no type to claim). Contact keys, social handles of players and photo URLs are not carried; a player's
birth date is a public biography field and is kept.

**Season.** The sdv season is the END year; the server's ``year`` is the START year (``year`` 2025 = EASL 25-26 =
sdv 2026). ``season`` (Int64) is stamped by the runtime or derived from ``year + 1`` on the seasons list.

**Id key spaces** (all DataCore ids are UUID v1 strings; none is numeric):

* ``fixture_id`` is a DataCore **fixture** (game); ``fixture_number`` (Int64) is the readable ``YYSSNNN`` (start year,
  end year, sequence: ``2627001`` is the first game of 2026-27) and is **null on every 2023-24 fixture**.
* ``season_id`` is the DataCore season (one per edition); ``competition_id`` the single EASL competition.
* ``team_id`` is the DataCore team (entity): it joins fixtures (``home_team_id`` / ``away_team_id``), box scores
  (``entityId``), play-by-play (``teamId``), standings (``team.id``) and ``easl_teams``.
* ``player_id`` is the DataCore person: the box score says ``personId``, every other route ``playerId``; same values.
* ``*_row_id`` columns (``season_row_id``, ``team_row_id``, ``player_row_id``) are the sync server's own integer row
  ids, NOT DataCore ids. A leaders row's ``player.seasonId`` is one of them (not the season UUID) and is not carried.

**Times.** ``start_time_utc`` / ``event_time`` / ``created_at`` / ``updated_at`` stay ISO strings as sent (the
fixture times carry no zone marker; ``start_time_utc`` is UTC, ``start_time_local`` the venue's local time);
``received_time`` is Unix epoch milliseconds. Durations (``minutes``, ``time_in_lead``) are ISO 8601 with unwrapped
minutes (``PT27M53S``, a team's ``PT200M``). There is no game clock and no shot x/y anywhere on this server.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_easl_game",
    "parse_easl_health",
    "parse_easl_leaders",
    "parse_easl_player",
    "parse_easl_player_box",
    "parse_easl_player_stats",
    "parse_easl_players",
    "parse_easl_pbp",
    "parse_easl_schedule",
    "parse_easl_seasons",
    "parse_easl_standings",
    "parse_easl_team",
    "parse_easl_team_box",
    "parse_easl_team_box_by_period",
    "parse_easl_team_leaders",
    "parse_easl_team_schedule",
    "parse_easl_team_stats",
    "parse_easl_teams",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``easl_runtime.REQUEST_KEY``).
_REQUEST = "_request"

_S, _I, _F, _B, _D = pl.String, pl.Int64, pl.Float64, pl.Boolean, pl.Date
Spec = List[Tuple[str, str, Any]]  # (column, wire path or "" for a derived column, dtype)


# --- value coercion (the boundary: one dtype per column) ----------------------------------------------------------


def _s(v: Any) -> Optional[str]:
    """A string as sent; an int becomes its decimal text; a float or bool (never an id) is refused."""
    if isinstance(v, str):
        return v
    return str(v) if isinstance(v, int) and not isinstance(v, bool) else None


def _i(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == v and v == int(v) else None
    if isinstance(v, str) and re.fullmatch(r"\s*-?\d+\s*", v):
        return int(v)
    return None


def _f(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v else None
    if isinstance(v, str):
        try:
            out = float(v)
        except ValueError:
            return None
        return out if out == out else None
    return None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _d(v: Any) -> Optional[dt.date]:
    if isinstance(v, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", v):
        try:
            return dt.date.fromisoformat(v[:10])
        except ValueError:
            return None
    return None


_COERCE = {_S: _s, _I: _i, _F: _f, _B: _b, _D: _d}

_SUFFIX = {"s": _S, "i": _I, "f": _F, "b": _B}


def _stats(wire_prefix: str, block: str) -> Spec:
    """Columns of a statistics object: ``name:t`` pairs (``t`` s/i/f/b) -> (snake_case column, wire path, dtype)."""
    out: Spec = []
    for pair in block.split():
        key, kind = pair.rsplit(":", 1)
        out.append((underscore(key), f"{wire_prefix}{key}", _SUFFIX[kind]))
    return out


# --- statistic blocks: types from the FULL bodies (the refs ``captures/presence`` sidecars) ------------------------
# ``i`` Int64, ``f`` Float64 (any value seen as a non-integral number, and every *PerGame / *Percentage / *Ratio / *Rate
# key, because a sampled whole number such as 90 points per game is still a rate), ``s`` String (ISO durations). A key
# that is null in every observed body is typed from its family (the 2026-27 bodies start filling the ``pointsFour*`` and
# ``fouls*Cat*`` columns): Float64 for rates and the unobserved true-shooting pair, Int64 for counts.

_BOX_STATS = (
    "assists:i assistsDefensive:i assistsTurnoverRatio:f blocks:i blocksReceived:i doubleDouble:i dunks:i "
    "efficiency:i efficiencyCustom:i fieldGoalsAttempted:i fieldGoalsEffectivePercentage:f fieldGoalsMade:i "
    "fieldGoalsPercentage:f foulsCoachDisqualifying:i foulsCoachTechnical:i foulsDisqualifying:i "
    "foulsDisruptive:i foulsDrawn:i foulsFlagrant:i foulsOffensive:i foulsPersonal:i foulsTechnical:i "
    "foulsTechnicalCat1:i foulsTechnicalCat2:i foulsTotal:i foulsUnsportsmanlike:i freeThrowsAttempted:i "
    "freeThrowsMade:i freeThrowsPercentage:f indexOfSuccess:i maxAssists:i maxFreeThrowsMade:i maxPoints:i "
    "maxPointsFourMade:i maxPointsThreeMade:i maxPointsTwoMade:i maxRebounds:i maxReboundsDefensive:i "
    "maxReboundsOffensive:i maxSteals:i minus:i minutes:s pir:i plus:i plusMinus:i points:i pointsFastBreak:i "
    "pointsFastBreakAttempted:i pointsFastBreakMade:i pointsFourAttempted:i pointsFourMade:i "
    "pointsFourPercentage:f pointsFromTurnover:i pointsInThePaint:i pointsInThePaintAttempted:i "
    "pointsInThePaintMade:i pointsSecondChance:i pointsSecondChanceAttempted:i pointsSecondChanceMade:i "
    "pointsThreeAttempted:i pointsThreeMade:i pointsThreePercentage:f pointsTwoAttempted:i pointsTwoMade:i "
    "pointsTwoPercentage:f powerPlayPoints:i powerPlayPointsAwarded:i rebounds:i reboundsDefensive:i "
    "reboundsDefensivePercentage:f reboundsOffensive:i reboundsOffensivePercentage:f reboundsPercentage:f "
    "steals:i tripleDouble:i trueShootingAttempts:f trueShootingPercentage:f turnovers:i turnoversPercentage:f "
    "usageRate:f "
)
_TEAM_GAME_STATS = (
    "assists:i assistsAgainst:i assistsDefensive:i assistsTurnoverRatio:f biggestLead:i biggestScoringRun:i "
    "blocks:i blocksReceived:i dunks:i efficiency:i efficiencyCustom:i fieldGoalsAttempted:i "
    "fieldGoalsAttemptedAgainst:i fieldGoalsEffectivePercentage:f fieldGoalsMade:i fieldGoalsMadeAgainst:i "
    "fieldGoalsPercentage:f fieldGoalsPercentageAgainst:f foulsBenchTechnical:i foulsCoachDisqualifying:i "
    "foulsCoachTechnical:i foulsDisqualifying:i foulsDisruptive:i foulsDrawn:i foulsFlagrant:i foulsOffensive:i "
    "foulsPersonal:i foulsPersonalAgainst:i foulsTechnical:i foulsTechnicalAgainst:i foulsTechnicalCat1:i "
    "foulsTechnicalCat2:i foulsTotal:i foulsUnsportsmanlike:i freeThrowsAttempted:i freeThrowsAttemptedAgainst:i "
    "freeThrowsMade:i freeThrowsMadeAgainst:i freeThrowsPercentage:f freeThrowsPercentageAgainst:f "
    "indexOfSuccess:i leadChanges:i minutes:s pir:i points:i pointsAgainst:i pointsFastBreak:i "
    "pointsFastBreakAgainst:i pointsFastBreakAttempted:i pointsFastBreakMade:i pointsFourAttempted:i "
    "pointsFourAttemptedAgainst:i pointsFourMade:i pointsFourMadeAgainst:i pointsFromBench:i "
    "pointsFromBenchAgainst:i pointsFromTurnover:i pointsFromTurnoverAgainst:i pointsInThePaint:i "
    "pointsInThePaintAgainst:i pointsInThePaintAttempted:i pointsInThePaintMade:i pointsSecondChance:i "
    "pointsSecondChanceAgainst:i pointsSecondChanceAttempted:i pointsSecondChanceMade:i pointsThreeAttempted:i "
    "pointsThreeAttemptedAgainst:i pointsThreeMade:i pointsThreeMadeAgainst:i pointsThreePercentage:f "
    "pointsThreePercentageAgainst:f pointsTwoAttempted:i pointsTwoAttemptedAgainst:i pointsTwoMade:i "
    "pointsTwoMadeAgainst:i pointsTwoPercentage:f pointsTwoPercentageAgainst:f powerPlayPoints:i "
    "powerPlayPointsAwarded:i rebounds:i reboundsAgainst:i reboundsDefensive:i reboundsDefensiveAgainst:i "
    "reboundsOffensive:i reboundsOffensiveAgainst:i reboundsTeamDefensive:i reboundsTeamDefensiveAgainst:i "
    "reboundsTeamOffensive:i reboundsTeamOffensiveAgainst:i reboundsTeamTotal:i reboundsTeamTotalAgainst:i "
    "steals:i stealsAgainst:i timeInLead:s timeoutsUsed:i timesScoreLevel:i turnovers:i turnoversAgainst:i "
    "turnoversTeam:i turnoversTeamAgainst:i "
)
_PERIOD_STATS = (
    "assists:i assistsAgainst:i assistsDefensive:i assistsTurnoverRatio:f biggestLead:i biggestScoringRun:i "
    "blocks:i blocksReceived:i dunks:i efficiency:i efficiencyCustom:i fieldGoalsAttempted:i "
    "fieldGoalsAttemptedAgainst:i fieldGoalsEffectivePercentage:f fieldGoalsMade:i fieldGoalsMadeAgainst:i "
    "fieldGoalsPercentage:f fieldGoalsPercentageAgainst:f foulsBenchTechnical:i foulsCoachDisqualifying:i "
    "foulsCoachTechnical:i foulsDisqualifying:i foulsDisruptive:i foulsDrawn:i foulsFlagrant:i foulsOffensive:i "
    "foulsPersonal:i foulsPersonalAgainst:i foulsTechnical:i foulsTechnicalAgainst:i foulsTechnicalCat1:i "
    "foulsTechnicalCat2:i foulsTotal:i foulsUnsportsmanlike:i freeThrowsAttempted:i freeThrowsAttemptedAgainst:i "
    "freeThrowsMade:i freeThrowsMadeAgainst:i freeThrowsPercentage:f freeThrowsPercentageAgainst:f "
    "indexOfSuccess:i leadChanges:i minutes:s pir:i points:i pointsAgainst:i pointsFastBreak:i "
    "pointsFastBreakAgainst:i pointsFastBreakAttempted:i pointsFastBreakMade:i pointsFourAttempted:i "
    "pointsFourAttemptedAgainst:i pointsFourMade:i pointsFourMadeAgainst:i pointsFromBench:i "
    "pointsFromBenchAgainst:i pointsFromTurnover:i pointsFromTurnoverAgainst:i pointsInThePaint:i "
    "pointsInThePaintAgainst:i pointsInThePaintAttempted:i pointsInThePaintMade:i pointsSecondChance:i "
    "pointsSecondChanceAgainst:i pointsSecondChanceAttempted:i pointsSecondChanceMade:i pointsThreeAttempted:i "
    "pointsThreeAttemptedAgainst:i pointsThreeMade:i pointsThreeMadeAgainst:i pointsThreePercentage:f "
    "pointsThreePercentageAgainst:f pointsTwoAttempted:i pointsTwoAttemptedAgainst:i pointsTwoMade:i "
    "pointsTwoMadeAgainst:i pointsTwoPercentage:f pointsTwoPercentageAgainst:f powerPlayPoints:i "
    "powerPlayPointsAwarded:i rebounds:i reboundsAgainst:i reboundsDefensive:i reboundsDefensiveAgainst:i "
    "reboundsOffensive:i reboundsOffensiveAgainst:i reboundsTeamDefensive:i reboundsTeamDefensiveAgainst:i "
    "reboundsTeamOffensive:i reboundsTeamOffensiveAgainst:i reboundsTeamTotal:i reboundsTeamTotalAgainst:i "
    "steals:i stealsAgainst:i timeInLead:s timeoutsUsed:i timesScoreLevel:i turnovers:i turnoversAgainst:i "
    "turnoversTeam:i turnoversTeamAgainst:i "
)
_TEAM_SEASON_STATS = (
    "assists:i assistsAgainst:i assistsAgainstPerGame:f assistsDefensive:i assistsPerGame:f "
    "assistsTurnoverRatio:f biggestLead:i biggestScoringRun:i blocks:i blocksPerGame:f blocksReceived:i "
    "blocksReceivedPerGame:f draws:i dunks:i dunksPerGame:f efficiency:i efficiencyCustom:i "
    "fieldGoalsAgainstPercentage:f fieldGoalsAttempted:i fieldGoalsAttemptedAgainst:i "
    "fieldGoalsAttemptedAgainstPerGame:f fieldGoalsEffectivePercentage:f fieldGoalsMade:i fieldGoalsMadeAgainst:i "
    "fieldGoalsMadeAgainstPerGame:f fieldGoalsPercentage:f foulsBenchTechnical:i foulsCoachDisqualifying:i "
    "foulsCoachTechnical:i foulsDisqualifying:i foulsDisruptive:i foulsDrawn:i foulsDrawnPerGame:f "
    "foulsFlagrant:i foulsOffensive:i foulsPersonal:i foulsPersonalAgainst:i foulsPersonalAgainstPerGame:f "
    "foulsPersonalPerGame:f foulsTechnical:i foulsTechnicalAgainst:i foulsTechnicalAgainstPerGame:f "
    "foulsTechnicalCat1:i foulsTechnicalCat2:i foulsTotal:i foulsUnsportsmanlike:i freeThrowsAgainstPercentage:f "
    "freeThrowsAttempted:i freeThrowsAttemptedAgainst:i freeThrowsAttemptedAgainstPerGame:f "
    "freeThrowsAttemptedPerGame:f freeThrowsMade:i freeThrowsMadeAgainst:i freeThrowsMadeAgainstPerGame:f "
    "freeThrowsMadePerGame:f freeThrowsPercentage:f games:i indexOfSuccess:i losses:i minutes:s offensiveRating:i "
    "pir:i points:i pointsAgainst:i pointsAgainstPerGame:f pointsFastBreak:i pointsFastBreakAgainst:i "
    "pointsFastBreakAgainstPerGame:f pointsFastBreakAttempted:i pointsFastBreakMade:i pointsFastBreakPerGame:f "
    "pointsFourAttempted:i pointsFourAttemptedAgainst:i pointsFourAttemptedAgainstPerGame:f "
    "pointsFourAttemptedPerGame:f pointsFourMade:i pointsFourMadeAgainst:i pointsFourMadeAgainstPerGame:f "
    "pointsFourMadePerGame:f pointsFourPercentage:f pointsFourPercentageAgainst:i pointsFromBench:i "
    "pointsFromBenchAgainst:i pointsFromBenchAgainstPerGame:f pointsFromBenchPerGame:f pointsFromTurnover:i "
    "pointsFromTurnoverAgainst:i pointsFromTurnoverAgainstPerGame:f pointsInThePaint:i pointsInThePaintAgainst:i "
    "pointsInThePaintAgainstPerGame:f pointsInThePaintAttempted:i pointsInThePaintMade:i "
    "pointsInThePaintPerGame:f pointsPerGame:f pointsSecondChance:i pointsSecondChanceAgainst:i "
    "pointsSecondChanceAgainstPerGame:f pointsSecondChanceAttempted:i pointsSecondChanceMade:i "
    "pointsSecondChancePerGame:f pointsThreeAttempted:i pointsThreeAttemptedAgainst:i "
    "pointsThreeAttemptedAgainstPerGame:f pointsThreeAttemptedPerGame:f pointsThreeMade:i "
    "pointsThreeMadeAgainst:i pointsThreeMadeAgainstPerGame:f pointsThreeMadePerGame:f pointsThreePercentage:f "
    "pointsThreePercentageAgainst:f pointsTwoAttempted:i pointsTwoAttemptedAgainst:i "
    "pointsTwoAttemptedAgainstPerGame:f pointsTwoAttemptedPerGame:f pointsTwoMade:i pointsTwoMadeAgainst:i "
    "pointsTwoMadeAgainstPerGame:f pointsTwoMadePerGame:f pointsTwoPercentage:f pointsTwoPercentageAgainst:f "
    "powerPlayPoints:i powerPlayPointsAwarded:i rebounds:i reboundsAgainst:i reboundsAgainstPerGame:f "
    "reboundsDefensive:i reboundsDefensiveAgainst:i reboundsDefensiveAgainstPerGame:f reboundsDefensivePerGame:f "
    "reboundsOffensive:i reboundsOffensiveAgainst:i reboundsOffensiveAgainstPerGame:f reboundsOffensivePerGame:f "
    "reboundsPerGame:f reboundsTeamDefensive:i reboundsTeamDefensiveAgainst:i "
    "reboundsTeamDefensiveAgainstPerGame:f reboundsTeamOffensive:i reboundsTeamOffensiveAgainst:i "
    "reboundsTeamOffensiveAgainstPerGame:f reboundsTeamTotal:i reboundsTeamTotalAgainst:i "
    "reboundsTeamTotalAgainstPerGame:f steals:i stealsAgainst:i stealsAgainstPerGame:f stealsPerGame:f "
    "trueShootingAttempts:f trueShootingPercentage:f turnovers:i turnoversAgainst:i turnoversAgainstPerGame:f "
    "turnoversPerGame:f turnoversPercentage:f turnoversTeam:i turnoversTeamAgainst:i "
    "turnoversTeamAgainstPerGame:f wins:i "
)
_PLAYER_SEASON_STATS = (
    "assists:i assistsDefensive:i assistsPerGame:f assistsTurnoverRatio:f blocks:i blocksPerGame:f "
    "blocksReceived:i blocksReceivedPerGame:f doubleDouble:i draws:i dunks:i dunksPerGame:f efficiency:i "
    "efficiencyCustom:i fieldGoalsAttempted:i fieldGoalsEffectivePercentage:f fieldGoalsMade:i "
    "fieldGoalsPercentage:f foulsCoachDisqualifying:i foulsCoachTechnical:i foulsDisqualifying:i "
    "foulsDisruptive:i foulsDrawn:i foulsDrawnPerGame:f foulsFlagrant:i foulsOffensive:i foulsPersonal:i "
    "foulsPersonalPerGame:f foulsTechnical:i foulsTechnicalCat1:i foulsTechnicalCat2:i foulsTotal:i "
    "foulsUnsportsmanlike:i freeThrowsAttempted:i freeThrowsAttemptedPerGame:f freeThrowsMade:i "
    "freeThrowsMadePerGame:f freeThrowsPercentage:f games:i gamesPercentage:f gamesStarted:i indexOfSuccess:i "
    "losses:i maxAssists:i maxFreeThrowsMade:i maxPoints:i maxPointsFourMade:i maxPointsThreeMade:i "
    "maxPointsTwoMade:i maxRebounds:i maxReboundsDefensive:i maxReboundsOffensive:i maxSteals:i minus:i minutes:s "
    "minutesPerGame:s pir:i plus:i plusMinus:i points:i pointsFastBreak:i pointsFastBreakAttempted:i "
    "pointsFastBreakMade:i pointsFourAttempted:i pointsFourAttemptedPerGame:f pointsFourMade:i "
    "pointsFourMadePerGame:f pointsFourPercentage:f pointsFromTurnover:i pointsInThePaint:i "
    "pointsInThePaintAttempted:i pointsInThePaintMade:i pointsPerGame:f pointsSecondChance:i "
    "pointsSecondChanceAttempted:i pointsSecondChanceMade:i pointsThreeAttempted:i pointsThreeAttemptedPerGame:f "
    "pointsThreeMade:i pointsThreeMadePerGame:f pointsThreePercentage:f pointsTwoAttempted:i "
    "pointsTwoAttemptedPerGame:f pointsTwoMade:i pointsTwoMadePerGame:f pointsTwoPercentage:f powerPlayPoints:i "
    "powerPlayPointsAwarded:i rebounds:i reboundsDefensive:i reboundsDefensivePerGame:f reboundsOffensive:i "
    "reboundsOffensivePerGame:f reboundsPerGame:f steals:i stealsPerGame:f tripleDouble:i trueShootingAttempts:f "
    "trueShootingPercentage:f turnovers:i turnoversPerGame:f turnoversPercentage:f wins:i "
)

# --- schemas -------------------------------------------------------------------------------------------------------

_FIXTURE_SIDE: Spec = [
    ("team_id", "teamId", _S),
    ("name", "nameFullLocal", _S),
    ("name_native", "nameFullLatin", _S),
    ("code_latin", "codeLatin", _S),
    ("code_local", "codeLocal", _S),
    ("score", "score", _I),
    ("result_place", "resultPlace", _I),
    ("draw", "draw", _B),
]
_FIXTURE: Spec = (
    [
        ("season", "", _I),
        ("season_id", "seasonId", _S),
        ("fixture_id", "fixtureId", _S),
        ("fixture_number", "fixtureNumber", _I),
        ("fixture_type", "fixtureType", _S),
        ("status", "status", _S),
        ("fixture_name", "nameLocal", _S),
        ("start_time_utc", "startTimeUTC", _S),
        ("start_time_local", "startTimeLocal", _S),
        ("attendance", "attendance", _I),
        ("live_data_available", "liveDataAvailable", _B),
        ("live_video_available", "liveVideoAvailable", _B),
        ("pool_code", "poolCode", _S),
        ("pool_id", "pool.id", _S),
        ("pool_name", "pool.name", _S),
        ("pool_abb", "pool.abb", _S),
        ("stage_id", "stage.id", _S),
        ("stage_code", "stage.stageCode", _S),
        ("stage_name", "stage.nameLocal", _S),
        ("stage_order", "stage.stageOrder", _I),
        ("venue_name", "venue.nameLocal", _S),
        ("venue_name_latin", "venue.nameLatin", _S),
        ("venue_abbreviation_latin", "venue.abbreviationLatin", _S),
        ("venue_abbreviation_local", "venue.abbreviationLocal", _S),
        ("venue_alternate_name_latin", "venue.alternateNameLatin", _S),
        ("venue_country_code", "venue.countryCode", _S),
        ("venue_city", "venue.address.city", _S),
        ("venue_state", "venue.address.state", _S),
    ]
    + [(f"home_{c}", "", t) for c, _w, t in _FIXTURE_SIDE]
    + [(f"away_{c}", "", t) for c, _w, t in _FIXTURE_SIDE]
)
_TEAM: Spec = [
    ("team_row_id", "id", _I),
    ("team_id", "teamId", _S),
    ("name", "nameFullLocal", _S),
    ("name_native", "nameFullLatin", _S),
    ("code_latin", "codeLatin", _S),
    ("code_local", "codeLocal", _S),
    ("standard", "standard", _S),
    ("age_group", "ageGroup", _S),
    ("gender", "gender", _S),
    ("status", "status", _S),
    ("primary_color", "primaryColor", _S),
    ("secondary_color", "secondaryColor", _S),
    ("tertiary_color", "tertiaryColor", _S),
    ("logo", "logo", _S),
    ("website", "website", _S),
    ("facebook_username", "facebook_username", _S),
    ("twitter_username", "twitter_username", _S),
    ("instagram_username", "instagram_username", _S),
    ("wikipedia_url", "wikipedia_url", _S),
    ("organization_id", "organizationId", _S),
    ("created_at", "createdAt", _S),
    ("updated_at", "updatedAt", _S),
]
_PLAYER: Spec = [
    ("player_row_id", "id", _I),
    ("player_id", "playerId", _S),
    ("name", "nameFullLocal", _S),
    ("name_latin", "nameFullLatin", _S),
    ("name_abbreviated", "nameAbbreviated", _S),
    ("nationality", "nationality", _S),
    ("gender", "gender", _S),
    ("dob", "dob", _D),
    ("height_cm", "heightInCm", _I),
    ("height_feet", "heightInFeet", _S),
    ("weight_kg", "weightInKg", _I),
    ("weight_lbs", "weightInLbs", _I),
    ("status", "status", _S),
    ("jersey_number", "jerseyNumber", _S),
    ("position", "position", _S),
    ("organization_id", "organizationId", _S),
    ("created_at", "createdAt", _S),
    ("updated_at", "updatedAt", _S),
]
_LEADER: Spec = [
    ("season", "", _I),
    ("season_id", "seasonId", _S),
    ("category", "", _S),
    ("rank", "", _I),
    ("value", "", _F),
    ("player_id", "player.playerId", _S),
    ("player_name", "player.nameFullLocal", _S),
    ("player_name_latin", "player.nameFullLatin", _S),
    ("jersey_number", "player.jerseyNumber", _S),
    ("position", "player.position", _S),
    ("player_status", "player.status", _S),
    ("team_id", "team.teamId", _S),
    ("team_name", "team.nameFullLocal", _S),
    ("team_name_native", "team.nameFullLatin", _S),
    ("team_code_latin", "team.codeLatin", _S),
    ("team_code_local", "team.codeLocal", _S),
]

SPECS: Dict[str, Spec] = {
    "parse_easl_health": [("status", "status", _S), ("message", "message", _S), ("timestamp", "timestamp", _S)],
    "parse_easl_seasons": [
        ("season", "", _I),
        ("season_id", "seasonId", _S),
        ("season_row_id", "id", _I),
        ("year", "year", _I),
        ("name", "nameLocal", _S),
        ("name_latin", "nameLatin", _S),
        ("status", "status", _S),
        ("start_date", "startDate", _D),
        ("end_date", "endDate", _D),
        ("competition_id", "competitionId", _S),
        ("profile_id", "profileId", _S),
        ("organization_id", "organizationId", _S),
        ("age_group", "ageGroup", _S),
        ("event_type", "eventType", _S),
        ("gender", "gender", _S),
        ("representation", "representation", _S),
        ("season_type", "seasonType", _S),
        ("standard", "standard", _S),
        ("created_at", "createdAt", _S),
        ("updated_at", "updatedAt", _S),
    ],
    "parse_easl_schedule": _FIXTURE,
    "parse_easl_game": _FIXTURE,
    "parse_easl_team_schedule": _FIXTURE,
    "parse_easl_player_box": (
        [
            ("fixture_id", "fixtureId", _S),
            ("team_id", "entityId", _S),
            ("team_name", "", _S),
            ("team_name_native", "", _S),
            ("player_id", "personId", _S),
            ("player_name", "player.nameFullLocal", _S),
            ("player_name_latin", "player.nameFullLatin", _S),
            ("bib", "bib", _S),
            ("position", "position", _S),
            ("starter", "starter", _B),
            ("is_player", "isPlayer", _B),
            ("is_team_official", "isTeamOfficial", _B),
            ("played", "", _B),
            ("minutes_played", "", _F),
        ]
        + _stats("statistics.", _BOX_STATS)
    ),
    "parse_easl_pbp": [
        ("fixture_id", "", _S),
        ("event_number", "", _I),
        ("sequence", "sequence", _I),
        ("period_id", "periodId", _I),
        ("period_label", "", _S),
        ("event_time", "eventTime", _S),
        ("received_time", "receivedTime", _I),
        ("description", "description", _S),
        ("team_id", "teamId", _S),
        ("player_id", "player.playerId", _S),
        ("player_name", "player.nameFullLocal", _S),
        ("player_name_latin", "player.nameFullLatin", _S),
        ("jersey_number", "player.jerseyNumber", _S),
        ("team_score", "", _I),
        ("opponent_score", "", _I),
        ("points_scored", "", _I),
        ("team1_id", "", _S),
        ("team1_score", "", _I),
        ("team2_id", "", _S),
        ("team2_score", "", _I),
    ],
    "parse_easl_team_box": (
        [
            ("fixture_id", "fixtureId", _S),
            ("team_id", "entityId", _S),
            ("team_name", "team.nameFullLocal", _S),
            ("team_name_native", "team.nameFullLatin", _S),
        ]
        + _stats("statistics.", _TEAM_GAME_STATS)
    ),
    "parse_easl_team_box_by_period": (
        [
            ("fixture_id", "", _S),
            ("team_id", "", _S),
            ("team_name", "", _S),
            ("team_name_native", "", _S),
            ("period", "", _I),
            ("period_label", "", _S),
        ]
        + _stats("stats.", _PERIOD_STATS)
    ),
    "parse_easl_standings": [
        ("season", "", _I),
        ("season_id", "", _S),
        ("standing_id", "id", _S),
        ("position", "position", _I),
        ("team_id", "team.id", _S),
        ("team_name", "team.name", _S),
        ("team_code", "team.code", _S),
        ("pool_id", "pool.id", _S),
        ("pool_name", "pool.name", _S),
        ("pool_abb", "pool.abb", _S),
        ("live", "live", _B),
        ("latest", "latest", _B),
    ]
    + _stats(
        "stats.",
        "gamePlayed:i gamePlayedHome:i gamePlayedAway:i wins:i winsHome:i winsAway:i losses:i lossesHome:i lossesAway:i "
        "draws:i drawsHome:i drawsAway:i winPercentage:f winPercentageHome:f winPercentageAway:f pointsFor:i "
        "pointsAgainst:i pointDifference:i pointDifferenceHome:i pointDifferenceAway:i steak:i streakHome:i "
        "streakAway:i",
    )
    + [("last_five", "", _S)],
    "parse_easl_leaders": _LEADER,
    "parse_easl_team_leaders": _LEADER,
    "parse_easl_teams": _TEAM,
    "parse_easl_team": _TEAM,
    "parse_easl_team_stats": [("team_id", "", _S), ("season", "", _I), ("season_id", "", _S)]
    + _stats("", _TEAM_SEASON_STATS),
    "parse_easl_players": [("season", "", _I), ("season_id", "", _S)] + _PLAYER,
    "parse_easl_player": _PLAYER,
    "parse_easl_player_stats": [("player_id", "", _S)] + _stats("", _PLAYER_SEASON_STATS),
}


# --- helpers -------------------------------------------------------------------------------------------------------


def _obj(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Mapping[str, Any]]:
    return [v for v in value if isinstance(v, dict)] if isinstance(value, list) else []


def _request(raw: Any) -> Mapping[str, Any]:
    return _obj(_obj(raw).get(_REQUEST))


def _items(raw: Any) -> List[Mapping[str, Any]]:
    """Rows of a list route: a bare list, or ``items`` of the wrapped body."""
    return _list(raw) if isinstance(raw, list) else _list(_obj(raw).get("items"))


def _path(obj: Any, path: str) -> Any:
    for part in path.split(".") if path else ():
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def _row(name: str, obj: Mapping[str, Any], **derived: Any) -> Dict[str, Any]:
    """One row of parser ``name``: every column of its spec, coerced to the column's dtype."""
    out: Dict[str, Any] = {}
    for col, wire, dtype in SPECS[name]:
        value = derived[col] if col in derived else (_path(obj, wire) if wire else None)
        out[col] = _COERCE[dtype](value)
    return out


def _frame(name: str, rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    """The frame of parser ``name`` over ``rows`` (zero rows keep the documented schema)."""
    schema = {col: dtype for col, _wire, dtype in SPECS[name]}
    rows = list(rows)
    return pl.DataFrame({c: [r.get(c) for r in rows] for c in schema}, schema=schema)


def _finish(df: pl.DataFrame, return_as_pandas: bool) -> Frame:
    return df.to_pandas() if return_as_pandas else df


_DURATION = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?")


def _minutes(value: Any) -> Optional[float]:
    """Minutes of an ISO 8601 duration with unwrapped minutes (``PT27M53S`` -> 27.8833); ``None`` if absent."""
    m = _DURATION.fullmatch(value) if isinstance(value, str) else None
    if not m or not any(m.groups()):
        return None
    h, mi, s = (float(g) if g else 0.0 for g in m.groups())
    return round(h * 60 + mi + s / 60, 4)


def _period_label(period: Optional[int]) -> Optional[str]:
    """``Q1``-``Q4`` for periods 1-4; ``OT1``, ``OT2`` ... for overtimes.

    The server numbers the first overtime 11 in team totals (and the second 12); no sampled body has a period 5-10, so
    those are read as ``OT{n-4}`` (unverified).
    """
    if period is None or period < 1:
        return None
    if period <= 4:
        return f"Q{period}"
    return f"OT{period - 10}" if period >= 11 else f"OT{period - 4}"


def _fixture_ctx(raw: Any, r: Mapping[str, Any]) -> Dict[str, Any]:
    """Derived columns of a fixture row: the request's season and the two competitors by ``isHome``."""
    req = _request(raw)
    comps = _list(r.get("competitors"))
    sides = {
        "home": next((c for c in comps if c.get("isHome") is True), {}),
        "away": next((c for c in comps if c.get("isHome") is False), {}),
    }
    out: Dict[str, Any] = {"season": req.get("season")}
    if not isinstance(r.get("seasonId"), str) and isinstance(req.get("season_id"), str):
        out["season_id"] = req["season_id"]
    for side, comp in sides.items():
        for col, wire, _t in _FIXTURE_SIDE:
            out[f"{side}_{col}"] = _path(comp, wire)
    return out


def _shape_fixtures(name: str, raw: Any) -> List[Dict[str, Any]]:
    rows = [raw] if isinstance(raw, dict) and "fixtureId" in raw else _items(raw)
    return [_row(name, r, **_fixture_ctx(raw, r)) for r in rows]


def _shape_box(raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    teams = {t.get("teamId"): t for t in _list(body.get("teams"))}
    fid = _request(raw).get("fixture_id")
    out = []
    for r in _list(body.get("boxScore")):
        team = _obj(teams.get(r.get("entityId")))
        stats = _obj(r.get("statistics"))
        minutes = _minutes(stats.get("minutes"))
        out.append(
            _row(
                "parse_easl_player_box",
                r,
                fixture_id=r.get("fixtureId") if isinstance(r.get("fixtureId"), str) else fid,
                player_id=r.get("personId") if r.get("personId") is not None else _path(r, "player.playerId"),
                team_name=team.get("nameFullLocal"),
                team_name_native=team.get("nameFullLatin"),
                played=minutes is not None,
                minutes_played=minutes,
            )
        )
    return out


_SHOT = re.compile(r"^(2pt|3pt)\b", re.IGNORECASE)


def _shape_pbp(raw: Any) -> List[Dict[str, Any]]:
    """One row per event in DELIVERED order (the only usable order). ``points_scored`` is the change of the event
    team's running score since the previous event, because the description always says ``missed``."""
    body = _obj(raw)
    ids = [t.get("teamId") for t in _list(body.get("teams"))]
    t1, t2 = (ids + [None, None])[:2]
    fid = _request(raw).get("fixture_id")
    last: Dict[Any, int] = {}
    out = []
    for n, e in enumerate(_list(body.get("playbyplay")), start=1):
        scores = {k: _i(v) for k, v in _obj(e.get("scores")).items()}
        tid = e.get("teamId") if isinstance(e.get("teamId"), str) else None
        mine = scores.get(tid) if tid else None
        others = [v for k, v in scores.items() if k != tid]
        pts = (mine - last.get(tid, 0)) if mine is not None else None
        period = _i(e.get("periodId"))
        out.append(
            _row(
                "parse_easl_pbp",
                e,
                fixture_id=fid,
                event_number=n,
                period_label=_period_label(period),
                team_score=mine,
                opponent_score=others[0] if tid and len(others) == 1 else None,
                points_scored=pts,
                team1_id=t1,
                team1_score=scores.get(t1) if t1 else None,
                team2_id=t2,
                team2_score=scores.get(t2) if t2 else None,
            )
        )
        last.update({k: v for k, v in scores.items() if v is not None})
    return out


def _shape_team_box(raw: Any) -> List[Dict[str, Any]]:
    return [_row("parse_easl_team_box", r) for r in _items(raw)]


def _shape_team_box_by_period(raw: Any) -> List[Dict[str, Any]]:
    fid = _request(raw).get("fixture_id")
    out = []
    for r in _items(raw):
        team = _obj(r.get("team"))
        for n in sorted(int(k[7:]) for k in r if re.fullmatch(r"period_\d+", k)):
            out.append(
                _row(
                    "parse_easl_team_box_by_period",
                    {"stats": _obj(r.get(f"period_{n}"))},
                    fixture_id=fid,
                    team_id=team.get("teamId"),
                    team_name=team.get("nameFullLocal"),
                    team_name_native=team.get("nameFullLatin"),
                    period=n,
                    period_label=_period_label(n),
                )
            )
    return out


def _shape_standings(raw: Any) -> List[Dict[str, Any]]:
    req = _request(raw)
    out = []
    for r in _items(raw):
        last = ",".join(m["result"] for m in _list(r.get("lastFiveMatches")) if isinstance(m.get("result"), str))
        out.append(
            _row(
                "parse_easl_standings",
                r,
                season=req.get("season"),
                season_id=req.get("season_id"),
                last_five=last or None,
            )
        )
    return out


def _shape_leaders(name: str, raw: Any) -> List[Dict[str, Any]]:
    body = _obj(raw)
    req = _request(raw)
    out = []
    for category, rows in body.items():
        if category == _REQUEST or not isinstance(rows, list):
            continue
        for rank, r in enumerate(_list(rows), start=1):
            out.append(_row(name, r, season=req.get("season"), category=category, rank=rank, value=r.get(category)))
    return out


def _shape_players(raw: Any) -> List[Dict[str, Any]]:
    req = _request(raw)
    return [
        _row("parse_easl_players", r, season=req.get("season"), season_id=req.get("season_id")) for r in _items(raw)
    ]


def _shape_one(name: str, raw: Any, **derived: Any) -> List[Dict[str, Any]]:
    """A single-object route (team, player, stats): the body itself minus the request stamp.

    A body none of whose wire keys is present (an empty object, an envelope) is no row.
    """
    body = {k: v for k, v in _obj(raw).items() if k != _REQUEST}
    if not any(_path(body, wire) is not None for _c, wire, _t in SPECS[name] if wire):
        return []
    return [_row(name, body, **derived)]


# --- parsers -------------------------------------------------------------------------------------------------------


def parse_easl_health(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/health`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the server `status` ('OK'), its `message` and its own `timestamp` (UTC ISO 8601 with milliseconds). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_health, parse_easl_health
            raw = easl_health(return_parsed=False)
            parse_easl_health(raw)

        Pandas::

            parse_easl_health(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_health", _shape_one("parse_easl_health", raw)), return_as_pandas)


def parse_easl_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/metadata/seasons`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per configured EASL season: `season` (END year, `year` + 1), the DataCore `season_id`, label, START `year`, status (COMPLETE or ACTIVE), dates and the competition and organisation ids. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_seasons, parse_easl_seasons
            raw = easl_seasons(return_parsed=False)
            parse_easl_seasons(raw)

        Pandas::

            parse_easl_seasons(raw, return_as_pandas=True)
    """
    return _finish(
        _frame(
            "parse_easl_seasons",
            [
                _row(
                    "parse_easl_seasons",
                    r,
                    season=(_i(r.get("year")) or 0) + 1 if _i(r.get("year")) is not None else None,
                )
                for r in _items(raw)
            ],
        ),
        return_as_pandas,
    )


def parse_easl_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per fixture of the season: ids, status (CONFIRMED, SCHEDULED or DRAFT), tip-off times, stage and pool, venue, and the two competitors as `home_*` / `away_*` (team ids, names, codes and `score`, an Int64; null before the game is played). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_schedule, parse_easl_schedule
            raw = easl_schedule(return_parsed=False)
            parse_easl_schedule(raw)

        Pandas::

            parse_easl_schedule(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_schedule", _shape_fixtures("parse_easl_schedule", raw)), return_as_pandas)


def parse_easl_game(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches/{fixtureId}`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the finished fixture's header, shaped exactly like `easl_schedule` (its `season` is null: the route carries no season). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_game, parse_easl_game
            raw = easl_game(return_parsed=False)
            parse_easl_game(raw)

        Pandas::

            parse_easl_game(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_game", _shape_fixtures("parse_easl_game", raw)), return_as_pandas)


def parse_easl_player_box(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches/{fixtureId}/boxscore`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per box-score line of both teams: `team_id`, `player_id`, names, `position`, `starter`, `played` (False when the player did not play; their counting statistics are null) and `minutes_played`, then one column per statistic (about 80). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_player_box, parse_easl_player_box
            raw = easl_player_box(return_parsed=False)
            parse_easl_player_box(raw)

        Pandas::

            parse_easl_player_box(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_player_box", _shape_box(raw)), return_as_pandas)


def parse_easl_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches/{fixtureId}/playbyplay`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per event in delivered order: `event_number` (1-based), `sequence`, `period_id` / `period_label`, `event_time`, `received_time`, `description`, the acting team and player, the event team's running `team_score` and `opponent_score`, `points_scored` (the change of the team's running score since the previous event; the text always says `missed`) and the running score of both teams as `team1_*` / `team2_*` (the body's team order). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_pbp, parse_easl_pbp
            raw = easl_pbp(return_parsed=False)
            parse_easl_pbp(raw)

        Pandas::

            parse_easl_pbp(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_pbp", _shape_pbp(raw)), return_as_pandas)


def parse_easl_team_box(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches/{fixtureId}/teamstats`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team: ids, names and one column per team statistic of the game (about 100, including the `*_against` family, which 2023-24 bodies lack except `points_against`). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team_box, parse_easl_team_box
            raw = easl_team_box(return_parsed=False)
            parse_easl_team_box(raw)

        Pandas::

            parse_easl_team_box(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_team_box", _shape_team_box(raw)), return_as_pandas)


def parse_easl_team_box_by_period(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/matches/{fixtureId}/teamstats/by/period`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team and period: `period` (1-4, overtimes after), `period_label` and the same statistics as `easl_team_box`. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team_box_by_period, parse_easl_team_box_by_period
            raw = easl_team_box_by_period(return_parsed=False)
            parse_easl_team_box_by_period(raw)

        Pandas::

            parse_easl_team_box_by_period(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_team_box_by_period", _shape_team_box_by_period(raw)), return_as_pandas)


def parse_easl_standings(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/standings`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per team and pool: `position`, ids, record, home and away splits, points for and against, `steak` (the proxy's spelling of streak) and the results of the last five games as `last_five` ('W,L,W,...', most recent first). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_standings, parse_easl_standings
            raw = easl_standings(return_parsed=False)
            parse_easl_standings(raw)

        Pandas::

            parse_easl_standings(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_standings", _shape_standings(raw)), return_as_pandas)


def parse_easl_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/leaders`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        Long frame of the tournament top-5 per category: `category` (e.g. pointsPerGame), `rank` (1-5 in delivered order), `value` (Float64) and the player and team. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_leaders, parse_easl_leaders
            raw = easl_leaders(return_parsed=False)
            parse_easl_leaders(raw)

        Pandas::

            parse_easl_leaders(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_leaders", _shape_leaders("parse_easl_leaders", raw)), return_as_pandas)


def parse_easl_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per ACTIVE team (the historical teams resolve only through `easl_team`): ids, names, codes, colours, logo, links and the proxy's timestamps. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_teams, parse_easl_teams
            raw = easl_teams(return_parsed=False)
            parse_easl_teams(raw)

        Pandas::

            parse_easl_teams(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_teams", [_row("parse_easl_teams", r) for r in _items(raw)]), return_as_pandas)


def parse_easl_team(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams/{teamId}`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the team's directory entry (shaped like `easl_teams`; historical teams resolve here). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team, parse_easl_team
            raw = easl_team(return_parsed=False)
            parse_easl_team(raw)

        Pandas::

            parse_easl_team(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_team", _shape_one("parse_easl_team", raw)), return_as_pandas)


def parse_easl_team_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams/{teamId}/matches`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per fixture of the team in the season, shaped like `easl_schedule` (its rows carry the season in `stage_id` rather than `season_id`). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team_schedule, parse_easl_team_schedule
            raw = easl_team_schedule(return_parsed=False)
            parse_easl_team_schedule(raw)

        Pandas::

            parse_easl_team_schedule(raw, return_as_pandas=True)
    """
    return _finish(
        _frame("parse_easl_team_schedule", _shape_fixtures("parse_easl_team_schedule", raw)), return_as_pandas
    )


def parse_easl_team_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams/{teamId}/stats`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the team's cumulative season statistics (about 130 columns, including per-game rates and the `*_against` family). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team_stats, parse_easl_team_stats
            raw = easl_team_stats(return_parsed=False)
            parse_easl_team_stats(raw)

        Pandas::

            parse_easl_team_stats(raw, return_as_pandas=True)
    """
    return _finish(
        _frame(
            "parse_easl_team_stats",
            _shape_one(
                "parse_easl_team_stats",
                raw,
                team_id=_request(raw).get("team_id"),
                season=_request(raw).get("season"),
                season_id=_request(raw).get("season_id"),
            ),
        ),
        return_as_pandas,
    )


def parse_easl_team_leaders(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/teams/{teamId}/leaders`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        Long frame of the team's top-5 per category, shaped like `easl_leaders`. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_team_leaders, parse_easl_team_leaders
            raw = easl_team_leaders(return_parsed=False)
            parse_easl_team_leaders(raw)

        Pandas::

            parse_easl_team_leaders(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_team_leaders", _shape_leaders("parse_easl_team_leaders", raw)), return_as_pandas)


def parse_easl_players(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/players`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row per player: ids, names, nationality, birth date, height, weight and status; with a season and team filter (a roster) also `jersey_number` and `position`. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_players, parse_easl_players
            raw = easl_players(return_parsed=False)
            parse_easl_players(raw)

        Pandas::

            parse_easl_players(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_players", _shape_players(raw)), return_as_pandas)


def parse_easl_player(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/players/{playerId}`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the player's biography, shaped like `easl_players`. Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_player, parse_easl_player
            raw = easl_player(return_parsed=False)
            parse_easl_player(raw)

        Pandas::

            parse_easl_player(raw, return_as_pandas=True)
    """
    return _finish(_frame("parse_easl_player", _shape_one("parse_easl_player", raw)), return_as_pandas)


def parse_easl_player_stats(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """``/players/{playerId}/stats`` body to a tidy frame.

    Args:
        raw: the body ``easl_runtime._get`` returns (or the bare API body).
        return_as_pandas: return a pandas frame instead of polars.

    Returns:
        One row: the player's cumulative statistics over ALL seasons (the server ignores `seasonId`; about 100 columns). Zero rows, typed from the returns schema, for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.easl import easl_player_stats, parse_easl_player_stats
            raw = easl_player_stats(return_parsed=False)
            parse_easl_player_stats(raw)

        Pandas::

            parse_easl_player_stats(raw, return_as_pandas=True)
    """
    return _finish(
        _frame(
            "parse_easl_player_stats",
            _shape_one("parse_easl_player_stats", raw, player_id=_request(raw).get("player_id")),
        ),
        return_as_pandas,
    )
