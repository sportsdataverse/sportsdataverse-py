"""Generate the ``atrium`` flat-API endpoint YAML + returns-schemas (Sportradar Atrium / Synergy embed API).

Idempotent: same spec + same fixtures -> byte-identical output. Modeled on ``gen_liiga.py``, with the
shared plumbing in ``gen_soccer_common.py``. Reads the frozen spec from the ``sdv-internal-refs``
checkout (``$SDV_INTERNAL_REFS_REPO``, else the sibling checkout) and the committed fixtures, and emits:

* ``tools/codegen/endpoints/atrium.yaml`` -- 22 wrappers over the spec's 14 paths. One route can feed
  several frames (``fixture_detail`` feeds the game header, the periods, the box score, the team box
  score, the play-by-play and the shots), each with its own parser; :data:`ENDPOINTS` is the source of
  truth. ``website_id`` is a path parameter (one host, many league sites).
* ``tools/codegen/schemas/native/atrium/<short>.yaml`` -- one returns-schema per wrapper, the documented
  columns of its parser (``atrium_parsers.SCHEMAS``). Every ``description`` is ``''``; the run first
  parses the wrapper's fixture and fails when a documented column has another dtype there (a body
  that made the parser widen a column) or the frame lacks one. Schema key ``atrium_<short>``.
* ``tools/codegen/notes/atrium_descriptions.yaml`` -- the column descriptions, keyed ``atrium_<short>``
  -> column, for ``manual_column_descriptions.yaml`` (Integration merges it). The text lives in this
  file (:data:`COMMON`, :data:`STAT`, :data:`ROUTE`); a filler or missing description stops the run.
* ``tests/fixtures/atrium/spec_paths.json`` -- the spec's routes, for the tests (CI has no refs).

Run: ``python tools/codegen/gen_atrium.py``
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
from gen_soccer_common import ROOT, load_spec, refs_dir, rewrite_schema_dir, write_yaml

from sportsdataverse.basketball.atrium import atrium_parsers as P

HOST = "https://embed-api.eui.connect.sportradar.com/v1/embed"
SPEC = "atrium-embed.openapi.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "atrium"
FULL = FIXTURES / "full"
SCHEMA_PREFIX = "atrium_"
NOTES = ROOT / "tools" / "codegen" / "notes" / "atrium_descriptions.yaml"
SPEC_PATHS = FIXTURES / "spec_paths.json"
#: What tests/codegen/test_manual_descriptions.py rejects as filler.
_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.IGNORECASE)

SITE_DESC = (
    "Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, "
    "13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions."
)
SEASON_DESC = (
    "Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on "
    "the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). "
    "Omit season and season_id for the site's default season."
)
SEASON_ID_DESC = "Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season."
COMPETITION_DESC = (
    "Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then "
    "not sent; with neither season nor season_id the API filters by it."
)
COMPETITION_ONLY_DESC = (
    "Competition UUID (competition_id in atrium_seasons); required, e.g. 3f4064bb-51ad-11f0-aaaf-2923c944b404."
)
ENTITY_DESC = "Team UUID (entity_id in atrium_teams or any fixture row)."
LOCALE_DESC = (
    "Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change."
)
FIXTURE_ID_DESC = "Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d."
PERSON_ID_DESC = "Player UUID (person_id in atrium_players or a box score), e.g. 4458f165-f865-11f0-8ebf-539c931c1654."
ROUND_DESC = "Round (journee) number to show; the pages that take it list their rounds."

F14, F12 = "0dc631bd-c34c-11f1-8d16-4d5a21afb79d", "5423072b-6715-11f0-bd8b-9ffadebc148f"
LNB_ELITE = "3f4064bb-51ad-11f0-aaaf-2923c944b404"
LNB_TEAM, LNB_PERSON = "f43b1b0f-55bc-11f0-84d9-01499d2f0a07", "4458f165-f865-11f0-8ebf-539c931c1654"


def _q(name: str, wire: str, desc: str, *, typ: str = "str", required: bool = False, **extra: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {"name": name, "query_key": wire, "type": typ, "description": desc}
    if required:
        out["required"] = True
    out.update(extra)
    return out


_SITE = {"name": "website_id", "type": "int", "required": True, "description": SITE_DESC}
_SEASON = [
    _q("season", "season", SEASON_DESC, typ="int"),
    _q("season_id", "seasonId", SEASON_ID_DESC),
    _q("competition_id", "competitionId", COMPETITION_DESC),
]
_LOCALE = _q("locale", "locale", LOCALE_DESC)
_ENTITY = _q("entity_id", "entityId", ENTITY_DESC)
_FIXTURE = _q("fixture_id", "fixtureId", FIXTURE_ID_DESC, required=True)
_PERSON = _q("person_id", "personId", PERSON_ID_DESC, required=True)

# short -> (spec path, parser, fixture, summary, endpoint fields)
Entry = Tuple[str, Callable[..., Any], str, str, Dict[str, Any]]
ENDPOINTS: Dict[str, Entry] = {
    "fixtures": (
        "/{websiteId}/fixtures",
        P.parse_atrium_fixtures,
        "full/12__fixtures.json",
        "Fixtures (schedule and results) of a season, home and away side by side.",
        {
            "extra_params": [
                *_SEASON,
                _ENTITY,
                _q("round_number", "roundNumber", ROUND_DESC, typ="int"),
                _q(
                    "sub",
                    "sub",
                    "Tab: FIXTURES (upcoming) or RESULTS (played). Omitted = the site's default tab.",
                ),
                _LOCALE,
            ],
            "example_args": {"website_id": 14},
        },
    ),
    "seasons": (
        "/{websiteId}/fixtures",
        P.parse_atrium_seasons,
        "full/12__fixtures.json",
        "Every season (competition and year) a site lists, with the sdv END year of each.",
        {"extra_params": [_LOCALE], "example_args": {"website_id": 12}},
    ),
    "fixtures_ribbon": (
        "/{websiteId}/fixtures_ribbon",
        P.parse_atrium_fixtures_ribbon,
        "14__fixtures_ribbon.json",
        "The fixtures ribbon: recent and upcoming games across the site's competitions (empty on LNB off-season).",
        {"extra_params": [*_SEASON, _ENTITY, _LOCALE], "example_args": {"website_id": 14}},
    ),
    "fixture": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_fixture,
        "full/14__fixture_detail__statistics.json",
        "One game's header: teams, score, venue, status, rules (periods, overtime, shot clock) and captains.",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "statistics"},
            "example_args": {"website_id": 14, "fixture_id": F14},
        },
    ),
    "fixture_periods": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_fixture_periods,
        "full/14__fixture_detail__statistics.json",
        "One game's periods with the points each team scored in each, overtime periods included (ids 11+).",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "statistics"},
            "example_args": {"website_id": 14, "fixture_id": F14},
        },
    ),
    "box_score": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_box_score,
        "full/14__fixture_detail__statistics.json",
        "One game's box score: one row per rostered player with the statistics the site configures.",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "statistics"},
            "example_args": {"website_id": 14, "fixture_id": F14},
        },
    ),
    "team_box_score": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_team_box_score,
        "full/12__fixture_detail__statistics.json",
        "One game's team lines: the team totals, the team-level counts and the game aggregates (lead changes, biggest lead).",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "statistics"},
            "example_args": {"website_id": 12, "fixture_id": F12},
        },
    ),
    "pbp": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_pbp,
        "full/14__fixture_detail__pbp.json",
        "One game's play-by-play: every event with the period clock, the player and the running score.",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "pbp"},
            "example_args": {"website_id": 14, "fixture_id": F14},
        },
    ),
    "shots": (
        "/{websiteId}/fixture_detail",
        P.parse_atrium_shots,
        "full/14__fixture_detail__shot_chart.json",
        "One game's 2pt / 3pt attempts with their court location (percent of the court) and result.",
        {
            "extra_params": [_FIXTURE, _LOCALE],
            "fixed_params": {"sub": "shot_chart"},
            "example_args": {"website_id": 14, "fixture_id": F14},
        },
    ),
    "standings": (
        "/{websiteId}/standings",
        P.parse_atrium_standings,
        "full/12__standings.json",
        "Standings of a season, with the columns the site configures (LNB home / away splits, LNBP played and points).",
        {
            "extra_params": [*_SEASON, _q("round_number", "roundNumber", ROUND_DESC, typ="int"), _LOCALE],
            "example_args": {"website_id": 14},
        },
    ),
    "standings_summary": (
        "/{websiteId}/standings_fragment",
        P.parse_atrium_standings,
        "12__standings_fragment.json",
        "Compact standings of a season: position, wins, losses and games played.",
        {"extra_params": [*_SEASON, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "teams": (
        "/{websiteId}/entities",
        P.parse_atrium_teams,
        "12__entities.json",
        "The teams of a season.",
        {"extra_params": [*_SEASON, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "team": (
        "/{websiteId}/entity_detail",
        P.parse_atrium_team,
        "12__entity_detail.json",
        "A team page (summary tab): the team and its last and next game.",
        {
            "extra_params": [_q("entity_id", "entityId", ENTITY_DESC, required=True), *_SEASON, _LOCALE],
            "example_args": {"website_id": 12, "entity_id": LNB_TEAM},
        },
    ),
    "players": (
        "/{websiteId}/persons",
        P.parse_atrium_players,
        "full/12__persons.json",
        "The players of a season, with position, nationality, birth date, age and height.",
        {"extra_params": [*_SEASON, _ENTITY, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "player": (
        "/{websiteId}/person_detail",
        P.parse_atrium_player,
        "12__person_detail.json",
        "A player page: bio and the season averages the card shows.",
        {
            "extra_params": [_PERSON, *_SEASON, _LOCALE],
            "example_args": {"website_id": 12, "person_id": LNB_PERSON},
        },
    ),
    "player_seasons": (
        "/{websiteId}/person_detail",
        P.parse_atrium_player_seasons,
        "12__person_detail.json",
        "A player's per-game averages, one row per competition-season.",
        {
            "extra_params": [_PERSON, *_SEASON, _LOCALE],
            "example_args": {"website_id": 12, "person_id": LNB_PERSON},
        },
    ),
    "player_game_log": (
        "/{websiteId}/person_detail",
        P.parse_atrium_player_game_log,
        "12__person_detail__log.json",
        "A player's game log: one row per game with the opponent, result and the statistics the site shows.",
        {
            "extra_params": [_PERSON, *_SEASON, _LOCALE],
            "fixed_params": {"sub": "log"},
            "example_args": {"website_id": 12, "person_id": LNB_PERSON},
        },
    ),
    "leaders": (
        "/{websiteId}/statistics_leaders",
        P.parse_atrium_leaders,
        "full/12__statistics_leaders.json",
        "Season leaders per statistic (top 5 each), with their player and team ids.",
        {"extra_params": [*_SEASON, _ENTITY, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "player_stats": (
        "/{websiteId}/statistics_persons",
        P.parse_atrium_stat_tables,
        "full/12__statistics_persons.json",
        "Player season statistics in long form (one row per player, table and statistic); ids decoded from the widget links.",
        {"extra_params": [*_SEASON, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "team_stats": (
        "/{websiteId}/statistics_entities",
        P.parse_atrium_stat_tables,
        "full/12__statistics_entities.json",
        "Team season statistics in long form (one row per team, table and statistic).",
        {"extra_params": [*_SEASON, _LOCALE], "example_args": {"website_id": 12}},
    ),
    "competition_player_stats": (
        "/{websiteId}/statistics_competition_persons",
        P.parse_atrium_stat_tables,
        "12__statistics_competition_persons.json",
        "Player statistics of a competition in long form (names only: the bodies carry no ids).",
        {
            "extra_params": [_q("competition_id", "competitionId", COMPETITION_ONLY_DESC, required=True), _LOCALE],
            "example_args": {"website_id": 12, "competition_id": LNB_ELITE},
        },
    ),
    "competition_team_stats": (
        "/{websiteId}/statistics_competition_entities",
        P.parse_atrium_stat_tables,
        "12__statistics_competition_entities.json",
        "Team statistics of a competition in long form (names only: the bodies carry no ids).",
        {
            "extra_params": [_q("competition_id", "competitionId", COMPETITION_ONLY_DESC, required=True), _LOCALE],
            "example_args": {"website_id": 12, "competition_id": LNB_ELITE},
        },
    ),
}

# --- column descriptions ----------------------------------------------------------------------------

#: Column descriptions common to every frame that has the column.
COMMON: Dict[str, str] = {
    "website_id": "Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, "
    "326 Montenegro), read from the body's links.self.",
    "season_id": "Season UUID: one competition in one year (LNB mints a new competition_id for every edition).",
    "season_end_year": "Season END year (the sdv key): the site's year + 1 on LNB (site 12), the calendar year on the "
    "other registered sites; null when the site is unregistered, lists no seasons or the body has no season list.",
    "competition_id": "Competition UUID of the season.",
    "competition_name": "Competition name in the site's language, e.g. Betclic ELITE or LIGA CALIENTE.MX LNBP.",
    "fixture_id": "Game UUID (fixture); the key of every game frame and of the fixture_detail routes.",
    "entity_id": "Team UUID (entity): one id per club across seasons, shared by every site that shows the team.",
    "person_id": "Player UUID (person), shared across seasons and sites.",
    "person_name": "Player name as the site prints it (upper case on LNBP and NBL1).",
    "team_name": "Team name in the site's language.",
    "bib": "Jersey number the player wears, as text.",
    "name": "Name as the site prints it.",
    "image_url": "URL of the Sportradar-hosted image (400 px).",
    "logo_url": "URL of the team logo image.",
    "status": "Status code of the game, e.g. CONFIRMED (final), as sent.",
    "status_label": "Status of the game as the site words it in its own language (e.g. Termine, Final).",
    "start_time_utc": "Scheduled start, naive datetime in UTC (the API sends no offset).",
    "start_time_local": "Scheduled start, naive datetime in the venue's local time (timezone gives the zone; no offset sent).",
    "timezone": "IANA time zone of the venue (e.g. Europe/Paris, Mexico/General) that start_time_local is in.",
    "venue": "Name of the arena where the game is played.",
    "venue_id": "Venue UUID; joins no other route.",
    "attendance": "Spectators at the game; null when not reported.",
    "fixture_type": "Kind of game as sent: REGULAR, PLAYOFF, ... (the set is open).",
    "series": "Playoff series the game belongs to as sent (JSON text when an object); null outside a series.",
    "times_unconfirmed": "True while the start time is not final.",
    "side": "Side of the game the row belongs to: home or away (from the fixture's isHome flag).",
    "date_of_birth": "Player's date of birth.",
    "age": "Player's age in years when the body was generated.",
    "nationality": "Player's nationality in the site's language (e.g. Belgique).",
    "rank": "Position of the leader within the statistic, 1-based (the API lists the top 5).",
    "value": "Numeric value of the statistic; null when the API sends a duration or text (see value_text).",
    "value_text": "Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game.",
    "stat_key": "Statistic key as the API names it (pointsPerGame, fieldGoalsMade, ...), unchanged from header key.",
    "stat_name": "Short column heading the site shows for the statistic (PTS, REB), in the site's language.",
    "stat_label": "Longer statistic label the site shows (code in the API), in the site's language.",
    "table_key": "Statistics table the row is from: averages, shootingstatistics, totals, ... (the set is per site).",
    "table_label": "The table's heading in the site's language (e.g. Moyennes).",
    "entity_name": "Team name as the site prints it; for player rows of a stat table it is the player's team.",
    "season_name": "Season name in the site's language, e.g. Betclic ELITE 2025.",
    "season_year": "The site's own year of the season (START year on LNB, calendar year elsewhere).",
    "year": "The site's own year of the season: the START year on LNB (site 12), the calendar year elsewhere.",
    "abbreviation_local": "Local abbreviation of the season; null in every body seen.",
    "is_default_season": "True for the season the body is about (the site's default when none was asked).",
    "team_id": "Team UUID of the player's team (entity_id elsewhere).",
}

#: Statistic-key descriptions (noun phrases; the frame adds the context in :data:`CONTEXT`).
STAT: Dict[str, str] = {
    "minutes": "Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M)",
    "seconds_played": "Time played in whole seconds, parsed from minutes",
    "points": "Points scored",
    "rebounds": "Total rebounds",
    "rebounds_offensive": "Offensive rebounds",
    "rebounds_defensive": "Defensive rebounds",
    "assists": "Assists",
    "steals": "Steals",
    "blocks": "Shots blocked",
    "blocks_received": "Own shots blocked by an opponent (LNB only)",
    "turnovers": "Turnovers",
    "fouls_total": "Fouls committed",
    "fouls_drawn": "Fouls drawn (committed against the player or team; LNBP only)",
    "fouls_personal": "Personal fouls",
    "plus_minus": "Plus/minus, the point differential while on court (LNB only; null in some team lines)",
    "efficiency": "Efficiency rating, from the site's own formula",
    "field_goals_made": "Field goals made, two- and three-pointers together (LNBP only)",
    "field_goals_attempted": "Field goals attempted (LNBP only)",
    "field_goals_percentage": "Field-goal percentage, 0-100 (LNBP only)",
    "points_two_made": "Two-point field goals made",
    "points_two_attempted": "Two-point field goals attempted",
    "points_two_percentage": "Two-point percentage, 0-100",
    "points_three_made": "Three-point field goals made",
    "points_three_attempted": "Three-point field goals attempted",
    "points_three_percentage": "Three-point percentage, 0-100",
    "free_throws_made": "Free throws made",
    "free_throws_attempted": "Free throws attempted",
    "free_throws_percentage": "Free-throw percentage, 0-100",
    "points_fast_break": "Fast-break points (LNBP only)",
    "dunks": "Dunks",
    "minutes_per_game": "Minutes per game as an ISO 8601 duration",
    "points_per_game": "Points per game",
    "rebounds_per_game": "Rebounds per game",
    "rebounds_offensive_per_game": "Offensive rebounds per game",
    "rebounds_defensive_per_game": "Defensive rebounds per game",
    "assists_per_game": "Assists per game",
    "steals_per_game": "Steals per game",
    "blocks_per_game": "Blocks per game",
    "turnovers_per_game": "Turnovers per game",
    "fouls_personal_per_game": "Personal fouls per game",
    "dunks_per_game": "Dunks per game",
    "wins": "Games won; -1 for two teams in the 2025-26 LNB table (an administrative ranking, unverified), kept as sent",
    "losses": "Games lost",
    "played": "Games played (LNBP and the compact standings)",
    "standing_points": "League table points (LNBP)",
    "win_percentage": "Share of games won as a 0-1 fraction (not 0-100)",
    "scored_for": "Points scored over the season",
    "scored_against": "Points conceded over the season",
    "point_difference": "Point differential over the season",
    "scored_for_home": "Points scored in home games",
    "scored_against_home": "Points conceded in home games",
    "point_difference_home": "Point differential in home games",
    "scored_for_away": "Points scored in away games",
    "scored_against_away": "Points conceded in away games",
    "point_difference_away": "Point differential in away games",
    "result_string_home": "Home results as a string of result letters in the site's language, one per game "
    "(order and letters per locale; unverified)",
    "result_string_away": "Away results as a string of result letters in the site's language, one per game "
    "(order and letters per locale; unverified)",
    "streak": "Length of the current run of results (the win / loss sign convention is unverified)",
    "streak_home": "Length of the current run of home results (sign convention unverified)",
    "streak_away": "Length of the current run of away results (sign convention unverified)",
    "biggest_lead": "Biggest lead of the game, in points",
    "biggest_scoring_run": "Longest unanswered scoring run of the game, in points",
    "lead_changes": "Number of lead changes in the game",
    "points_from_bench": "Points scored by non-starters (LNBP only)",
    "points_from_turnover": "Points scored off opponent turnovers (LNBP only)",
    "time_in_lead": "Time the team led, as an ISO 8601 duration (PT36M11S)",
    "time_in_lead_seconds": "Time the team led, in whole seconds",
}
#: Frame -> the suffix that says whose statistic it is.
CONTEXT = {
    "box_score": "player, this game",
    "team_box_score": "team, this game",
    "player_game_log": "player, that game",
    "player_seasons": "player, that competition-season",
    "player": "player, that season",
    "standings": "team, this table",
}

_SIDE = {
    "entity_id": "Team UUID of the {side} team.",
    "code": "Short code of the {side} team (e.g. PAB).",
    "name": "Name of the {side} team.",
    "score": "Points scored by the {side} team (the API sends a string; Int64 here).",
    "result_status": "Result status of the {side} team's score, e.g. CONFIRMED.",
    "result_place": "Result place of the {side} team: 1 for the winner, 2 for the loser.",
    "draw": "True when the game ended level for the {side} team.",
    "shootout_attempts": "Shootout attempts of the {side} team as sent (an empty string becomes null).",
    "secondary_score": "Secondary score of the {side} team (unused in the samples; shown as text).",
    "is_leading": "True while the {side} team leads (ribbon rows).",
    "points": "Points the {side} team scored in this period (not the running score).",
    "captain_person_id": "Person UUID of the {side} team's captain; null when the body names none.",
}
_PN = {
    "fixture_id": "Game UUID of the {when} game shown on the team page.",
    "round": "Round of the {when} game as the site words it (e.g. 30).",
    "status": "Status code of the {when} game, e.g. CONFIRMED.",
    "start_time_utc": "Start of the {when} game, naive UTC datetime.",
    "start_time_local": "Start of the {when} game, naive venue-local datetime.",
    "venue": "Arena of the {when} game.",
    "attendance": "Spectators at the {when} game; null when not reported.",
    "home_entity_id": "Team UUID of the {when} game's home team.",
    "home_name": "Name of the {when} game's home team.",
    "home_score": "Home team's points in the {when} game.",
    "away_entity_id": "Team UUID of the {when} game's away team.",
    "away_name": "Name of the {when} game's away team.",
    "away_score": "Away team's points in the {when} game.",
}

#: Route-specific descriptions (per wrapper short) where the common text would be wrong or too vague.
ROUTE: Dict[str, Dict[str, str]] = {
    "seasons": {
        "season_id": "Season UUID; pass it as season_id to any season-scoped wrapper.",
        "competition_id": "Competition UUID; pass it as competition_id to pick one of several seasons of an END year.",
        "competition_name": "Competition name in the site's language; the same name can appear for several years.",
    },
    "fixtures": {
        "season_id": "Season UUID the page is about (the site's default season when none was asked).",
        "name": "Fixture name as the site prints it (e.g. Paris - Lyon-Villeurbanne); null on LNBP.",
        "round": "Round of the game as the site words it, e.g. Journee: 30.",
        "round_code": "Round code; null in every body seen.",
        "pool": "Pool the game belongs to; null when the competition has none.",
        "stage_code": "Opaque 30-hex stage code of the game; null in every body seen.",
        "home_name": "Name of the home team.",
    },
    "fixtures_ribbon": {
        "season_id": "Season UUID of the game.",
        "season_name": "Name of the season / competition the game belongs to.",
        "stage_name": "Name of the stage; null in every body seen.",
        "is_final": "True when the game is over.",
        "is_live": "True while the game is being played.",
        "start_time_local": "The ribbon's fixture date: a naive local datetime (no offset; the zone is not given).",
        "status": "Status code of the game, e.g. CONFIRMED.",
    },
    "fixture": {
        "season_id": "Season UUID of the game.",
        "competition_name": "Name of the competition, in the site's language.",
        "name": "Fixture name; null on LNBP.",
        "age_group": "Age group of the competition, e.g. SENIOR.",
        "external_id": "The league's own match number (LNB: the lnb.fr number); null elsewhere.",
        "updated": "When the record was last updated, naive datetime (no offset; zone unverified).",
        "live_data_available": "True when live data feeds are enabled for the game.",
        "live_video_available": "True when a live video stream is attached.",
        "discipline": "Discipline label of the game; null in every body seen.",
        "number_of_periods": "Regulation periods in the game (4).",
        "period_length": "Length of a regulation period, in minutes.",
        "overtime_allowed": "True when the game may go to overtime.",
        "overtime_length": "Length of an overtime period, in minutes.",
        "shot_clock": "Shot clock length, in seconds.",
        "max_fouls_personal": "Personal fouls that disqualify a player.",
        "fouls_before_bonus": "Team fouls in a period before the bonus applies.",
        "maximum_players_on_court": "Players a team has on court at once.",
        "maximum_roster_size": "Largest roster a team may enter for the game.",
    },
    "fixture_periods": {
        "season_id": "Season UUID of the game.",
        "label": "Short period label in the site's language (Q1, P1), as sent.",
        "period_id": "Period number: 1-4 regulation, 11, 12, ... overtimes (not 5).",
        "is_overtime": "True for an overtime period.",
        "is_shootout": "True for a shootout period.",
        "started": "True once the period has started.",
        "ended": "True once the period has ended.",
        "duration_minutes": "Length of the period in minutes.",
        "elapsed_minutes_before_period": "Minutes of game time played before the period starts (40 for the first overtime).",
        "home_entity_id": "Team UUID of the home team, from the fixture's isHome flag.",
        "away_entity_id": "Team UUID of the away team, from the fixture's isHome flag.",
    },
    "box_score": {
        "season_id": "Season UUID of the game.",
        "entity_id": "Team UUID the player is rostered with for this game.",
        "position": "Position code on the roster (LNB, e.g. PG); null on LNBP.",
        "starter": "True when the player started the game.",
        "active": "True when the player is on the court in a live game.",
        "participated": "True when the player entered the game; false rows have null statistics.",
        "did_not_play_reason": "Reason the player did not play; null in every body seen.",
    },
    "team_box_score": {
        "season_id": "Season UUID of the game.",
        "entity_id": "Team UUID (from the fixture's isHome flag).",
    },
    "pbp": {
        "season_id": "Season UUID of the game.",
        "sequence": "1-based position of the event in the game (periods in order, events chronological).",
        "event_id": "Event UUID, shared with the same event in atrium_shots.",
        "period_id": "Period number: 1-4 regulation, 11, 12, ... overtimes (not 5).",
        "clock": "Time REMAINING in the period as an ISO 8601 duration (PT10M0S at the start).",
        "clock_seconds": "Seconds remaining in the period, parsed from clock.",
        "entity_id": "Team UUID the event is credited to.",
        "person_name": "Player the event is credited to; null for team events (time-outs, team rebounds).",
        "bib": "Jersey number of the player, as text; null for team events.",
        "person_id": "Player UUID; null for team events (time-outs, team rebounds).",
        "event_type": "Event type: 2pt, 3pt, freeThrow, rebound, assist, steal, block, turnover, foul, substitution, "
        "timeOut, jumpBall, ... (language-independent).",
        "event_sub_type": "Event detail: jumpShot, layup, offensive, defensive, in, out, 1Of2, personal, ...; null when none.",
        "description": "The event's text in the site's language (e.g. Entre-deux remporte).",
        "success": "True when a shot or free throw was made, false when missed; null for other events.",
        "success_label": "The made / missed label in the site's language; sent on shot events only.",
        "x": "Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home "
        "team attacks the low-x basket all game; null for non-shots.",
        "y": "Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is "
        "unverified; null for non-shots.",
        "home_score": "Home team's running score after the event.",
        "away_score": "Away team's running score after the event.",
    },
    "standings": {
        "season_id": "Season UUID of the table.",
        "group": "Label of the table when the standings hold several groups; null for a single table.",
        "position": "League table position (1 = first).",
        "entity_id": "Team UUID of the row.",
        "team_name": "Team name as the site prints it.",
        "is_current_team": "True for the team the page was opened for (team context); false on a plain standings request.",
    },
    "teams": {
        "season_id": "Season UUID of the team list.",
        "name": "Team name in the site's language.",
    },
    "team": {
        "entity_id": "Team UUID, decoded from the page's tab links (the body does not echo it elsewhere).",
        "name": "Team name from the page banner.",
        "color": "Team colour from the banner, hex string with its #.",
        "logo_url": "URL of the banner's team logo.",
    },
    "players": {
        "season_id": "Season UUID of the roster list.",
        "name": "Player name as the site prints it.",
        "bib": "Jersey number, as text.",
        "entity_id": "Team UUID the player is rostered with that season.",
        "position_code": "Position code (C, PG, SF, ...), language-independent.",
        "position_name": "Position name in the site's language.",
        "height": "Height as an integer, centimetres presumed (unit unverified); null when the site gives none.",
        "status": "Player status in the site's language (Actif, Blesse).",
    },
    "player": {
        "name": "Player name as the site prints it.",
        "bib": "Jersey number on the card; null when none.",
        "team_id": "Team UUID of the player's current team on the card.",
        "team_name": "Name of the player's current team on the card.",
        "date_of_birth": "Player's date of birth, from the card's info items.",
        "age": "Player's age in years, from the card's info items.",
        "nationality": "Player's nationality in the site's language, from the card's info items.",
        "points_per_game": "Points per game over the season shown on the card.",
        "rebounds_offensive_per_game": "Offensive rebounds per game over the season shown on the card.",
        "rebounds_defensive_per_game": "Defensive rebounds per game over the season shown on the card.",
    },
    "player_seasons": {
        "season_name": "Competition-season the line is for, in the site's language (Betclic ELITE - Playoffs 2025).",
        "season_year": "The site's own year of that season (START year on LNB).",
        "season_end_year": "Season END year of that season (sdv key); null when the site is unmapped.",
        "team_id": "Team UUID the player played for in that season.",
        "team_name": "Name of the team the player played for in that season.",
    },
    "player_game_log": {
        "season_id": "Season UUID the log is for (the season asked, else the site's default).",
        "date": "Local start of the game, naive datetime (no offset; zone not given).",
        "opponent_entity_id": "Team UUID of the opponent.",
        "opponent_name": "Name of the opponent.",
        "result": "Result for the player's team: W or L.",
        "result_label": "Result label in the site's language (V, D).",
        "score": "Final score as the site's text (e.g. 88 - 91); which side is listed first is unverified, so it is "
        "not split.",
    },
    "leaders": {
        "season_id": "Season UUID of the leaders.",
        "entity_id": "Team UUID of the leader's team.",
        "entity_name": "Name of the leader's team.",
        "person_id": "Player UUID of the leader.",
        "person_name": "Name of the leader.",
        "stat_key": "Statistic the leader is ranked in (points, pointsPerGame, efficiency, doubleDouble, ...).",
    },
    "stat_tables": {
        "season_id": "Season UUID of the table; null for the competition-scoped routes (their bodies carry none).",
        "person_id": "Player UUID decoded from the widget link beside the name (the tables carry names only); null "
        "when the link is missing or undecodable, and always null in team and competition bodies.",
        "person_name": "Player name as the site prints it; null in team tables.",
        "entity_id": "Team UUID decoded from the widget link (team tables only; player tables name the team but "
        "carry no id).",
    },
}


#: The shot chart's rows are the pbp events of the 2pt / 3pt attempts, with the same columns.
ROUTE["shots"] = {
    **ROUTE["pbp"],
    "sequence": "1-based position of the shot among the shots of the game (not the pbp sequence).",
    "event_id": "Event UUID of the shot; the same event in atrium_pbp.",
    "x": "Shot location along the 28 m court length as percent of the court (0-100, origin at a corner); the home "
    "team attacks the low-x basket all game; LNBP and NBL1 clamp rim attempts to 5 / 95.",
    "y": "Shot location across the 15 m width as percent of the court (0-100); which sideline is 0 is unverified.",
    "person_id": "Player UUID of the shooter.",
    "person_name": "Name of the shooter.",
    "success": "True when the shot was made, false when missed.",
}


def _stat_desc(frame: str, col: str) -> Optional[str]:
    base, scope = col, CONTEXT.get(frame)
    if frame == "team_box_score" and col.startswith("extra_"):
        base, scope = col[6:], "team, credited to the team rather than a player"
    elif frame == "team_box_score" and col.startswith("total_"):
        base, scope = col[6:], "team, game total"
    text = STAT.get(base)
    if text is None or scope is None:
        return None
    return f"{text} ({scope})."


def _describe(frame: str, col: str) -> str:
    """:data:`ROUTE`, then a side / previous / next prefix, then a statistic, then :data:`COMMON`."""
    hit = ROUTE.get(frame, {}).get(col)
    if hit:
        return hit
    for prefix in ("home", "away"):
        if col.startswith(prefix + "_"):
            rest = col[len(prefix) + 1 :]
            if frame == "team" or rest not in _SIDE:
                break
            return _SIDE[rest].format(side=prefix)
    if frame == "team":
        for when, key in (("last played", "previous"), ("next scheduled", "next")):
            if col.startswith(key + "_") and col[len(key) + 1 :] in _PN:
                return _PN[col[len(key) + 1 :]].format(when=when)
    return _stat_desc(frame, col) or COMMON.get(col, "")


def _filler(name: str, desc: str) -> bool:
    d = desc.strip()
    if len(d) < 15 or d.lower() == name.lower() or _BANNED.match(d):
        return True
    words = "".join(re.findall(r"[a-z0-9]+", d.lower().split(":")[-1]))
    return bool(words) and name.replace("_", "").endswith(words)


_TYPE = {pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.Date: "date"}


def _type(dtype: Any) -> str:
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    return _TYPE.get(dtype, "character")


def _check_frame(short: str, entry: Entry) -> None:
    """Parse the wrapper's fixture; every documented column must be present with its documented dtype."""
    _spec_path, parser, fixture, _summary, _fields = entry
    df = parser(json.loads((FIXTURES / fixture).read_text(encoding="utf-8")))
    schema = P.SCHEMAS[_schema_name(parser)]
    bad = [f"{c}: {df.schema.get(c)} != {t}" for c, t in schema.items() if df.schema.get(c) != t]
    if bad or df.height == 0:
        raise SystemExit(f"atrium: {short} on {fixture}: {df.height} rows; dtype drift {bad[:5]}")


def _schema_name(parser: Callable[..., Any]) -> str:
    name = parser.__name__.removeprefix("parse_atrium_")
    return name


def _endpoint(short: str, entry: Entry) -> Dict[str, Any]:
    spec_path, parser, _fixture, summary, fields = entry
    path = spec_path.replace("{websiteId}", "{website_id}")
    fields = copy.deepcopy(fields)
    names = ["website_id"]
    assert not {"league", "sport"} & set(names), path  # the renderer substitutes those as ESPN slugs
    assert set(re.findall(r"\{([^}]+)\}", path)) == set(names), path
    return {
        "short": short,
        "summary": summary,
        "path": path,
        "path_params": [dict(_SITE)],
        "parser": parser.__name__,
        "returns_schema": f"native/atrium/{short}",
        **fields,
    }


def main() -> None:
    refs = refs_dir("atrium-embed", SPEC)
    spec = load_spec(refs, SPEC)
    used = {e[0] for e in ENDPOINTS.values()}
    if used ^ set(spec["paths"]):
        raise SystemExit(f"atrium: spec paths and ENDPOINTS differ: {sorted(used ^ set(spec['paths']))}")
    for short, entry in ENDPOINTS.items():
        _check_frame(short, entry)
    doc = {
        "api": "atrium",
        "host": HOST,
        "name_pattern": "atrium_{short}",
        "module": "atrium",
        "parser_module": "basketball.atrium.atrium_parsers",
        "getter_module": "sportsdataverse.basketball.atrium.atrium_runtime",
        "qualifier": "",
        "passthrough_query": False,
        "schema_prefix": SCHEMA_PREFIX,
        "runtime_imports": ["_get"],
        "docstring": {
            "example_import": True,
            "notes": [
                "Unofficial: this is the public endpoint the Sportradar Atrium / Synergy ``Connect`` widget calls from "
                "every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the "
                "leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, "
                "and never republish it as release assets. Keep crawls small and incremental.",
                "``website_id`` picks the league site (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, "
                "22 Panama, 236 Austria, 326 Montenegro); one site carries many competitions. Seasons are END years "
                "(``season=2026``): the site's own year is the START year on LNB and the calendar year elsewhere, "
                "and a year can hold several competitions, so pair ``season`` with ``competition_id`` (see "
                "``atrium_seasons``) or pass ``season_id`` directly. ``atrium_end_year(website_id, year)`` maps a site's "
                "own year to that END year.",
                "Columns are configured per site: a statistic one site omits is null. The player and team ids are "
                "shared across sites; the player stat tables carry names only, so ``person_id`` there is decoded "
                "from the widget link beside each name.",
            ],
            "raises": [
                "sportsdataverse.errors.NoDataError: the site has nothing for the request (no such season, an "
                "empty fixture list, a game with no play-by-play yet, an unknown id answered 404).",
                "AssetFetchError: also raised for an error body, a stub, or a body that belongs to another "
                "site, type, season, game, player or team than the one asked for.",
            ],
            "see_also": [
                {
                    "name": "Sportradar Connect widget",
                    "url": "https://widget.eui.connect.sportradar.com/",
                    "note": "the widget whose embed API these wrappers read",
                },
                {
                    "name": "hoopR",
                    "url": "https://hoopR.sportsdataverse.org",
                    "note": "SportsDataverse men's basketball (R)",
                },
                {
                    "name": "wehoop",
                    "url": "https://wehoop.sportsdataverse.org",
                    "note": "SportsDataverse women's basketball (R)",
                },
            ],
        },
        "endpoints": [_endpoint(short, entry) for short, entry in ENDPOINTS.items()],
    }
    write_yaml(ROOT / "tools/codegen/endpoints/atrium.yaml", doc)

    schema_dir = ROOT / "tools/codegen/schemas/native/atrium"
    rewrite_schema_dir(schema_dir)
    notes: Dict[str, Dict[str, str]] = {}
    bad: List[str] = []
    for short, entry in ENDPOINTS.items():
        frame = _schema_name(entry[1])
        key = f"{SCHEMA_PREFIX}{short}"
        notes[key] = {}
        cols = []
        for col, dtype in P.SCHEMAS[frame].items():
            desc = _describe(frame, col)
            if _filler(col, desc):
                bad.append(f"{key}.{col}: {desc!r}")
            notes[key][col] = desc
            cols.append({"name": col, "type": _type(dtype), "description": ""})
        write_yaml(schema_dir / f"{short}.yaml", {"schema": key, "kind": "dataframe", "columns": cols})
    if bad:
        raise SystemExit("atrium: filler or missing descriptions:\n" + "\n".join(bad))
    write_yaml(NOTES, notes)
    SPEC_PATHS.write_text(json.dumps(sorted(spec["paths"]), indent=1) + "\n", encoding="utf-8", newline="\n")
    print(f"atrium: {len(ENDPOINTS)} endpoints")


if __name__ == "__main__":
    main()
