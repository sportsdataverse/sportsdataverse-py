---
title: ACB — ACB API (api2.acb.com, Spain)
sidebar_label: ACB API (api2.acb.com, Spain)
description: "ACB — ACB API (api2.acb.com, Spain) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain)

`sportsdataverse.basketball.acb` — 45 endpoints.

> **Note:** Legal notice (sdv-internal-refs/acb/README.md, aviso legal of acb.com read 2026-10-12): ACEB, S.A.U. claims intellectual property in the site's databases (the EU sui generis database right) among its other content. It authorises viewing and private copies for personal or journalistic use with the source credited, and requires prior express consent for distribution, reproduction or public communication. These wrappers fetch on the user's behalf; a published data release or a hosted mirror needs the owner's decision first. robots.txt allows the paths.

> **Note:** Unofficial and unsupported: api2.acb.com is the .NET API behind acb.com and live.acb.com, not a documented public API. It needs the header `X-APIKEY`, a public key shipped in acb.com's bundle: `acb_runtime` sends the documented value (`$ACB_API_KEY` overrides it), re-reads the live bundle once on a 401, and never prints the key.

> **Note:** Seasons: the API keys a season by `editionId` = season START year - 1935 (91 = 2026-27, 48 = 1983-84, the first). The wrappers take `season`, the END year (2027 = 2026-27), and `acb_runtime` sends `editionId = season - 1936`. A route that omits `season` serves the current edition. `team_id` is a season-team entry (new every season); `club_id` is the stable club.

> **Note:** Coverage: box scores from 1983-84 (old seasons carry only the whole-game period, quarter 0); play-by-play and shots from 2016-17 (earlier games answer empty lists, which raise NoDataError). Shot coordinates come in two systems, split by match id: millimetres from the attacked basket from 2020-21 (`coord_system` mm), a 0-100 full-court percent grid before (grid). A game that is not finished (`matchFinished` false) raises AssetFetchError for the routes that report it.

## Match

| Function | Summary |
|---|---|
| [acb_match_header](acb/match.md#acb_match_header) | Match header: teams, status, score, bonus, current period and the content the site offers. |
| [acb_match_leaders](acb/match.md#acb_match_leaders) | Each team's leaders of a game by category. |
| [acb_match_advanced_stats](acb/match.md#acb_match_advanced_stats) | Team advanced statistics of a game against the team's season and win/loss splits. |

## Player

| Function | Summary |
|---|---|
| [acb_player_box](acb/player.md#acb_player_box) | Per-player box score of a game, per period (quarter 0 = the whole game). |
| [acb_player_advanced_stats](acb/player.md#acb_player_advanced_stats) | One player's advanced statistics in a game against his season and win/loss splits. |
| [acb_player_data](acb/player.md#acb_player_data) | Player biography: birth, nationality, height, current team and social links. |
| [acb_player_stats](acb/player.md#acb_player_stats) | A player's season statistics, one row per team and edition. |
| [acb_player_totals](acb/player.md#acb_player_totals) | A player's career totals, averages and per-40 values. |
| [acb_player_matches](acb/player.md#acb_player_matches) | A player's game log for an edition (default: the current one). |

## Team

| Function | Summary |
|---|---|
| [acb_team_box](acb/team.md#acb_team_box) | Team totals of a game per period: everything the team did, and the part credited to the team itself. |
| [acb_team_comparison](acb/team.md#acb_team_comparison) | Team-versus-team comparison bars of a game per period. |
| [acb_teams](acb/team.md#acb_teams) | Teams entered in a competition edition. |
| [acb_team_matches](acb/team.md#acb_team_matches) | A club's games in a competition edition. |
| [acb_team_leaders](acb/team.md#acb_team_leaders) | A club's statistical leaders for an edition and all-time. |
| [acb_team_stats](acb/team.md#acb_team_stats) | A club's player season statistics for an edition: totals, averages and per-40 side by side. |
| [acb_team_totals](acb/team.md#acb_team_totals) | A club's team season totals, averages and per-40 values for an edition. |
| [acb_team_records](acb/team.md#acb_team_records) | A club's single-game records (best performance per statistic). |

## Other

| Function | Summary |
|---|---|
| [acb_period_scores](acb/other.md#acb_period_scores) | Points each team scored in each period of a game. |
| [acb_box_info](acb/other.md#acb_box_info) | Coaches and game facts per team: head coach, assistants, arena, attendance, referees. |
| [acb_pbp](acb/other.md#acb_pbp) | Play-by-play of a game (2016-17 onward): every shot, rebound, foul, turnover, substitution and time-out. |
| [acb_shots](acb/other.md#acb_shots) | Every field-goal and free-throw attempt of a game (2016-17 onward) with its court position. |
| [acb_lead_tracker](acb/other.md#acb_lead_tracker) | Score through a game, one row per scoring moment, with the margin. |
| [acb_lead_stats](acb/other.md#acb_lead_stats) | Lead statistics of a game (best run, maximum lead, time ahead, lead changes) per side. |
| [acb_lineup](acb/other.md#acb_lineup) | Players of a game with their game line, starters first. |
| [acb_serie](acb/other.md#acb_serie) | The playoff series a game belongs to, one row per game of the series (zero rows outside a series: HTTP 204). |
| [acb_quickfacts](acb/other.md#acb_quickfacts) | Pre-match facts per team: standing, record, recent results. |
| [acb_season_leaders](acb/other.md#acb_season_leaders) | Each team's season leaders before a game, by category. |
| [acb_matchlist](acb/other.md#acb_matchlist) | The games of the round (or week) around a game. |
| [acb_competition_editions](acb/other.md#acb_competition_editions) | Every competition edition (season) the API knows, one row per edition. |
| [acb_competition_rounds](acb/other.md#acb_competition_rounds) | The rounds of every competition edition: the round ids to crawl. |
| [acb_competition_weeks](acb/other.md#acb_competition_weeks) | The weeks of every competition edition: the week ids to crawl. |
| [acb_competition_current](acb/other.md#acb_competition_current) | The API's current pointers: competition, edition, week, round and match. |
| [acb_round_matches](acb/other.md#acb_round_matches) | Games of one round (or week) with scores, status and broadcasters. |
| [acb_standings](acb/other.md#acb_standings) | League table after a round (default: the current round). |
| [acb_standings_rounds](acb/other.md#acb_standings_rounds) | Standings history: each team's position after each round, with the round's game. |
| [acb_bracket](acb/other.md#acb_bracket) | Playoff bracket: one row per series (quarter-final, semifinal, final). |
| [acb_bracket_games](acb/other.md#acb_bracket_games) | The games of every playoff series of a bracket, one row per game. |
| [acb_calendar](acb/other.md#acb_calendar) | Every round of a season with its games: one call lists every game id. |
| [acb_league_zones](acb/other.md#acb_league_zones) | League-wide shooting by court zone for a season. |
| [acb_next_matches](acb/other-2.md#acb_next_matches) | A club's most recent and upcoming games. |
| [acb_club_data](acb/other-2.md#acb_club_data) | Club profile: arena, contacts and links. |
| [acb_current_roster](acb/other-2.md#acb_current_roster) | A club's players and staff for an edition (default: the current one). |
| [acb_career_stints](acb/other-2.md#acb_career_stints) | A player's club stints, one row per stint. |
| [acb_career_trophies](acb/other-2.md#acb_career_trophies) | A player's trophies, one row per competition. |
| [acb_search](acb/other-2.md#acb_search) | Search players, teams and coaches by name. |
