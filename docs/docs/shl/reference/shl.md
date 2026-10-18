---
title: SHL — SHL site API (www.shl.se/api, Sweden)
sidebar_label: SHL site API (www.shl.se/api, Sweden)
description: "SHL — SHL site API (www.shl.se/api, Sweden) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# SHL — SHL site API (www.shl.se/api, Sweden)

`sportsdataverse.hockey.shl` — 27 endpoints.

## Athlete

| Function | Summary |
|---|---|
| [shl_athlete_career](shl/athlete.md#shl_athlete_career) | Player career totals by season. |
| [shl_athlete_details](shl/athlete.md#shl_athlete_details) | Player bio plus playerExtIds (isa, ramses_legacy, ...). |
| [shl_athlete_game_log](shl/athlete.md#shl_athlete_game_log) | Player game log of one season. |
| [shl_athlete_profile](shl/athlete.md#shl_athlete_profile) | Player profile: age, height, weight, position. |
| [shl_athlete_seasons](shl/athlete.md#shl_athlete_seasons) | Seasons a player has stats for. |

## Game

| Function | Summary |
|---|---|
| [shl_game_header](shl/game.md#shl_game_header) | Game header: StatNet extId, arena, state, teams, scores. |
| [shl_game_overview](shl/game.md#shl_game_overview) | Score, state, clock, StatNet gameId and gameSourceId. |
| [shl_game_summary](shl/game.md#shl_game_summary) | Goals by period, head-to-head summary, best scorers. |

## Team

| Function | Summary |
|---|---|
| [shl_team](shl/team.md#shl_team) | One team. |
| [shl_team_played_games](shl/team.md#shl_team_played_games) | A team's recent games. |
| [shl_team_stats](shl/team.md#shl_team_stats) | Team stats per period and total (2023/24 on). |
| [shl_team_upcoming_games](shl/team.md#shl_team_upcoming_games) | A team's next games. |

## Other

| Function | Summary |
|---|---|
| [shl_boxscore](shl/other.md#shl_boxscore) | Skater and goalie box score per team (2023/24 on). |
| [shl_last_game_day](shl/other.md#shl_last_game_day) | Games of the last played match day of an ssgt. |
| [shl_layout_info](shl/other.md#shl_layout_info) | Directory of every SHL ssgt (season + series + game type) with teams and the stats modules. |
| [shl_next_game_day](shl/other.md#shl_next_game_day) | Games of the next match day of an ssgt. |
| [shl_pbp](shl/other.md#shl_pbp) | Play-by-play events with shot coordinates (2023/24 on). |
| [shl_player_box](shl/other.md#shl_player_box) | Extended skater and goalie game stats per team (2023/24 on). |
| [shl_player_h2h](shl/other.md#shl_player_h2h) | Player head-to-head for a matchup. |
| [shl_post_game_team_stats](shl/other.md#shl_post_game_team_stats) | Post-game team comparison. |
| [shl_roster](shl/other.md#shl_roster) | Current roster of a team, grouped by position. |
| [shl_schedule](shl/other.md#shl_schedule) | Schedule and results of one season + series + game type. |
| [shl_season_filter](shl/other.md#shl_season_filter) | Series, seasons (1975/76 on) and game types with their uuids, plus the default ssgt. |
| [shl_sites](shl/other.md#shl_sites) | Teams (sites) taking part in an ssgt. |
| [shl_ssgt_provider](shl/other.md#shl_ssgt_provider) | Stats provider of an ssgt, a bare string such as statnet. |
| [shl_standings](shl/other.md#shl_standings) | Standings of an ssgt (gameday widget). |
| [shl_stats_info](shl/other.md#shl_stats_info) | Stats table of one module for an ssgt (standings, players, goalies, teams, games). |
