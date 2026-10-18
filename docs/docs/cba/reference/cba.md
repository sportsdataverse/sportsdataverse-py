---
title: CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies)
sidebar_label: CBA data API (data-server.cbaleague.com, China; encrypted bodies)
description: "CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# CBA — CBA data API (data-server.cbaleague.com, China; encrypted bodies)

`sportsdataverse.basketball.cba` — 27 endpoints.

> **Note:** Unofficial and unsupported: the routes cbaleague.com's own data site (a Vue SPA) calls. SportsDataverse has not ruled on the site's terms, so payloads are not redistributed as release assets. Requests are spaced at least 1 s apart; keep use small.

> **Note:** Access: every response body is AES-128-ECB encrypted with a public key shipped in the site's bundle; `cba_runtime` decrypts it (stdlib implementation, no dependency) and refuses anything that does not decrypt. The host blocks datacenter IPs at TCP: set the environment variable CBA_PROXY to a residential proxy URL (it is never logged). Without it the request goes direct and, from a datacenter, fails within seconds (a 5 s connect timeout, no retry of a dropped connection). Where direct access works (a residential IP), set CBA_DIRECT=1 for the full 60 s timeout and retries.

> **Note:** Seasons: the API's season is the START year; every `season` argument here is the END year (2026 = 2025-26) and the runtime sends season - 1. Columns named `season` keep the wire value (the START year, so sdv END = season + 1). The season list reaches 1995-96; box data before 2009-10 is unverified.

> **Note:** Coverage: game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). Play-by-play is verified on 2017-18 and 2025-26 only and is absent for 2009-10 (NoDataError). There are NO shot x/y coordinates in this API: shot location exists only as team and player zone splits. Play-by-play has no player or team ids (Chinese sentence text plus surnames). Box data has 12-minute quarters in 2009-10 and 2017-18 and 10-minute quarters in 2025-26; the women's CBA is not covered (no official source).

> **Note:** Not game data: cba_game_team_shots and cba_game_team_four answer the two teams' SEASON tables (the shot zones are the teams' current profile; the four factors are the previous season), whichever game is asked for; rows cannot be scoped to the game. The reference lists (cba_team_options, cba_team_counts) and cba_report_status are not echoed by the API, so their scope is unchecked.

> **Note:** Duplicate rows: in some games (2025-26 game 100105799) the advanced-box and extras routes repeat player rows (26 rows for 12 players); the repeats are not always copies. cba_player_advanced and cba_player_extras keep the FIRST row per player_id.

> **Note:** Text fields are Chinese (names, sentences, position and nationality labels); the column descriptions carry the English glosses, and tools/codegen/notes/cba_glossary.yaml holds the full nationality, position, team-name and play-by-play phrase glossaries.

> **Note:** Personal data: the operator ids (createdBy / lastModifiedBy, one holds a mobile number) are removed from every body. Players' public bio fields (birth date, height, weight) are kept.

## Game

| Function | Summary |
|---|---|
| [cba_game_info](cba/game.md#cba_game_info) | Game report header: both teams, score, tip-off time and colours. |
| [cba_game_team_shots](cba/game.md#cba_game_team_shots) | Shot-zone profile of the two teams of a game. NOT game data: the teams' current season table. |
| [cba_game_team_four](cba/game.md#cba_game_team_four) | Four-factors table of the two teams of a game. NOT game data: the teams' previous season table. |

## Player

| Function | Summary |
|---|---|
| [cba_player_box](cba/player.md#cba_player_box) | Player box score of a finished game, both sides, plus a team rebounds/turnovers row per side (null player_id). |
| [cba_player_advanced](cba/player.md#cba_player_advanced) | Advanced player box of a finished game (usage, true shooting, rebound/steal/block rates, zone splits), one row per player. |
| [cba_player_extras](cba/player.md#cba_player_extras) | Fast-break points, dunk points and paint points per player of a finished game, one row per player. |
| [cba_player_base](cba/player.md#cba_player_base) | Season player box (one page of the season player table). |
| [cba_player_offensive](cba/player.md#cba_player_offensive) | Season player offensive rates (one page). |
| [cba_player_defensive](cba/player.md#cba_player_defensive) | Season player defensive rates (one page). |
| [cba_player_shots](cba/player-2.md#cba_player_shots) | Season player shot-zone profile (one page). |

## Team

| Function | Summary |
|---|---|
| [cba_team_options](cba/team.md#cba_team_options) | The teams of one season as a select list: team id and the season's display name. |
| [cba_teams](cba/team.md#cba_teams) | The current team list: logo, team id, club name and display name. |
| [cba_team_counts](cba/team.md#cba_team_counts) | Team counts of one season, keyed by match type. |
| [cba_team_basic](cba/team.md#cba_team_basic) | A team's per-game averages or totals, one row per season of the team. |
| [cba_team_four](cba/team.md#cba_team_four) | A team's four-factors table, one row per season of the team. |
| [cba_team_overview](cba/team.md#cba_team_overview) | A team's overview table (record, pace, per-100 ratings), one row per season of the team. |
| [cba_team_shots](cba/team.md#cba_team_shots) | A team's shot-zone profile, one row per season of the team (zone splits from 2017-18). |

## Other

| Function | Summary |
|---|---|
| [cba_seasons](cba/other.md#cba_seasons) | Every season of the CBA data site, newest first (the upcoming season is the default), 1995-96 on. |
| [cba_match_types](cba/other.md#cba_match_types) | The match types (regular season, playoffs, preseason, all-star, relegation play-off) with their ids. |
| [cba_schedule_month](cba/other.md#cba_schedule_month) | Game count per day of one calendar month. |
| [cba_schedule_day](cba/other.md#cba_schedule_day) | The games of one day: ids, teams, status, score, round and the data flags. |
| [cba_period_scores](cba/other.md#cba_period_scores) | Points each side scored in each period of a finished game (up to seven periods). |
| [cba_best_player](cba/other.md#cba_best_player) | The best player of each side in the game report. |
| [cba_best_stats](cba/other.md#cba_best_stats) | Single-category leaders of each side in the game report. |
| [cba_rosters](cba/other.md#cba_rosters) | Registered rosters of both sides with player bio fields (the season registration, not the game box). |
| [cba_report_status](cba/other.md#cba_report_status) | Whether the game report page exists for the game. |
| [cba_pbp](cba/other.md#cba_pbp) | Play-by-play of a finished game, every period merged, oldest first (Chinese text; no player or team ids). |
