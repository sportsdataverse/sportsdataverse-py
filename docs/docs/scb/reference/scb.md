---
title: SCB — SCB basketball API (BBL and DBBL, Germany)
sidebar_label: SCB basketball API (BBL and DBBL, Germany)
description: "SCB — SCB basketball API (BBL and DBBL, Germany) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# SCB — SCB basketball API (BBL and DBBL, Germany)

`sportsdataverse.basketball.scb` — 23 endpoints.

> **Note:** Unofficial use of a public source: these wrappers read the front-end API of the German Bundesliga sites (easycredit-bbl.de, toyota-dbbl.de), run by SCB; payloads are not redistributed as release assets.

> **Note:** Legal notice: the BBL Impressum forbids storing BBL content (`Inhalte`) in an information system without BBL's written permission. That is a copyright term, not a robots rule (the API host has no robots.txt). The owner reviewed it and accepted the intake (2026-10-12 ruling); keep requests slow (the runtime waits at least 1 s per host) and raise any published data release with the owner. The DBBL host is the same operator and API; its site notice was not separately reviewed.

> **Note:** Both hosts need two headers: a constant `x-api-key` and an `x-api-secret` that rotates. The runtime derives the secret on first use from the key embedded in an easycredit-bbl.de page (reversed, `:` turned into `-`) and never logs or returns it. The derivation was reverse-engineered from observed requests and responses and has not been checked against the site's own front-end code; if the site changes how it signs requests, every call fails with AssetFetchError.

> **Note:** Id key spaces: seasonId is the START year (2025 = 2025-26), so a wrapper's END-year `season` is sent as `season - 1`. Teams are keyed by teamId (seasonTeamId equals it in every sample). seasonPlayerId is per player per season and per club (a player traded mid-season has two); realPlayerId is the person. playerId is ambiguous (the seasonPlayerId in box scores, records and /season-players; the person in /transfers). Game ids are strings.

## Game

| Function | Summary |
|---|---|
| [scb_games_recent](scb/game.md#scb_games_recent) | Recent, live and upcoming games, one row per game with its past / present / future bucket. |
| [scb_game_players](scb/game.md#scb_game_players) | Box score of a finished game: one row per player line of both teams (2002-03 on). |
| [scb_game_teams](scb/game.md#scb_game_teams) | Box score of a finished game: one row per team, with the head coach and the team line. |
| [scb_game_actions](scb/game.md#scb_game_actions) | Play-by-play of a finished game with shot x/y (BBL 2008-09 on, DBBL 2024-25 on). |

## Team

| Function | Summary |
|---|---|
| [scb_team_statistics](scb/team.md#scb_team_statistics) | Season team stat table: totals and per-game values, one row per team and stat block. |
| [scb_team](scb/team.md#scb_team) | Club profile: names, code, home page, ticket shop, year joined and honours. |
| [scb_team_seasons](scb/team.md#scb_team_seasons) | Every season a club played: its name that season and its championship and cup titles. |
| [scb_team_rosters](scb/team.md#scb_team_rosters) | Per club, the current roster, one row per player (BBL only). |

## Other

| Function | Summary |
|---|---|
| [scb_seasons](scb/other.md#scb_seasons) | Seasons, newest first, with the active flag (BBL back to 1966, DBBL 2024 on). |
| [scb_season_players](scb/other.md#scb_season_players) | Every player registered in a season: the id crosswalk (season_player_id, real_player_id, season_team_id). |
| [scb_season_teams](scb/other.md#scb_season_teams) | Every team of a season: club id, season-team id, name and three-letter code. |
| [scb_playoff_series](scb/other.md#scb_playoff_series) | Playoff bracket of a season: one row per series (quarter-finals, semi-finals, finals). |
| [scb_playoff_games](scb/other.md#scb_playoff_games) | Every playoff game of a season with its series and the points of each series team. |
| [scb_player_statistics](scb/other.md#scb_player_statistics) | Season player stat table: totals and per-game values, one row per player and stat block. |
| [scb_games](scb/other.md#scb_games) | Games of a season (the whole season in one page): teams, score by period, officials, venue and top scorer. |
| [scb_game](scb/other.md#scb_game) | One game header: teams, score by period, officials, venue and top scorer. |
| [scb_standings](scb/other.md#scb_standings) | League table of a season: record, home / away split, streaks and tie-break detail (2005-06 on). |
| [scb_season_player](scb/other.md#scb_season_player) | Player profile by season-player id: bio, position, club and previous clubs. |
| [scb_referees](scb/other.md#scb_referees) | Current referees and commissioners (BBL only). |
| [scb_transfers](scb/other.md#scb_transfers) | Latest transfers, one row per move (BBL only). |
| [scb_transfers_teams](scb/other.md#scb_transfers_teams) | Per club, the players joining and leaving, one row per move with its direction (BBL only). |
| [scb_records_per_game](scb/other.md#scb_records_per_game) | Single-game records: the season's and the all-time ones, one row per record. |
| [scb_records_season_averages](scb/other-2.md#scb_records_season_averages) | All-time season-average records (points, rebounds, assists, ...), one row per record. |
