---
title: National League — National League app API (www.nationalleague.ch/api, Switzerland)
sidebar_label: National League app API (www.nationalleague.ch/api, Switzerland)
description: "National League — National League app API (www.nationalleague.ch/api, Switzerland) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# National League — National League app API (www.nationalleague.ch/api, Switzerland)

`sportsdataverse.hockey.nl` — 17 endpoints.

> **Note:** robots.txt on www.nationalleague.ch disallows /api. The owner approved building on it on 2026-10-12 for polite, incremental use: requests are spaced at least 1 s apart, and a bulk backfill or a published mirror needs the owner's sign-off first.

> **Note:** Crawl incrementally: fetch a finished game once (keyed by game_id), find new games with one nl_schedule call per run, and fetch season tables at most once per game day.

## Player

| Function | Summary |
|---|---|
| [nl_player_box](nationalleague/player.md#nl_player_box) | One game's player box score: goals, assists, shots, faceoffs, ice time, goaltending. |
| [nl_player_stats](nationalleague/player.md#nl_player_stats) | Season stats for every player, with playerId. |
| [nl_player_season](nationalleague/player.md#nl_player_season) | One player's season stats. |

## Team

| Function | Summary |
|---|---|
| [nl_team_schedule](nationalleague/team.md#nl_team_schedule) | One team's games for one season. |
| [nl_team_stats](nationalleague/team.md#nl_team_stats) | One game's team totals: shots by period, faceoffs by zone, power play and penalty kill. |
| [nl_team_player_stats](nationalleague/team.md#nl_team_player_stats) | Season stats for one team's players. |
| [nl_team_info](nationalleague/team.md#nl_team_info) | Club CMS record: names, logo, website, ticket links. |

## Other

| Function | Summary |
|---|---|
| [nl_schedule](nationalleague/other.md#nl_schedule) | Schedule and results for one season: NL plus exhibitions. |
| [nl_current_games](nationalleague/other.md#nl_current_games) | The current or next game day. |
| [nl_playoffs](nationalleague/other.md#nl_playoffs) | Playoff and play-in series by round: one row per game, with its series score. |
| [nl_playouts](nationalleague/other.md#nl_playouts) | Playout and league-qualification series by round: one row per game, with its series score. |
| [nl_game_overview](nationalleague/other.md#nl_game_overview) | One game's header: teams, date, status, final and per-period score, coaches, referees, arena, attendance. |
| [nl_pbp](nationalleague/other.md#nl_pbp) | One game's event timeline: goals with assists, penalties, goaltender changes, revoked goals. |
| [nl_lineups](nationalleague/other.md#nl_lineups) | One game's lineups: every dressed player by unit, with licence number, position and line. |
| [nl_shots](nationalleague/other.md#nl_shots) | One game's shot attempts with rink location (metres) and expected goals (xG); NL games only. |
| [nl_top_scorers](nationalleague/other.md#nl_top_scorers) | Top scorers (same row shape as /player). |
| [nl_standings](nationalleague/other.md#nl_standings) | Standings with team season stats. |
