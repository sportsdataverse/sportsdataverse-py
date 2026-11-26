---
title: EASL
sidebar_label: EASL
description: "sdv-py EASL: endpoint references, dataset loaders and parsers for EASL in the SportsDataverse Python package."
---
# EASL (`sportsdataverse.basketball.easl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [EASL (third-party sync server over Sportradar DataCore)](#easl-third-party-sync-server-over-sportradar-datacore) | `easl.joshricha.com` | 18 | none |

## EASL (third-party sync server over Sportradar DataCore) {#easl-third-party-sync-server-over-sportradar-datacore}

| Reference | Functions |
|---|---:|
| [EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore)](reference/easl) | 18 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.easl` (Python) | `hoopR` (R) |
|---|---|
| [`easl_game`](reference/easl/other#easl_game) | [`easl_game`](https://hoopR.sportsdataverse.org/reference/easl_game.html) |
| [`easl_health`](reference/easl/other#easl_health) | [`easl_health`](https://hoopR.sportsdataverse.org/reference/easl_health.html) |
| [`easl_leaders`](reference/easl/other#easl_leaders) | [`easl_leaders`](https://hoopR.sportsdataverse.org/reference/easl_leaders.html) |
| [`easl_pbp`](reference/easl/other#easl_pbp) | [`easl_pbp`](https://hoopR.sportsdataverse.org/reference/easl_pbp.html) |
| [`easl_player`](reference/easl/player#easl_player) | [`easl_player`](https://hoopR.sportsdataverse.org/reference/easl_player.html) |
| [`easl_player_box`](reference/easl/player#easl_player_box) | [`easl_player_box`](https://hoopR.sportsdataverse.org/reference/easl_player_box.html) |
| [`easl_player_stats`](reference/easl/player#easl_player_stats) | [`easl_player_stats`](https://hoopR.sportsdataverse.org/reference/easl_player_stats.html) |
| [`easl_players`](reference/easl/player#easl_players) | [`easl_players`](https://hoopR.sportsdataverse.org/reference/easl_players.html) |
| [`easl_schedule`](reference/easl/other#easl_schedule) | [`easl_schedule`](https://hoopR.sportsdataverse.org/reference/easl_schedule.html) |
| [`easl_seasons`](reference/easl/other#easl_seasons) | [`easl_seasons`](https://hoopR.sportsdataverse.org/reference/easl_seasons.html) |
| [`easl_standings`](reference/easl/other#easl_standings) | [`easl_standings`](https://hoopR.sportsdataverse.org/reference/easl_standings.html) |
| [`easl_team`](reference/easl/team#easl_team) | [`easl_team`](https://hoopR.sportsdataverse.org/reference/easl_team.html) |
| [`easl_team_box`](reference/easl/team#easl_team_box) | [`easl_team_box`](https://hoopR.sportsdataverse.org/reference/easl_team_box.html) |
| [`easl_team_box_by_period`](reference/easl/team#easl_team_box_by_period) | [`easl_team_box_by_period`](https://hoopR.sportsdataverse.org/reference/easl_team_box_by_period.html) |
| [`easl_team_leaders`](reference/easl/team-2#easl_team_leaders) | [`easl_team_leaders`](https://hoopR.sportsdataverse.org/reference/easl_team_leaders.html) |
| [`easl_team_schedule`](reference/easl/team#easl_team_schedule) | [`easl_team_schedule`](https://hoopR.sportsdataverse.org/reference/easl_team_schedule.html) |
| [`easl_team_stats`](reference/easl/team-2#easl_team_stats) | [`easl_team_stats`](https://hoopR.sportsdataverse.org/reference/easl_team_stats.html) |
| [`easl_teams`](reference/easl/team#easl_teams) | [`easl_teams`](https://hoopR.sportsdataverse.org/reference/easl_teams.html) |
