---
title: HockeyAllsvenskan
sidebar_label: HockeyAllsvenskan
description: "sdv-py HockeyAllsvenskan: endpoint references, dataset loaders and parsers for HockeyAllsvenskan in the SportsDataverse Python package."
---
# HockeyAllsvenskan (`sportsdataverse.hockey.ha`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyAllsvenskan site API](#hockeyallsvenskan-site-api) | `hockeyallsvenskan.se` | 12 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## HockeyAllsvenskan site API {#hockeyallsvenskan-site-api}

| Reference | Functions |
|---|---:|
| [HockeyAllsvenskan site API (hockeyallsvenskan.se, Sweden)](reference/ha) | 11 |
| [Hand-written wrappers](reference/additional) | 1 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.ha` (Python) | `fastRhockey` (R) |
|---|---|
| [`ha_game`](reference/ha#ha_game) | [`ha_game`](https://fastRhockey.sportsdataverse.org/reference/ha_game.html) |
| [`ha_games`](reference/ha#ha_games) | [`ha_games`](https://fastRhockey.sportsdataverse.org/reference/ha_games.html) |
| [`ha_goalie_stats`](reference/ha#ha_goalie_stats) | [`ha_goalie_stats`](https://fastRhockey.sportsdataverse.org/reference/ha_goalie_stats.html) |
| [`ha_leaders`](reference/ha#ha_leaders) | [`ha_leaders`](https://fastRhockey.sportsdataverse.org/reference/ha_leaders.html) |
| [`ha_pbp`](reference/ha#ha_pbp) | [`ha_pbp`](https://fastRhockey.sportsdataverse.org/reference/ha_pbp.html) |
| [`ha_players`](reference/ha#ha_players) | [`ha_players`](https://fastRhockey.sportsdataverse.org/reference/ha_players.html) |
| [`ha_schedule`](reference/ha#ha_schedule) | [`ha_schedule`](https://fastRhockey.sportsdataverse.org/reference/ha_schedule.html) |
| [`ha_skater_stats`](reference/ha#ha_skater_stats) | [`ha_skater_stats`](https://fastRhockey.sportsdataverse.org/reference/ha_skater_stats.html) |
| [`ha_standings`](reference/ha#ha_standings) | [`ha_standings`](https://fastRhockey.sportsdataverse.org/reference/ha_standings.html) |
| [`ha_team_stats`](reference/ha#ha_team_stats) | [`ha_team_stats`](https://fastRhockey.sportsdataverse.org/reference/ha_team_stats.html) |
| [`ha_teams`](reference/ha#ha_teams) | [`ha_teams`](https://fastRhockey.sportsdataverse.org/reference/ha_teams.html) |
