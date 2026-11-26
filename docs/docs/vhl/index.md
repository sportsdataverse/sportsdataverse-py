---
title: VHL
sidebar_label: VHL
description: "sdv-py VHL: endpoint references, dataset loaders and parsers for VHL in the SportsDataverse Python package."
---
# VHL (`sportsdataverse.hockey.vhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [VHL](#vhl) | `online.vhlru.ru`, `www.vhlru.ru` | 14 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 1 | — |

## VHL {#vhl}

| Reference | Functions |
|---|---:|
| [VHL (vhlru.ru server-rendered pages, parsed)](reference/vhl) | 13 |
| [Hand-written wrappers](reference/additional) | 1 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.vhl` (Python) | `fastRhockey` (R) |
|---|---|
| [`vhl_game_info`](reference/vhl#vhl_game_info) | [`vhl_game_info`](https://fastRhockey.sportsdataverse.org/reference/vhl_game_info.html) |
| [`vhl_goalie_box`](reference/vhl#vhl_goalie_box) | [`vhl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/vhl_goalie_box.html) |
| [`vhl_goalie_stats`](reference/vhl#vhl_goalie_stats) | [`vhl_goalie_stats`](https://fastRhockey.sportsdataverse.org/reference/vhl_goalie_stats.html) |
| [`vhl_goals`](reference/vhl#vhl_goals) | [`vhl_goals`](https://fastRhockey.sportsdataverse.org/reference/vhl_goals.html) |
| [`vhl_pbp`](reference/vhl#vhl_pbp) | [`vhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/vhl_pbp.html) |
| [`vhl_penalties`](reference/vhl#vhl_penalties) | [`vhl_penalties`](https://fastRhockey.sportsdataverse.org/reference/vhl_penalties.html) |
| [`vhl_player`](reference/vhl#vhl_player) | [`vhl_player`](https://fastRhockey.sportsdataverse.org/reference/vhl_player.html) |
| [`vhl_player_box`](reference/vhl#vhl_player_box) | [`vhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/vhl_player_box.html) |
| [`vhl_player_career`](reference/vhl#vhl_player_career) | [`vhl_player_career`](https://fastRhockey.sportsdataverse.org/reference/vhl_player_career.html) |
| [`vhl_roster`](reference/vhl#vhl_roster) | [`vhl_roster`](https://fastRhockey.sportsdataverse.org/reference/vhl_roster.html) |
| [`vhl_schedule`](reference/vhl#vhl_schedule) | [`vhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/vhl_schedule.html) |
| [`vhl_skater_stats`](reference/vhl#vhl_skater_stats) | [`vhl_skater_stats`](https://fastRhockey.sportsdataverse.org/reference/vhl_skater_stats.html) |
| [`vhl_standings`](reference/vhl#vhl_standings) | [`vhl_standings`](https://fastRhockey.sportsdataverse.org/reference/vhl_standings.html) |
| [`vhl_tn`](reference/additional#vhl_tn) | [`vhl_tn`](https://fastRhockey.sportsdataverse.org/reference/vhl_tn.html) |
