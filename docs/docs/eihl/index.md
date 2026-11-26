---
title: EIHL
sidebar_label: EIHL
description: "sdv-py EIHL: endpoint references, dataset loaders and parsers for EIHL in the SportsDataverse Python package."
---
# EIHL (`sportsdataverse.hockey.eihl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen)](#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen) | `s3.eu-west-1.amazonaws.com` | 9 | none |

## eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) {#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen}

| Reference | Functions |
|---|---:|
| [EIHL data feed (eSports hokejovyzapis S3 bucket, UK)](reference/eihl_esports) | 9 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.eihl` (Python) | `fastRhockey` (R) |
|---|---|
| [`eihl_game_info`](reference/eihl_esports#eihl_game_info) | [`eihl_game_info`](https://fastRhockey.sportsdataverse.org/reference/eihl_game_info.html) |
| [`eihl_goalie_box`](reference/eihl_esports#eihl_goalie_box) | [`eihl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/eihl_goalie_box.html) |
| [`eihl_pbp`](reference/eihl_esports#eihl_pbp) | [`eihl_pbp`](https://fastRhockey.sportsdataverse.org/reference/eihl_pbp.html) |
| [`eihl_player_box`](reference/eihl_esports#eihl_player_box) | [`eihl_player_box`](https://fastRhockey.sportsdataverse.org/reference/eihl_player_box.html) |
| [`eihl_schedule`](reference/eihl_esports#eihl_schedule) | [`eihl_schedule`](https://fastRhockey.sportsdataverse.org/reference/eihl_schedule.html) |
| [`eihl_shots`](reference/eihl_esports#eihl_shots) | [`eihl_shots`](https://fastRhockey.sportsdataverse.org/reference/eihl_shots.html) |
| [`eihl_standings`](reference/eihl_esports#eihl_standings) | [`eihl_standings`](https://fastRhockey.sportsdataverse.org/reference/eihl_standings.html) |
| [`eihl_team_player_stats`](reference/eihl_esports#eihl_team_player_stats) | [`eihl_team_player_stats`](https://fastRhockey.sportsdataverse.org/reference/eihl_team_player_stats.html) |
| [`eihl_top_scorers`](reference/eihl_esports#eihl_top_scorers) | [`eihl_top_scorers`](https://fastRhockey.sportsdataverse.org/reference/eihl_top_scorers.html) |
