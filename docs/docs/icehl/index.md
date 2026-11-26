---
title: ICE Hockey League
sidebar_label: ICE Hockey League
description: "sdv-py ICE Hockey League: endpoint references, dataset loaders and parsers for ICE Hockey League in the SportsDataverse Python package."
---
# ICE Hockey League (`sportsdataverse.hockey.icehl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen)](#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen) | `s3.eu-west-1.amazonaws.com` | 12 | none |

## eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) {#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen}

| Reference | Functions |
|---|---:|
| [ICE Hockey League data feed (eSports hokejovyzapis S3 bucket, Austria)](reference/icehl_esports) | 12 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.icehl` (Python) | `fastRhockey` (R) |
|---|---|
| [`icehl_faceoffs`](reference/icehl_esports/other#icehl_faceoffs) | [`icehl_faceoffs`](https://fastRhockey.sportsdataverse.org/reference/icehl_faceoffs.html) |
| [`icehl_game_center`](reference/icehl_esports/game#icehl_game_center) | [`icehl_game_center`](https://fastRhockey.sportsdataverse.org/reference/icehl_game_center.html) |
| [`icehl_game_info`](reference/icehl_esports/game#icehl_game_info) | [`icehl_game_info`](https://fastRhockey.sportsdataverse.org/reference/icehl_game_info.html) |
| [`icehl_goalie_box`](reference/icehl_esports/other#icehl_goalie_box) | [`icehl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/icehl_goalie_box.html) |
| [`icehl_league_part_player_stats`](reference/icehl_esports/other#icehl_league_part_player_stats) | [`icehl_league_part_player_stats`](https://fastRhockey.sportsdataverse.org/reference/icehl_league_part_player_stats.html) |
| [`icehl_pbp`](reference/icehl_esports/other#icehl_pbp) | [`icehl_pbp`](https://fastRhockey.sportsdataverse.org/reference/icehl_pbp.html) |
| [`icehl_player_box`](reference/icehl_esports/other#icehl_player_box) | [`icehl_player_box`](https://fastRhockey.sportsdataverse.org/reference/icehl_player_box.html) |
| [`icehl_schedule`](reference/icehl_esports/other#icehl_schedule) | [`icehl_schedule`](https://fastRhockey.sportsdataverse.org/reference/icehl_schedule.html) |
| [`icehl_shots`](reference/icehl_esports/other#icehl_shots) | [`icehl_shots`](https://fastRhockey.sportsdataverse.org/reference/icehl_shots.html) |
| [`icehl_standings`](reference/icehl_esports/other#icehl_standings) | [`icehl_standings`](https://fastRhockey.sportsdataverse.org/reference/icehl_standings.html) |
| [`icehl_team_player_stats`](reference/icehl_esports/other#icehl_team_player_stats) | [`icehl_team_player_stats`](https://fastRhockey.sportsdataverse.org/reference/icehl_team_player_stats.html) |
| [`icehl_top_scorers`](reference/icehl_esports/other#icehl_top_scorers) | [`icehl_top_scorers`](https://fastRhockey.sportsdataverse.org/reference/icehl_top_scorers.html) |
