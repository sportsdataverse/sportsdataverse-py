---
title: DEL
sidebar_label: DEL
description: "sdv-py DEL: endpoint references, dataset loaders and parsers for DEL in the SportsDataverse Python package."
---
# DEL (`sportsdataverse.hockey.del_`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen)](#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen) | `s3.eu-west-1.amazonaws.com` | 12 | none |

## eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) {#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen}

| Reference | Functions |
|---|---:|
| [DEL data feed (eSports hokejovyzapis S3 bucket, Germany)](reference/del_esports) | 12 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.del_` (Python) | `fastRhockey` (R) |
|---|---|
| [`del_faceoffs`](reference/del_esports#del_faceoffs) | [`del_faceoffs`](https://fastRhockey.sportsdataverse.org/reference/del_faceoffs.html) |
| [`del_game_info`](reference/del_esports#del_game_info) | [`del_game_info`](https://fastRhockey.sportsdataverse.org/reference/del_game_info.html) |
| [`del_goalie_box`](reference/del_esports#del_goalie_box) | [`del_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/del_goalie_box.html) |
| [`del_pbp`](reference/del_esports#del_pbp) | [`del_pbp`](https://fastRhockey.sportsdataverse.org/reference/del_pbp.html) |
| [`del_player_box`](reference/del_esports#del_player_box) | [`del_player_box`](https://fastRhockey.sportsdataverse.org/reference/del_player_box.html) |
| [`del_roster`](reference/del_esports#del_roster) | [`del_roster`](https://fastRhockey.sportsdataverse.org/reference/del_roster.html) |
| [`del_schedule`](reference/del_esports#del_schedule) | [`del_schedule`](https://fastRhockey.sportsdataverse.org/reference/del_schedule.html) |
| [`del_shifts`](reference/del_esports#del_shifts) | [`del_shifts`](https://fastRhockey.sportsdataverse.org/reference/del_shifts.html) |
| [`del_shots`](reference/del_esports#del_shots) | [`del_shots`](https://fastRhockey.sportsdataverse.org/reference/del_shots.html) |
| [`del_standings`](reference/del_esports#del_standings) | [`del_standings`](https://fastRhockey.sportsdataverse.org/reference/del_standings.html) |
| [`del_team_player_stats`](reference/del_esports#del_team_player_stats) | [`del_team_player_stats`](https://fastRhockey.sportsdataverse.org/reference/del_team_player_stats.html) |
| [`del_top_scorers`](reference/del_esports#del_top_scorers) | [`del_top_scorers`](https://fastRhockey.sportsdataverse.org/reference/del_top_scorers.html) |
