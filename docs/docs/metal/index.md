---
title: Metal Ligaen
sidebar_label: Metal Ligaen
description: "sdv-py Metal Ligaen: endpoint references, dataset loaders and parsers for Metal Ligaen in the SportsDataverse Python package."
---
# Metal Ligaen (`sportsdataverse.hockey.metal`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen)](#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen) | `s3.eu-west-1.amazonaws.com` | 8 | none |

## eSports hokejovyzapis (DEL, ICEHL, EIHL, Metal Ligaen) {#esports-hokejovyzapis-del-icehl-eihl-metal-ligaen}

| Reference | Functions |
|---|---:|
| [Metal Ligaen data feed (eSports hokejovyzapis S3 bucket, Denmark)](reference/metal_esports) | 8 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://fastRhockey.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.hockey.metal` (Python) | `fastRhockey` (R) |
|---|---|
| [`metal_game_center`](reference/metal_esports#metal_game_center) | [`metal_game_center`](https://fastRhockey.sportsdataverse.org/reference/metal_game_center.html) |
| [`metal_game_info`](reference/metal_esports#metal_game_info) | [`metal_game_info`](https://fastRhockey.sportsdataverse.org/reference/metal_game_info.html) |
| [`metal_goalie_box`](reference/metal_esports#metal_goalie_box) | [`metal_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/metal_goalie_box.html) |
| [`metal_pbp`](reference/metal_esports#metal_pbp) | [`metal_pbp`](https://fastRhockey.sportsdataverse.org/reference/metal_pbp.html) |
| [`metal_player_box`](reference/metal_esports#metal_player_box) | [`metal_player_box`](https://fastRhockey.sportsdataverse.org/reference/metal_player_box.html) |
| [`metal_schedule`](reference/metal_esports#metal_schedule) | [`metal_schedule`](https://fastRhockey.sportsdataverse.org/reference/metal_schedule.html) |
| [`metal_standings`](reference/metal_esports#metal_standings) | [`metal_standings`](https://fastRhockey.sportsdataverse.org/reference/metal_standings.html) |
| [`metal_top_scorers`](reference/metal_esports#metal_top_scorers) | [`metal_top_scorers`](https://fastRhockey.sportsdataverse.org/reference/metal_top_scorers.html) |
