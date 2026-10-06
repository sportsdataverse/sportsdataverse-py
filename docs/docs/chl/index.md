---
title: CHL
sidebar_label: CHL
description: "sdv-py CHL: endpoint references, dataset loaders and parsers for CHL in the SportsDataverse Python package."
---
# CHL (`sportsdataverse.chl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 11 | per-league public key (SDV_<LEAGUE>_API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 14 | — |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 11 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`build_family`](reference/additional#build_family)

### Dates and seasons {#dates-and-seasons}

- [`chl_season_id`](reference/additional#chl_season_id)
- [`most_recent_chl_season`](reference/additional#most_recent_chl_season)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
